import os
import re
import json
import time
import unicodedata
import requests
import pandas as pd
from supabase import create_client, Client
from concurrent.futures import ThreadPoolExecutor, as_completed


def get_supabase_client():
    """
    Crea el cliente de Supabase usando SUPABASE_URL y SUPABASE_KEY
    desde variables de entorno.
    """
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError(
            "Faltan SUPABASE_URL o SUPABASE_KEY en las variables de entorno."
        )
    return create_client(url, key)


def extract_json_object_by_key(html_content, key_name):
    """
    Extrae un objeto JSON desde HTML buscando una clave (por ejemplo 'jsonResult')
    y encontrando la llave de cierre correspondiente.
    """
    idx_key = html_content.find(key_name)
    if idx_key == -1:
        idx_key = html_content.find('"{}"'.format(key_name))
    if idx_key == -1:
        return None

    # Primera llave '{' después de la clave
    idx_open_brace = html_content.find("{", idx_key)
    if idx_open_brace == -1:
        return None

    brace_count = 0
    idx_end_brace = -1
    # Límite de búsqueda para no congelar en HTML muy grande
    search_limit = min(idx_open_brace + 500000, len(html_content))

    for i in range(idx_open_brace, search_limit):
        if html_content[i] == "{":
            brace_count += 1
        elif html_content[i] == "}":
            brace_count -= 1
        if brace_count == 0:
            idx_end_brace = i + 1
            break

    if idx_end_brace != -1:
        json_str = html_content[idx_open_brace:idx_end_brace]
        try:
            json_str_clean = (
                json_str.replace("\n", "")
                .replace("\r", "")
                .replace('\\"', '"')
            )
            return json.loads(json_str_clean)
        except json.JSONDecodeError:
            try:
                return json.loads(json_str.replace("'", '"'))
            except Exception:
                pass

    return None


def extract_product_id(html_content):
    """
    Extrae el productId principal necesario para buscar ofertas en offerPrices.
    """
    match = re.search(r'"productId"\s*:\s*"(\d+)"', html_content)
    if match:
        return match.group(1)
    return None


def clean_column_name(region_name):
    """
    Normaliza nombres de región (por compatibilidad con columnas tipo 'Precio_Region').
    """
    nfkd_form = unicodedata.normalize("NFKD", region_name)
    cleaned = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    cleaned = cleaned.replace("Region de ", "").replace("Region del ", "")
    cleaned = cleaned.replace(" ", "_").replace("-", "_").replace(".", "")
    return "Precio_{}".format(cleaned)


def clean_price_value(value):
    """
    Convierte un string de precio (ej: '15,167.00') a entero (15167).
    """
    if not value:
        return 0
    try:
        if isinstance(value, str):
            clean_val = value.replace(",", "").replace("$", "").strip()
            if not clean_val:
                return 0
            return int(float(clean_val))
        else:
            return int(float(value))
    except (ValueError, TypeError):
        return 0


def get_minimum_price_by_region_with_offers(json_prices, offer_prices, product_id, region_names_map):
    """
    Calcula precio mínimo por región, priorizando 'special_price' de offerPrices
    si está disponible.
    """
    if not json_prices:
        return {}

    precios_finales = {}

    # json_prices tiene keys = IDs de región
    for region_id, providers in json_prices.items():
        nombre_region_real = region_names_map.get(region_id, "Region_ID_{}".format(region_id))

        if not isinstance(providers, dict):
            continue

        lista_precios = []

        for provider_id, data in providers.items():
            if not isinstance(data, dict):
                continue

            # 1. Precio estándar desde jsonResult
            price_raw = data.get("price", "0")
            price_final = clean_price_value(price_raw)

            # 2. Revisar special_price en offerPrices
            # Estructura: offerPrices[provider_id][product_id][region_id]['special_price']
            if offer_prices and product_id:
                try:
                    provider_offers = offer_prices.get(str(provider_id))
                    if provider_offers:
                        product_offers = provider_offers.get(str(product_id))
                        if product_offers:
                            region_offer = product_offers.get(str(region_id))
                            if region_offer:
                                special_price_raw = region_offer.get("special_price")
                                if special_price_raw:
                                    special_price = clean_price_value(special_price_raw)
                                    if special_price > 0:
                                        price_final = special_price
                except Exception:
                    # En caso de error en estructura de offers, usar precio estándar
                    pass

            if price_final > 0:
                lista_precios.append(price_final)

        if lista_precios:
            precios_finales[nombre_region_real] = min(lista_precios)

    return precios_finales


def process_one_product(row, headers, total_count):
    """
    Procesa un producto individual:
    - hace request a la ficha,
    - obtiene jsonResult y offerPrices,
    - calcula precios mínimos por región,
    - retorna un dict listo para guardar en Supabase.
    """
    idx = row.get("_index", 0)
    producto_id_csv = row.get("id_producto")
    nombre = row.get("nombre_producto", "")
    link = row.get("link_producto", "")

    # Parseo seguro de número de proveedores
    try:
        raw_prov = row.get("numero_proveedores", 0)
        if pd.isna(raw_prov):
            num_providers = 0
        else:
            num_providers = int(float(raw_prov))
    except Exception:
        num_providers = 0

    print("[{}/{}] ID: {} | {}...".format(idx, total_count, producto_id_csv, nombre[:30]))

    try:
        response = requests.get(link, headers=headers, timeout=15)
        if response.status_code != 200:
            print(" ⚠ Error HTTP {} para ID {}".format(response.status_code, producto_id_csv))
            time.sleep(0.2)
            return None

        html = response.text

        # 1. Metadatos de regiones
        region_names_map = extract_json_object_by_key(html, "region_names")
        if not region_names_map:
            region_names_map = extract_json_object_by_key(html, "regionMapping") or {}

        product_id_internal = extract_product_id(html)

        # 2. Datos de precios
        json_prices = extract_json_object_by_key(html, "jsonResult")
        offer_prices = extract_json_object_by_key(html, "offerPrices")

        if json_prices:
            # 3. Cálculo de precios por región (incluyendo ofertas)
            precios_region = get_minimum_price_by_region_with_offers(
                json_prices,
                offer_prices,
                product_id_internal,
                region_names_map,
            )

            if precios_region:
                precio_global = int(min(precios_region.values()))
                mejor_region = min(precios_region, key=precios_region.get)
                print(" ✓ Mínimo encontrado para {}: ${} ({})".format(producto_id_csv, precio_global, mejor_region))

                row_data = {
                    "id_producto": str(producto_id_csv),
                    "nombre_producto": nombre,
                    "numero_proveedores": num_providers,
                    "link_producto": link,
                    "precio_minimo_global": precio_global,
                    "region_mejor_precio": mejor_region,
                    # JSON con precios por región: { "Region Metropolitana": 1234, ... }
                    "precios_region": precios_region,
                }

                time.sleep(0.2)
                return row_data
            else:
                print(" ⚠ Sin precios válidos encontrados para ID {}".format(producto_id_csv))
        else:
            print(" ⚠ No se encontró jsonResult para ID {}".format(producto_id_csv))

    except Exception as e:
        print(" ✗ Error procesando ID {}: {}".format(producto_id_csv, e))

    time.sleep(0.2)
    return None


def process_products_with_prices(max_products=3, max_workers=8):
    """
    - Lee productos desde cm_productos en Supabase.
    - Procesa en paralelo con ThreadPoolExecutor.
    - Guarda resumen en cm_precios_minimos (Supabase).
    """
    print("=" * 70)
    print("EXTRACCIÓN DE PRECIOS CON OFERTAS")
    print("=" * 70)

    supabase = get_supabase_client()

    # Leer productos desde cm_productos (limitado por max_products)
    resp = (
        supabase.table("cm_productos")
        .select("id_producto,nombre_producto,numero_proveedores,link_producto")
        .limit(max_products)
        .execute()
    )

    if not resp.data:
        print("✗ No se encontraron productos en cm_productos.")
        return None

    rows = resp.data
    total_count = len(rows)
    print("✓ Procesando {} productos...\n".format(total_count))

    # Añadir índice 1..N para logs
    for idx, row in enumerate(rows, start=1):
        row["_index"] = idx

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
    }

    resultados = []

    # Paralelización con ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_one_product, row, headers, total_count)
            for row in rows
        ]

        for fut in as_completed(futures):
            try:
                data = fut.result()
                if data is not None:
                    resultados.append(data)
            except Exception as e:
                print("✗ Error en thread: {}".format(e))

    if not resultados:
        print("✗ No se generaron resultados.")
        return None

    print("\n[4/4] Guardando {} productos en Supabase...".format(len(resultados)))

    # Opcional: DataFrame para inspección local
    df_final = pd.DataFrame(resultados)

    # Upsert en cm_precios_minimos
    chunk_size = 200
    for i in range(0, len(resultados), chunk_size):
        chunk = resultados[i : i + chunk_size]
        (
            supabase.table("cm_precios_minimos")
            .upsert(chunk, on_conflict="id_producto")
            .execute()
        )

    print("✓ Listo: datos guardados/actualizados en cm_precios_minimos en Supabase")
    return df_final


if __name__ == "__main__":
    # max_products: cuántos productos leer de cm_productos
    # max_workers: cuántos threads en paralelo (no subir demasiado para no saturar el sitio)
    process_products_with_prices(max_products=999999, max_workers=8)
