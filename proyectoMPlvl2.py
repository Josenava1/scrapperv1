import os
import re
import json
import time
import unicodedata
import requests
import pandas as pd
from supabase import create_client, Client


def get_supabase_client() -> Client:
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


def extract_json_object_by_key(html_content: str, key_name: str):
    """
    Extrae un objeto JSON desde HTML buscando una clave (por ejemplo 'jsonResult')
    y encontrando la llave de cierre correspondiente.
    """
    idx_key = html_content.find(key_name)
    if idx_key == -1:
        idx_key = html_content.find(f'"{key_name}"')
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


def extract_product_id(html_content: str) -> str | None:
    """
    Extrae el productId principal necesario para buscar ofertas en offerPrices.
    """
    match = re.search(r'"productId"\s*:\s*"(\d+)"', html_content)
    if match:
        return match.group(1)
    return None


def clean_column_name(region_name: str) -> str:
    """
    Normaliza nombres de región (por si quisieras columnas tipo 'Precio_Region').
    Se mantiene por compatibilidad, aunque ahora usamos JSON en la BD.
    """
    nfkd_form = unicodedata.normalize("NFKD", region_name)
    cleaned = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    cleaned = cleaned.replace("Region de ", "").replace("Region del ", "")
    cleaned = cleaned.replace(" ", "_").replace("-", "_").replace(".", "")
    return f"Precio_{cleaned}"


def clean_price_value(value) -> int:
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


def get_minimum_price_by_region_with_offers(
    json_prices: dict,
    offer_prices: dict | None,
    product_id: str | None,
    region_names_map: dict,
) -> dict:
    """
    Calcula precio mínimo por región, priorizando 'special_price' de offerPrices
    si está disponible.
    """
    if not json_prices:
        return {}

    precios_finales: dict = {}

    # json_prices tiene keys = IDs de región
    for region_id, providers in json_prices.items():
        nombre_region_real = region_names_map.get(region_id, f"Region_ID_{region_id}")

        if not isinstance(providers, dict):
            continue

        lista_precios: list[int] = []

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
                                    special_price = clean_price_value(
                                        special_price_raw
                                    )
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


def process_products_with_prices(max_products: int = 3):
    """
    Función principal:
    - Lee productos desde cm_productos en Supabase.
    - Para cada producto obtiene jsonResult y offerPrices.
    - Calcula precios mínimos por región.
    - Guarda resumen en cm_precios_minimos (Supabase).
    """
    print("=" * 70)
    print("EXTRACCIÓN DE PRECIOS CON OFERTAS")
    print("=" * 70)

    supabase = get_supabase_client()

    # Leer productos desde cm_productos
    resp = (
        supabase.table("cm_productos")
        .select("id_producto,nombre_producto,numero_proveedores,link_producto")
        .limit(max_products)
        .execute()
    )

    if not resp.data:
        print("✗ No se encontraron productos en cm_productos.")
        return None

    df = pd.DataFrame(resp.data)
    df_test = df  # ya viene limitado por .limit()
    print(f"✓ Procesando {len(df_test)} productos...\n")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
    }

    resultados: list[dict] = []

    for idx, row in df_test.iterrows():
        producto_id_csv = row["id_producto"]
        nombre = row["nombre_producto"]
        link = row["link_producto"]

        # Parseo seguro de número de proveedores
        try:
            raw_prov = row.get("numero_proveedores", 0)
            if pd.isna(raw_prov):
                num_providers = 0
            else:
                num_providers = int(float(raw_prov))
        except Exception:
            num_providers = 0

        print(f"[{idx + 1}/{len(df_test)}] ID: {producto_id_csv} | {nombre[:30]}...")

        try:
            response = requests.get(link, headers=headers, timeout=30)
            if response.status_code != 200:
                print(f" ⚠ Error HTTP {response.status_code}")
                continue

            html = response.text

            # 1. Metadatos de regiones
            region_names_map = extract_json_object_by_key(html, "region_names")
            if not region_names_map:
                region_names_map = (
                    extract_json_object_by_key(html, "regionMapping") or {}
                )

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
                    print(
                        f" ✓ Mínimo encontrado: ${precio_global} ({mejor_region})"
                    )

                    # Estructura única por producto (para cm_precios_minimos)
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

                    resultados.append(row_data)
                else:
                    print(" ⚠ Sin precios válidos encontrados.")
            else:
                print(" ⚠ No se encontró jsonResult.")

        except Exception as e:
            print(f" ✗ Error: {e}")

        time.sleep(1)

    if not resultados:
        print("✗ No se generaron resultados.")
        return None

    print(f"\n[4/4] Guardando {len(resultados)} productos en Supabase...")

    # Opcional: DataFrame para inspección local (no se escribe CSV)
    df_final = pd.DataFrame(resultados)

    # Upsert en cm_precios_minimos
    chunk_size = 200
    for i in range(0, len(resultados), chunk_size):
        chunk = resultados[i : i + chunk_size]
        supabase.table("cm_precios_minimos").upsert(
            chunk,
            on_conflict="id_producto",  # requiere UNIQUE(id_producto) en la tabla
        ).execute()

    print("✓ Listo: datos guardados/actualizados en cm_precios_minimos en Supabase")
    return df_final


if __name__ == "__main__":
    # Cambia max_products si quieres limitar la corrida en pruebas
    process_products_with_prices(max_products=999999)
