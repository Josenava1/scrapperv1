import os
import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup
from supabase import create_client, Client

# URL base
base_url = "https://conveniomarco2.mercadopublico.cl/alimentos2/alimentos"


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


def get_total_products(url: str) -> int | None:
    """
    Extrae el total de productos de la página.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
    }

    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        # Buscar el span con clase "toolbar-number" que contiene el total
        toolbar_spans = soup.find_all("span", class_="toolbar-number")
        if len(toolbar_spans) >= 2:
            # El segundo span contiene el total
            return int(toolbar_spans[1].get_text().strip())
    except Exception as e:
        print(f"Error al obtener total de productos: {e}")
        return None

    return None


def scrape_products_page(page_num: int) -> list[dict]:
    """
    Extrae información de productos de una página específica.
    """
    url = (
        f"{base_url}?p={page_num}"
        "&product_list_limit=25&product_list_mode=list&product_list_order=name"
    )
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
    }

    products_data: list[dict] = []

    try:
        print(f"Extrayendo página {page_num}...")
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        # Encontrar todos los productos con class="item product product-item"
        products = soup.find_all("li", class_="item product product-item")

        for product in products:
            try:
                # Nombre del producto
                product_name_elem = product.find("a", class_="product-item-link")
                product_name = (
                    product_name_elem.get_text().strip() if product_name_elem else ""
                )

                # Link del producto
                product_link = (
                    product_name_elem.get("href", "") if product_name_elem else ""
                )

                # Número de proveedores (desde div con class="sellers-count")
                sellers_elem = product.find("div", class_="sellers-count")
                num_providers = ""
                if sellers_elem:
                    sellers_text = sellers_elem.get_text().strip()
                    # Extraer solo el número (ej: "78 proveedores" -> "78")
                    match = re.search(r"(\d+)", sellers_text)
                    if match:
                        num_providers = match.group(1)

                # ID del producto (desde div con class="product-id-top")
                product_id_elem = product.find("div", class_="product-id-top")
                product_id = ""
                if product_id_elem:
                    id_text = product_id_elem.get_text().strip()
                    match = re.search(r"ID\s+(\d+)", id_text)
                    if match:
                        product_id = match.group(1)

                products_data.append(
                    {
                        "ID_Producto": product_id,
                        "Nombre_Producto": product_name,
                        "Numero_Proveedores": num_providers,
                        "Link_Producto": product_link,
                        "Pagina": page_num,
                    }
                )
            except Exception as e:
                print(f"Error al procesar un producto en página {page_num}: {e}")
                continue

        return products_data

    except Exception as e:
        print(f"Error al procesar página {page_num}: {e}")
        return []


def main():
    # CONFIGURACIÓN: cuántas páginas máximo quieres recorrer
    MAX_PAGES_TEST = 999999  # Cambia este número si quieres limitar

    print("=" * 60)
    print("EXTRACCIÓN DE PRODUCTOS - CONVENIO MARCO ALIMENTOS")
    print("=" * 60)
    print("\nExtrayendo páginas")
    print("\n[1/3] Obteniendo total de productos...")

    first_page_url = (
        f"{base_url}?p=1"
        "&product_list_limit=25&product_list_mode=list&product_list_order=name"
    )
    total_products = get_total_products(first_page_url)

    if not total_products:
        print(
            "✗ No se pudo obtener el total de productos. "
            "Verifica la URL o la conexión."
        )
        return

    print(f"✓ Total de productos encontrados: {total_products}")

    # Calcular total de páginas (total_products / 25, redondeado hacia arriba)
    total_pages = (total_products + 24) // 25
    print(f"✓ Total de páginas disponibles: {total_pages}")

    # Limitar a MAX_PAGES_TEST
    pages_to_extract = min(MAX_PAGES_TEST, total_pages)
    print(f"✓ Páginas a extraer en esta corrida: {pages_to_extract}")

    # Paso 2: Extraer información de las páginas
    print(f"\n[2/3] Extrayendo información de {pages_to_extract} páginas...\n")

    all_products: list[dict] = []
    for page in range(1, pages_to_extract + 1):
        products = scrape_products_page(page)
        all_products.extend(products)
        print(
            f" ✓ Página {page}/{pages_to_extract} completada - "
            f"{len(products)} productos extraídos"
        )
        # Pausa entre requests para no sobrecargar el servidor
        time.sleep(2)

    # Paso 3: Crear DataFrame y guardar en Supabase
    print("\n[3/3] Procesando y guardando datos en Supabase...")

    df = pd.DataFrame(all_products)
    print(f"✓ Total de productos extraídos: {len(df)}")

    if df.empty:
        print("✗ No se extrajeron productos, nada que guardar.")
        return

    # Normalizar tipos
    df["Numero_Proveedores"] = (
        pd.to_numeric(df["Numero_Proveedores"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    df["Pagina"] = pd.to_numeric(df["Pagina"], errors="coerce").fillna(0).astype(int)

    # Crear registros alineados con cm_productos
    registros: list[dict] = []
    for _, row in df.iterrows():
        registros.append(
            {
                "id_producto": str(row["ID_Producto"]),
                "nombre_producto": row["Nombre_Producto"],
                "numero_proveedores": int(row["Numero_Proveedores"]),
                "link_producto": row["Link_Producto"],
                "pagina": int(row["Pagina"]),
            }
        )

    # Enviar a Supabase (upsert para que sea idempotente)
    supabase = get_supabase_client()
    chunk_size = 500

    for i in range(0, len(registros), chunk_size):
        chunk = registros[i : i + chunk_size]
        supabase.table("cm_productos").upsert(
            chunk,
            on_conflict="id_producto",  # requiere UNIQUE en la tabla
        ).execute()

    # Resumen
    print("\n" + "=" * 60)
    print("RESUMEN DE EXTRACCIÓN")
    print("=" * 60)
    print(f"Total de productos extraídos: {len(df)}")
    print(f"Productos únicos (por ID): {df['ID_Producto'].nunique()}")
    print(f"Páginas procesadas: {pages_to_extract} de {total_pages} disponibles")
    print("\nPrimeros 10 productos extraídos (solo en memoria):")
    print(df[["ID_Producto", "Nombre_Producto", "Numero_Proveedores"]].head(10))
    print("\n✓ Datos guardados/actualizados en tabla cm_productos (Supabase)")


if __name__ == "__main__":
    main()
