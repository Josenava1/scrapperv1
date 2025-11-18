import requests
from bs4 import BeautifulSoup
import re
import os
from supabase import create_client, Client

# --- CONFIGURACIÓN SUPABASE ---
# Lo ideal es usar variables de entorno, si no, pega tus credenciales aquí
SUPABASE_URL = os.environ.get("SUPABASE_URL") 
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- CONFIGURACIÓN SCRAPING ---
URL_BASE = "https://conveniomarco2.mercadopublico.cl/alimentos2/alimentos"
MAX_PAGES = 5  # Ajusta esto según necesites

def get_total_pages(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=30)
        soup = BeautifulSoup(response.text, 'html.parser')
        toolbar = soup.find('div', class_='toolbar-products')
        if toolbar:
            pages = toolbar.find_all('li', class_='item')
            if pages:
                return int(pages[-2].text.strip())
    except:
        return 1
    return 1

def scrape_and_save():
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    total_pages = get_total_pages(URL_BASE)
    pages_to_process = min(total_pages, MAX_PAGES)
    
    print(f"--- Iniciando Scraper Lista (Tabla 1) - {pages_to_process} páginas ---")

    products_batch = []

    for page in range(1, pages_to_process + 1):
        print(f"Procesando página {page}...")
        url = f"{URL_BASE}?p={page}"
        try:
            response = requests.get(url, headers=headers, timeout=20)
            soup = BeautifulSoup(response.text, 'html.parser')
            items = soup.find_all('li', class_='product-item')
            
            for item in items:
                try:
                    link_tag = item.find('a', class_='product-item-link')
                    if not link_tag: continue
                    
                    nombre = link_tag.text.strip()
                    link = link_tag['href']
                    
                    sku_div = item.find('div', class_='price-box')
                    id_prod = sku_div['data-product-id'] if sku_div else "0"
                    
                    # Extraer número de proveedores (Regex básico)
                    num_prov = 0
                    prov_text = item.text
                    match = re.search(r'(\d+)\s*Proveedores', prov_text, re.IGNORECASE)
                    if match:
                        num_prov = int(match.group(1))
                    
                    # Preparamos el objeto para Supabase
                    product_data = {
                        "id_producto": id_prod,
                        "nombre_producto": nombre,
                        "link_producto": link,
                        "num_proveedores": num_prov
                    }
                    products_batch.append(product_data)

                except Exception as e:
                    print(f"Error al extraer item: {e}")
                    continue
                    
        except Exception as e:
            print(f"Error cargando página {page}: {e}")

    # Guardar en Supabase (Batch Upsert es más eficiente)
    if products_batch:
        print(f"Guardando {len(products_batch)} productos en tabla 'productos_lista'...")
        try:
            # Insertamos en lotes para no saturar
            for i in range(0, len(products_batch), 100):
                batch = products_batch[i:i+100]
                supabase.table("productos_lista").upsert(batch).execute()
            print("✓ Guardado exitoso en Supabase.")
        except Exception as e:
            print(f"✗ Error guardando en Supabase: {e}")
    else:
        print("No se encontraron productos.")

if __name__ == "__main__":
    scrape_and_save()
