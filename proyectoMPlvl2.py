import requests
import json
import time
import unicodedata
import os
import re
from supabase import create_client, Client

# --- CONFIGURACIÓN SUPABASE ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# --- FUNCIONES DE UTILIDAD (Mismas que antes) ---
def extract_json_object_by_key(html_content, key_name):
    idx_key = html_content.find(key_name)
    if idx_key == -1: idx_key = html_content.find(f'"{key_name}"')
    if idx_key == -1: return None
    idx_open_brace = html_content.find('{', idx_key)
    if idx_open_brace == -1: return None
    brace_count = 0
    idx_end_brace = -1
    search_limit = min(idx_open_brace + 500000, len(html_content))
    for i in range(idx_open_brace, search_limit):
        if html_content[i] == '{': brace_count += 1
        elif html_content[i] == '}':
            brace_count -= 1
            if brace_count == 0:
                idx_end_brace = i + 1
                break
    if idx_end_brace != -1:
        try:
            return json.loads(html_content[idx_open_brace:idx_end_brace].replace('\n', '').replace('\r', '').replace('\\"', '"'))
        except:
            try: return json.loads(html_content[idx_open_brace:idx_end_brace].replace("'", '"'))
            except: pass
    return None

def clean_column_name(region_name):
    nfkd = unicodedata.normalize('NFKD', region_name)
    cleaned = "".join([c for c in nfkd if not unicodedata.combining(c)])
    return f"Precio_{cleaned.replace('Region de ', '').replace('Region del ', '').replace(' ', '_').replace('-', '_').replace('.', '')}"

def clean_price_value(value):
    if not value: return 0
    try:
        if isinstance(value, str): return int(float(value.replace(',', '').replace('$', '').strip()))
        return int(float(value))
    except: return 0

def get_prices_with_offers(json_prices, offer_prices, product_id, region_map):
    if not json_prices: return {}
    precios_finales = {}
    for r_id, provs in json_prices.items():
        nombre_real = region_map.get(r_id, f"Region_{r_id}")
        # Usamos el nombre limpio como CLAVE del JSON
        nombre_json = clean_column_name(nombre_real).replace("Precio_", "") 
        
        if not isinstance(provs, dict): continue
        lista_precios = []
        for p_id, data in provs.items():
            if not isinstance(data, dict): continue
            price_raw = data.get('price', '0')
            p_final = clean_price_value(price_raw)
            
            # Chequear oferta
            if offer_prices and product_id:
                try:
                    offers = offer_prices.get(str(p_id), {}).get(str(product_id), {}).get(str(r_id), {})
                    spec_raw = offers.get('special_price', '0')
                    p_spec = clean_price_value(spec_raw)
                    if p_spec > 0: p_final = p_spec
                except: pass
            
            if p_final > 0: lista_precios.append(p_final)
        
        if lista_precios:
            precios_finales[nombre_json] = min(lista_precios)
    return precios_finales

# --- PROCESO PRINCIPAL ---
def process_details_from_supabase():
    print("--- Iniciando Scraper Detalles (Tabla 2) ---")
    
    # 1. Leer productos de la Tabla 1 (productos_lista)
    # Puedes agregar .limit(10) para pruebas o .range(0, 1000) para paginación
    print("Leyendo lista de productos desde Supabase...")
    response = supabase.table("productos_lista").select("id_producto, link_producto, nombre_producto").execute()
    products = response.data
    
    print(f"Se procesarán {len(products)} productos.")
    
    for idx, prod in enumerate(products):
        p_id = prod['id_producto']
        link = prod['link_producto']
        nombre = prod['nombre_producto']
        
        print(f"[{idx+1}/{len(products)}] ID: {p_id}...")
        
        try:
            # 2. Hacer Request al producto
            resp = requests.get(link, headers=headers, timeout=20)
            html = resp.text
            
            # 3. Extraer Datos
            region_map = extract_json_object_by_key(html, "region_names") or extract_json_object_by_key(html, "regionMapping") or {}
            json_prices = extract_json_object_by_key(html, "jsonResult")
            offer_prices = extract_json_object_by_key(html, "offerPrices")
            
            match_id = re.search(r'"productId"\s*:\s*"(\d+)"', html)
            internal_id = match_id.group(1) if match_id else p_id
            
            if json_prices:
                precios_region = get_prices_with_offers(json_prices, offer_prices, internal_id, region_map)
                
                if precios_region:
                    min_global = int(min(precios_region.values()))
                    mejor_region = min(precios_region, key=precios_region.get)
                    
                    # 4. Guardar en Tabla 2 (productos_detalles)
                    detalle_data = {
                        "id_producto": p_id,
                        "nombre_producto": nombre,
                        "precio_min_global": min_global,
                        "region_mejor_precio": mejor_region,
                        "precios_por_region": precios_region # Supabase guarda esto como JSONB automáticamente
                    }
                    
                    supabase.table("productos_detalles").upsert(detalle_data).execute()
                    print(f"  ✓ Guardado: Min ${min_global}")
                else:
                    print("  ⚠ Sin precios válidos.")
            else:
                print("  ⚠ No se encontró jsonResult.")
                
        except Exception as e:
            print(f"  ✗ Error procesando {p_id}: {e}")
        
        time.sleep(1) # Pausa para no ser bloqueado

if __name__ == "__main__":
    process_details_from_supabase()
