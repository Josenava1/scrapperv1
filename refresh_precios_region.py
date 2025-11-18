import os
from supabase import create_client, Client

def get_supabase_client() -> Client:
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("Faltan SUPABASE_URL o SUPABASE_KEY en variables de entorno.")
    return create_client(url, key)

def main():
    supabase = get_supabase_client()
    print("Llamando función refrescar_cm_precios_region() ...")
    resp = supabase.rpc("refrescar_cm_precios_region").execute()
    if getattr(resp, "error", None):
        print(f"✗ Error al refrescar cm_precios_region: {resp.error}")
        raise SystemExit(1)
    print("✓ Tabla cm_precios_region refrescada correctamente.")

if __name__ == "__main__":
    main()
