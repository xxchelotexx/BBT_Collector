import os
import sys
import time
import json
from datetime import datetime, timezone
from curl_cffi import requests
from dotenv import load_dotenv
from pymongo import MongoClient

# --- CONFIGURACIÓN GLOBAL ---
load_dotenv()

# Configuración de consola para soportar emoticones en Windows
if sys.platform.startswith('win'):
    sys.stdout.reconfigure(encoding='utf-8')

# --- CONFIGURACIÓN MONGODB ATLAS ---
db_user = os.getenv("MONGO_USER")
db_pass = os.getenv("MONGO_PASS")
db_cluster = os.getenv("MONGO_CLUSTER")

MONGO_URI = f"mongodb+srv://{db_user}:{db_pass}@{db_cluster}/?retryWrites=true&w=majority"

try:
    client = MongoClient(MONGO_URI)
    db = client["Monitor_P2P_Bolivia"]
    collection = db["BBT_PRICE"]
    client.admin.command('ping')
except Exception as e:
    print(f"❌ Error crítico de conexión a MongoDB: {e}")
    sys.exit(1)

# --- CONFIGURACIÓN ENDPOINT BYBIT X-API ---
BYBIT_OTC_URL = "https://www.bybit.com/x-api/fiat/otc/item/online"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "Content-Type": "application/json",
    "Origin": "https://www.bybit.com",
    "Referer": "https://www.bybit.com/fiat/trade/express/home",
    "Sec-Ch-Ua": '"Not A(Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
}

def consultar_bybit_otc(side: int, page: int):
    """Consulta la API usando curl_cffi con TLS impersonation para evadir Akamai."""
    payload = {
        "tokenId": "USDT",
        "currencyId": "BOB",
        "side": str(side),
        "page": str(page),
        "size": "20"
    }
    
    # impersonate="chrome120" fuerza a curl_cffi a replicar el TLS Fingerprint exacto de Chrome
    response = requests.post(
        BYBIT_OTC_URL, 
        json=payload, 
        headers=HEADERS, 
        impersonate="chrome120",
        timeout=15
    )
    
    if response.status_code != 200:
        print(f"🚨 [HTTP {response.status_code}] Error en lado={side}, pag={page}")
        print(f"   ↳ Body: {response.text[:300]}")
        response.raise_for_status()

    return response.json()


# --- FUNCIÓN DE PROCESAMIENTO Y GUARDADO ---

def ejecutar_recoleccion_datos():
    print(f"\n--- 📡 Iniciando recolección Bybit: {datetime.now().strftime('%H:%M:%S')} ---")
    
    resultados_finales = []
    anuncios_por_tipo = {} 
    estados = [1, 0] 

    for estado in estados:
        items = []
        ordenes_abiertas_por_tipo = []
        anuncios = []
        
        trade_type = "BUY" if estado == 1 else "SELL"
        merchant_key = "merchant_buy" if trade_type == "BUY" else "merchant_sell"

        for page in range(1, 20): 
            try:
                response = consultar_bybit_otc(side=estado, page=page)
                
                if response.get("ret_code") == 0 and response.get("result") and response["result"].get("items"):
                    p_items = response["result"]["items"]
                    items.extend(p_items)
                    # print(f"   🔍 Lado {trade_type} | Pág {page}: Recibidos {len(p_items)} anuncios.")
                else:
                    ret_code = response.get('ret_code')
                    ret_msg = response.get('ret_msg')
                    # print(f"   ℹ️ Fin de paginación o respuesta vacía (lado={estado}, p={page}). Code: {ret_code}, Msg: '{ret_msg}'")
                    break 
            except Exception as e:
                print(f"⚠️ Deteniendo iteración por error en API Bybit (lado={estado}, p={page}): {e}")
                break 

        agrupado = {}
        vol_total = 0.0

        for item in items:
            try:
                precio_float = float(item["price"])
                cantidad = float(item["lastQuantity"])
                frozen = float(item.get("frozenQuantity", 0))
                executed = float(item.get("executedQuantity", 0))
                nickname = item.get("nickName", "Sin nombre")
                    
                anuncios.append({
                    "nickName": nickname,
                    "price": precio_float,
                    "tradableQuantity": cantidad,
                    "minSingleTransAmount": float(item.get("minAmount", 0)),
                    "maxSingleTransAmount": float(item.get("maxAmount", 0)),
                    "executed": executed,
                    "frozenQuantity": frozen
                })    
                    
                if frozen != 0:
                    ordenes_abiertas_por_tipo.append({
                        "nickname": nickname,
                        "executed": executed,
                        "frozenQuantity": frozen,
                        "precio": precio_float  
                    })

                vol_total += cantidad
                precio_key = f"{precio_float:.3f}".replace(".", "_")

                if precio_key not in agrupado:
                    agrupado[precio_key] = {
                        "suma": 0.0, 
                        "conteo": 0,
                        "min_amounts": [],
                        "max_amounts": [],
                        "frozen_total": 0.0,
                        "executed_total": 0.0
                    }

                agrupado[precio_key]["suma"] += cantidad
                agrupado[precio_key]["conteo"] += 1
                agrupado[precio_key]["min_amounts"].append(float(item["minAmount"]))
                agrupado[precio_key]["max_amounts"].append(float(item["maxAmount"]))
                agrupado[precio_key]["frozen_total"] += frozen
                agrupado[precio_key]["executed_total"] += executed

            except (TypeError, ValueError, KeyError):
                continue
        
        datos_agrupados_mongo = {}
        
        for p_key, valores in agrupado.items():
            p_float = float(p_key.replace("_", "."))
            min_agrupado = min(valores["min_amounts"]) if valores["min_amounts"] else 0.0
            max_agrupado = max(valores["max_amounts"]) if valores["max_amounts"] else 0.0
            
            suma_max_bob = sum(valores["max_amounts"])
            inmediato_usdt = suma_max_bob / p_float if p_float != 0 else 0.0
            
            datos_agrupados_mongo[p_key] = {
                "suma": valores["suma"],
                "conteo": valores["conteo"],
                "min": min_agrupado,
                "max": max_agrupado,
                "inmediato": inmediato_usdt,
                "volumen_en_proceso": valores["frozen_total"],
                "volumen_ejecutado": valores["executed_total"]
            }
            
        resultados_finales.append({
            "trade_type": trade_type,
            "vol_total_anuncios": vol_total,
            "datos_agrupados": datos_agrupados_mongo,
            "ordenes_abiertas": ordenes_abiertas_por_tipo
        })

        anuncios_por_tipo[merchant_key] = anuncios

    documento = {
        "timestamp": datetime.now(timezone.utc),
        "exchange": "bybit",
        "resultados": resultados_finales,
        "merchant_buy": anuncios_por_tipo.get("merchant_buy", []),
        "merchant_sell": anuncios_por_tipo.get("merchant_sell", [])
    }
    
    try:
        collection.insert_one(documento)
        print(f"✅ Recolección completa. Datos guardados en MongoDB. {datetime.now().strftime('%H:%M:%S')}")
    except Exception as e:
        print(f"❌ Error MongoDB: {e}")


def worker():
    print("🚀 Programador Bybit -> MongoDB iniciado.")
    
    while True:
        ahora = datetime.now()
        intervalo = 10 if 6 <= ahora.hour <= 23 else 30
        
        ejecutar_recoleccion_datos()
        time.sleep(intervalo)

if __name__ == "__main__":
    try:
        worker()
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo el colector de Bybit...")