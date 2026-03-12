from bybit_p2p import P2P
from dotenv import load_dotenv
import sys
import json
import os
import time
from datetime import datetime, timezone
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
    # Verificar conexión
    client.admin.command('ping')
except Exception as e:
    print(f"❌ Error crítico de conexión a MongoDB: {e}")
    sys.exit(1)

# --- INICIALIZACIÓN API BYBIT ---
try:
    api = P2P(
        testnet=False,
        api_key=os.getenv("BYBIT_API_KEY"),
        api_secret=os.getenv("BYBIT_API_SECRET")
    )
except Exception as e:
    print(f"❌ Error al inicializar la API de Bybit: {e}")
    sys.exit(1)

# --- FUNCIÓN DE PROCESAMIENTO Y GUARDADO ---

def ejecutar_recoleccion_datos():
    print(f"\n--- 📡 Iniciando recolección Bybit: {datetime.now().strftime('%H:%M:%S')} ---")
    
    resultados_finales = []
    # 1 para anuncios de venta (tú compras -> BUY), 0 para anuncios de compra (tú vendes -> SELL)
    estados = [1, 0] 

    for estado in estados:
        items = []
        ordenes_abiertas_por_tipo = []  # Lista específica para este grupo (BUY o SELL)
        anuncios= []
        
        trade_type = "BUY" if estado == 1 else "SELL" # Definimos el tipo según el estado

        for page in range(1, 20): 
            try:
                response = api.get_online_ads(
                    tokenId="USDT",
                    currencyId="BOB",
                    side=estado,
                    page=str(page)
                )
                if response.get("result") and response["result"].get("items"):
                    items.extend(response["result"]["items"])
                else:
                    break 
            except Exception as e:
                print(f"⚠️ Error API Bybit (lado={estado}, p={page}): {e}")
                break 

        agrupado = {}
        vol_total = 0.0

        for item in items:
            try:
                precio_float = float(item["price"])
                cantidad = float(item["lastQuantity"])
                # Campos de interés
                frozen = float(item.get("frozenQuantity", 0))
                executed = float(item.get("executedQuantity", 0))
                nickname = item.get("nickName", "Sin nombre")
                    
                anuncios.append({
                            "nickname": nickname,
                            "cantidad": cantidad,
                            "executed": executed,
                            "frozenQuantity": frozen,
                            "precio": precio_float
                        })    
                    
                # 🟢 Lógica solicitada: Si frozenQuantity != 0, agregar a la lista del grupo actual
                if frozen != 0:
                    ordenes_abiertas_por_tipo.append({
                        "nickname": nickname,
                        "executed": executed,
                        "frozenQuantity": frozen,
                        "precio": precio_float  # Agregado para mayor utilidad
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
            
        # 🟢 Guardamos el resultado del grupo actual (BUY o SELL)
        resultados_finales.append({
            "trade_type": trade_type,
            "vol_total_anuncios": vol_total,
            "datos_agrupados": datos_agrupados_mongo,
            "ordenes_abiertas": ordenes_abiertas_por_tipo,
            "anuncios": anuncios  # Aquí quedan agrupadas
        })

    # Inserción en MongoDB
    documento = {
        "timestamp": datetime.now(timezone.utc),
        "exchange": "bybit",
        "resultados": resultados_finales
    }
    
    try:
        collection.insert_one(documento)
        print(f"✅ Recolección completa. Datos agrupados por BUY/SELL guardados.")
    except Exception as e:
        print(f"❌ Error MongoDB: {e}")

def worker():
    print("🚀 Programador Bybit -> MongoDB iniciado.")
    
    while True:
        ahora = datetime.now()
        # Horario: 10s en día, 30s en noche
        intervalo = 10 if 6 <= ahora.hour <= 23 else 30
        
        ejecutar_recoleccion_datos()
        time.sleep(intervalo)

if __name__ == "__main__":
    try:
        worker()
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo el colector de Bybit...")