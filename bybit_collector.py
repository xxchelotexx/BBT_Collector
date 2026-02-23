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
    """
    Llama a la API de Bybit, procesa los datos y los inserta en MongoDB.
    """
    print(f"\n--- 📡 Iniciando recolección Bybit: {datetime.now().strftime('%H:%M:%S')} ---")
    
    resultados_finales = []
    # 1 para SELL (Venta del usuario), 0 para BUY (Compra del usuario)
    # Nota: Bybit API usa side=1 para Ads de venta, side=0 para Ads de compra
    estados = [1, 0] 

    for estado in estados:
        items = []
        # Paginación de anuncios
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
                # Para MongoDB, reemplazamos el punto decimal por guion bajo en la llave
                precio_key = f"{precio_float:.2f}".replace(".", "_")
                
                cantidad = float(item["lastQuantity"])
                min_amount = float(item["minAmount"])
                max_amount = float(item["maxAmount"])
                frozen = float(item.get("frozenQuantity", 0))
                executed = float(item.get("executedQuantity", 0))   

                vol_total += cantidad

                if precio_key not in agrupado:
                    agrupado[precio_key] = {
                        "suma": 0.0, 
                        "conteo": 0,
                        "min_amounts": [],
                        "max_amounts": [],
                        "frozen": 0.0,
                        "executed": 0.0
                    }

                agrupado[precio_key]["suma"] += cantidad
                agrupado[precio_key]["conteo"] += 1
                agrupado[precio_key]["min_amounts"].append(min_amount)
                agrupado[precio_key]["max_amounts"].append(max_amount)
                agrupado[precio_key]["frozen"] += frozen
                agrupado[precio_key]["executed"] += executed

            except (TypeError, ValueError, KeyError):
                continue
        
        trade_type = "SELL" if estado == 0 else "BUY" 
        datos_agrupados_mongo = {}
        
        # Procesar cálculos finales por cada grupo de precio
        for p_key, valores in agrupado.items():
            p_float = float(p_key.replace("_", "."))
            
            min_agrupado = min(valores["min_amounts"]) if valores["min_amounts"] else 0.0
            max_agrupado = max(valores["max_amounts"]) if valores["max_amounts"] else 0.0
            
            # Cálculo de inmediato (Suma de maxAmounts en BOB / precio)
            suma_max_bob = sum(valores["max_amounts"])
            inmediato_usdt = suma_max_bob / p_float if p_float != 0 else 0.0
            
            datos_agrupados_mongo[p_key] = {
                "suma": valores["suma"],
                "conteo": valores["conteo"],
                "min": min_agrupado,
                "max": max_agrupado,
                "inmediato": inmediato_usdt,
                "volumen_en_proceso": valores["frozen"],
                "volumen_ejecutado": valores["executed"]
            }
            
        resultados_finales.append({
            "trade_type": trade_type,
            "vol_total": vol_total,
            "datos_agrupados": datos_agrupados_mongo
        })

    # --- INSERCIÓN EN MONGODB ---
    documento = {
        "timestamp": datetime.now(timezone.utc),
        "exchange": "bybit",
        "resultados": resultados_finales
    }
    
    try:
        collection.insert_one(documento)
        print(f"✅ Datos de Bybit guardados en MongoDB Atlas.")
    except Exception as e:
        print(f"❌ Error al insertar en MongoDB: {e}")


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