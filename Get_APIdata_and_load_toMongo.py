from datetime import datetime, timezone
import requests
import logging

from pymongo import MongoClient
from pymongo.errors import PyMongoError 

from airflow.decorators import dag, task
from airflow.models import Variable

log = logging.getLogger(__name__)

@dag(
    dag_id="Get_APIdata_and_load_toMongo",
    start_date=datetime(2026, 6, 7),
    schedule="*/10 * * * *", # Запуск каждые 10 минут
    catchup=False,
    tags=["Project_ETL_Air_Quality"]
)

def extract_and_save_data():

    @task
    def fetch_api_data():
        DATA_API_URL = "https://api.purpleair.com/v1/sensors/{sensor_id}?fields=name,confidence,channel_flags,location_type,latitude,longitude,humidity,temperature,pressure,pm10.0,pm1.0,pm2.5,pm2.5_a,pm2.5_b,pm2.5_alt"
        HEADERS = {"X-API-Key": Variable.get("X-API-Key")}  
        #массив sensor_id по разным регионам, всего около 50
        sensor_ids = [40237, 30759, 24519, 46683, 227901, 4105, 163445, 140476, 74251, 3151, 32981, 101511, 176857, 199317, 104076, 200747, 290700, 165897, 305846, 283278, 305642, 293859, 225711, 94253, 183603, 181333, 125083, 131429, 159361, 256435, 97711, 311457, 128039, 93747, 283510, 93783, 261179, 93801, 294545, 174393, 93999, 225873, 33495, 11178, 222719, 296551, 182167, 137722, 189007, 209723, 310811, 241901, 237041]
        results = []
        for sensor_id in sensor_ids:
            try:
                url = DATA_API_URL.format(sensor_id=sensor_id)  #подстановка sensor_id в URL
                response = requests.get(url, timeout=1000, headers=HEADERS)
                response.raise_for_status()
                results.append(response.json())

            except requests.exceptions.ConnectionError:
                log.error("Нет соединения с API")
            except requests.exceptions.Timeout:
                log.error("Превышено время ожидания ответа от API")
            except requests.exceptions.HTTPError as e:
                status = e.response.status_code
                if status == 400:
                    log.error("[sensor=%s] Неверный запрос (InvalidParameterValueError): проверьте параметры", sensor_id) 
                elif status == 403:
                    log.error("[sensor=%s] Доступ запрещён (403): проверьте API-ключ", sensor_id)  
                elif status == 404:
                    log.error("[sensor=%s] Датчик не найден (NotFoundError)", sensor_id)  
                elif status == 429:
                    log.error("[sensor=%s] Превышен лимит запросов (429)", sensor_id) 
                else:
                    log.error("[sensor=%s] HTTP-ошибка: %s", sensor_id, e) 
            except requests.exceptions.RequestException as e:
                log.error("[sensor=%s] Неожиданная ошибка запроса: %s", sensor_id, e)

        return results

    @task
    def save_data_to_Mongo(results):

        # Получаем URI подключения к Mongo из Variables Airflow
        mongo_uri = Variable.get("mongo_uri")
        
        # Запись в MongoDB
        try:
            with MongoClient(mongo_uri) as client:
                db = client["source_db"]
                collection = db["sensors_data"]
                if "sensors_data" not in db.list_collection_names():
                    db.create_collection("sensors_data")
                result = collection.insert_many(results)
                log.info("[mongo] Данные успешно сохранены: %s", result.inserted_ids)
            return True
        except PyMongoError as e:
            log.error("[mongo] Ошибка при сохранении документа: %s", e)
            return False        
    
    results = fetch_api_data()
    save_data_to_Mongo(results)
    

extract_and_save_data() 