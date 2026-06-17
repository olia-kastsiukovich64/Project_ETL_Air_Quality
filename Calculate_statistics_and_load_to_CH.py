import clickhouse_connect
from datetime import datetime, timezone
import logging
from utils.fetch_countries_api import fetch_countries 

import aqi

from airflow.datasets import Dataset 
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import get_current_context

logger = logging.getLogger(__name__)

sensors_dataset = Dataset("postgres:/raw_valid_sensors_data")
from utils.tg_callbacks import on_failure_tg 

@dag(
    dag_id="Calculate_statistic_and_load_to_CH",
    start_date=datetime(2026, 6, 10),
    schedule=[sensors_dataset], 
    catchup=False, 
    max_active_runs=1,
    tags=["Project_ETL_Air_Quality"],
    on_failure_callback=on_failure_tg
)
def calculate_statistic_and_load_to_ch():

    @task(inlets=[sensors_dataset], on_failure_callback=on_failure_tg)
    def calculate_statistics():
        context = get_current_context()
        window_start = context["data_interval_start"]
        window_end = context["data_interval_end"]

        ts_start = int(window_start.replace(tzinfo=timezone.utc).timestamp())
        ts_end = int(window_end.replace(tzinfo=timezone.utc).timestamp())

        try:
            postgres_hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")

            query_1 = """
                SELECT 
                    s.sensor_index,
                    l.city,
                    l.admin_area,
                    l.country_code,
                    l.latitude,
                    l.longitude, 
                    count(*) AS measurements,
                    avg(pm2_5) AS pm2_5_avg,
                    avg(pm2_5_a) AS pm2_5_a_avg,
                    avg(pm2_5_b) AS pm2_5_b_avg,
                    avg(pm2_5_alt) AS pm2_5_alt_avg,
                    max(pm2_5) AS pm2_5_max,
                    min (pm2_5) AS pm2_5_min,
                    count(*) FILTER (WHERE pm2_5 > 35) AS exceed_35_cnt,
                    avg(pm1_0) AS pm1_0_avg,
                    avg(pm10_0) AS pm10_0_avg,
                    avg(temperature)              AS temperature_avg,
                    avg(humidity)                 AS humidity_avg,
                    avg(pressure)                 AS pressure_avg,
                    avg(confidence)               AS confidence_avg   
                FROM dds.records r
                JOIN dds.particles p          ON r.record_id   = p.record_id
                JOIN dds.weather_condition w  ON r.record_id   = w.record_id
                JOIN dds.sensors s            ON r.sensor_index = s.sensor_index
                JOIN dds.sensor_location l    ON r.sensor_index = l.sensor_index
                WHERE r.time_stamp >= to_timestamp(%(window_start)s) AND r.time_stamp <  to_timestamp(%(window_end)s)
                GROUP BY s.sensor_index, l.city, l.admin_area, l.country_code, l.latitude, l.longitude; 
            """

            results = postgres_hook.get_records(
                query_1,
                parameters={"window_start": ts_start, "window_end": ts_end})
            
        except Exception as e:
            logger.error(
                "Ошибка при выполнении запроса к PostgreSQL за окно %s – %s: %s",
                window_start, window_end, e
            )
            raise    

        
        if not results:
            logger.warning("Нет данных за окно %s – %s", window_start, window_end)
            return []
        
        all_metrics = []  # собираем метрики по каждому сенсору

        for row in results:
            (
                sensor_index, city, admin_area, country_code,
                latitude, longitude, measurements,
                pm2_5_avg,pm2_5_a_avg, pm2_5_b_avg, pm2_5_alt_avg, pm2_5_max, pm2_5_min, exceed_35_cnt,
                pm1_0_avg, pm10_0_avg,
                temperature_avg, humidity_avg, pressure_avg,
                confidence_avg
            ) = row

            # Рассчитываем AQI по стандарту US EPA через библиотеку python-aqi отдельно для каждого сенсора по его pm2_5_avg
            try:
                aqi_value = aqi.to_aqi([
                    (aqi.POLLUTANT_PM25, float(pm2_5_avg or 0))
                ])
            except (IndexError, ValueError) as e:
                logger.warning(
                    "Не удалось рассчитать AQI для sensor_index=%s, pm2_5_avg=%s: %s",
                    sensor_index, pm2_5_avg, e
                )
                aqi_value = 0    

            all_metrics.append({
                "window_start":    window_start.isoformat(),
                "window_end":      window_end.isoformat(),
                "sensor_index":    int(sensor_index   or 0),
                "city":            str(city           or ""),
                "admin_area":      str(admin_area     or ""),
                "country_code":    str(country_code   or ""),
                "latitude":        float(latitude    or 0),
                "longitude":       float(longitude    or 0),
                "measurements":    int(measurements   or 0),
                "pm2_5_avg":       float(pm2_5_avg    or 0),
                "pm2_5_a_avg":     float(pm2_5_a_avg  or 0),
                "pm2_5_b_avg":     float(pm2_5_b_avg  or 0),
                "pm2_5_alt_avg":   float(pm2_5_alt_avg or 0),
                "pm2_5_max":       float(pm2_5_max    or 0),
                "pm2_5_min":       float(pm2_5_min    or 0),
                "exceed_35_cnt":   int(exceed_35_cnt  or 0),
                "pm1_0_avg":       float(pm1_0_avg    or 0),
                "pm10_0_avg":      float(pm10_0_avg   or 0),
                "temperature_avg": float(temperature_avg or 0),
                "humidity_avg":    float(humidity_avg or 0),
                "pressure_avg":    float(pressure_avg or 0),
                "confidence_avg":  float(confidence_avg or 0),
                "aqi":             int(aqi_value)
                })
        
        logger.info("Посчитаны метрики за окно %s – %s: %s", window_start, window_end, all_metrics)
        return all_metrics  # возвращаем список словарей 


    @task(on_failure_callback=on_failure_tg)
    def load_to_ch(all_metrics):
        if not all_metrics:
            logger.warning("Метрики пустые, загрузка в ClickHouse пропущена.")
            return
        
        # Парсим строки обратно в объект datetime для ClickHouse
        for m in all_metrics:
            m["window_start"] = datetime.fromisoformat(m["window_start"]).replace(tzinfo=timezone.utc)
            m["window_end"]   = datetime.fromisoformat(m["window_end"]).replace(tzinfo=timezone.utc)

        try:
            ch_client = clickhouse_connect.get_client(
                host=Variable.get("clickhouse_host"),
                port=int(Variable.get("clickhouse_port")),
                username=Variable.get("clickhouse_user"),
                password=Variable.get("clickhouse_password"),
                database=Variable.get("clickhouse_database"),
            )
        except KeyError as e:
            logger.error("Отсутствует переменная Airflow для подключения к ClickHouse: %s", e)
            raise
        except Exception as e:
            logger.error("Не удалось подключиться к ClickHouse: %s", e)
            raise 

        # Создаём БД и таблицу
        ch_client.command("CREATE DATABASE IF NOT EXISTS marts")
        
        ch_client.command("""
            CREATE TABLE IF NOT EXISTS marts.air_quality_stats
            (
                window_start    DateTime,
                window_end      DateTime,
                sensor_index    Int32,
                city            String,
                admin_area      String,
                country_code    FixedString(2),
                latitude        Float64,
                longitude       Float64,
                measurements    UInt32,
                pm2_5_avg       Float64,
                pm2_5_a_avg     Float64,
                pm2_5_b_avg     Float64,
                pm2_5_alt_avg   Float64,                    
                pm2_5_max       Float64,
                pm2_5_min       Float64,
                exceed_35_cnt   UInt32,
                pm1_0_avg       Float64,
                pm10_0_avg      Float64,
                temperature_avg Float64,
                humidity_avg    Float64,
                pressure_avg    Float64,
                confidence_avg  Float64,
                aqi             UInt16
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(window_start)
            ORDER BY (country_code, city, sensor_index, window_start);
        """)

        # Вставляем список строк с метриками
        columns = [
            "window_start", "window_end",
            "sensor_index", "city", "admin_area", "country_code", 
            "latitude", "longitude", "measurements",
            "pm2_5_avg", "pm2_5_a_avg", "pm2_5_b_avg", "pm2_5_alt_avg", "pm2_5_max", "pm2_5_min", "exceed_35_cnt",
            "pm1_0_avg", "pm10_0_avg",
            "temperature_avg", "humidity_avg", "pressure_avg",
            "confidence_avg",
            "aqi"
        ]

        data = [
            [metrics[col] for col in columns]
            for metrics in all_metrics
        ]

        ch_client.insert(table="marts.air_quality_stats", data=data, column_names=columns)
        
      
        logger.info(
            "Загружено %s строк за окно %s – %s успешно записаны в ClickHouse",
            len(data), all_metrics[0]["window_start"], all_metrics[0]["window_end"]
            )


    load_to_ch(calculate_statistics())


calculate_statistic_and_load_to_ch() 