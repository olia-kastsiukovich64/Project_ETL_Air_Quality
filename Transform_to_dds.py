from datetime import datetime
import logging
import reverse_geocoder as rg

from pydantic import BaseModel, Field, ValidationError
from typing import Literal  

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.python import get_current_context

logger = logging.getLogger(__name__)

# Определяем Dataset для запуска DAG загрузки в Clickhouse 
sensors_dataset = Dataset("postgres://raw_valid_sensors_data")

# Описание Pydantic модели
class SensorModel(BaseModel):
    record_id: int = Field(gt=0)
    sensor_index: int = Field(gt=0)  # Индекс сенсора должен быть положительным
    name: str = Field(max_length=64)
    location_type: Literal[0, 1]  # 0 = Outside, 1 = Inside
    latitude: float = Field(ge=-90, le=90)  # Широта должна быть в пределах -90 до 90
    longitude: float = Field(ge=-180, le=180)  # Долгота должна быть в пределах -180 до 180
    channel_flags: int = Field(ge=0)  
    confidence: int = Field(ge=60, le=100)  # Уровень доверия к данным датчика в процентах от 60
    humidity: int = Field(ge=0, le=100)  # Влажность в процентах от 0 до 100
    temperature: int = Field(ge=-76, le=176)   # Диапазон при переводе в градусы -60 /+80 °C
    pressure: float = Field(gt=0)  # Давление должно быть положительным
    pm1_0: float = Field(ge=0)  # Концентрации частиц не могут быть отрицательными и содержат не более 2-ух знаков после запятой
    pm2_5: float = Field(ge=0)
    pm2_5_a: float = Field(ge=0)
    pm2_5_b: float = Field(ge=0)
    pm2_5_alt: float = Field(ge=0)
    pm10_0: float = Field(ge=0)
    time_stamp: datetime
    

@dag(
    dag_id="Extract_from_raw_and_load_to_dds",
    start_date=datetime(2026, 6, 10),
    schedule="*/30 * * * *",  # Запуск каждые 30 минут
    catchup=True, #нужен чтобы обработать пропущенные интервалы
    max_active_runs=1,
    tags=["Project_ETL_Air_Quality"]
)
def transform_data_and_load_to_dds():

    @task
    def extract_and_validate_data():
        
        context = get_current_context()
        window_start = context["data_interval_start"]
        window_end = context["data_interval_end"]

        valid_sensor_data = []

        query = """
            SELECT *
            FROM raw.data_sensors
            WHERE load_ts >= %(window_start)s AND load_ts <  %(window_end)s; 
        """
        
        pg_hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")
        conn = pg_hook.get_conn()

        try:
            with conn.cursor() as cursor:
                cursor.execute(query, {"window_start": window_start, "window_end": window_end})

                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()

            for row in rows:
                row_dict = dict(zip(columns, row))
                try:
                    sensor_data = SensorModel.model_validate(row_dict)
                    valid_sensor_data.append(sensor_data.model_dump(mode="json"))
                except ValidationError as exc:
                    logger.warning(
                        "Пропущена невалидная строка sensor_index=%s. Ошибка: %s",
                        row_dict.get("sensor_index"),
                        exc,
                    )

        except Exception as exc:
            logger.error("Ошибка при выполнении запроса к БД: %s", exc)
            raise 

        finally:
            conn.close()

        return valid_sensor_data 

    # Публикуем Dataset-событие через outlets 
    @task(outlets=[sensors_dataset]) 
    def load_to_dds(valid_sensor_data):
        if not valid_sensor_data:
            logger.info("Нет данных для вставки в dds")
            return "no_data"

        pg_hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")

        create_sql_1 = """
            CREATE TABLE IF NOT EXISTS dds.sensors (
                sensor_index    INTEGER PRIMARY KEY,
                name            TEXT,
                location_type   INTEGER,
                channel_flags   INTEGER,
                confidence      INTEGER,
                load_ts         TIMESTAMPTZ DEFAULT NOW()
            );
        """

        create_sql_2 = """
            CREATE TABLE IF NOT EXISTS dds.records (
                record_id       INTEGER PRIMARY KEY,
                sensor_index    INTEGER REFERENCES dds.sensors(sensor_index),
                time_stamp      TIMESTAMPTZ,
                load_ts         TIMESTAMPTZ DEFAULT NOW()
            );
        """
       
        create_sql_3 = """
            CREATE TABLE IF NOT EXISTS dds.weather_condition (
                record_id       INTEGER PRIMARY KEY REFERENCES dds.records(record_id),
                humidity        INTEGER,
                temperature     INTEGER,
                pressure        DOUBLE PRECISION,
                load_ts         TIMESTAMPTZ DEFAULT NOW()
            );
        """
        create_sql_4 = """
            CREATE TABLE IF NOT EXISTS dds.particles (
                record_id       INTEGER PRIMARY KEY REFERENCES dds.records(record_id),
                pm1_0           REAL,
                pm2_5           REAL,
                pm2_5_a         REAL,
                pm2_5_b         REAL,
                pm2_5_alt       REAL,
                pm10_0          REAL, 
                load_ts         TIMESTAMPTZ DEFAULT NOW()
            );
        """

        create_sql_5 = """
            CREATE TABLE IF NOT EXISTS dds.sensor_location (
                sensor_index    INTEGER PRIMARY KEY,
                latitude        DOUBLE PRECISION,
                longitude       DOUBLE PRECISION,
                city            TEXT,
                admin_area      TEXT,
                country_code    CHAR(2), 
                load_ts         TIMESTAMPTZ DEFAULT NOW()
            );
        """


        sensors_rows = []
        records_rows = []
        weather_rows = []
        particles_rows = []
        

        for r in valid_sensor_data:
            sensors_rows.append((
                r["sensor_index"], r["name"], r["location_type"],
                r["channel_flags"], r["confidence"]
            ))
            records_rows.append((
                r["record_id"], r["sensor_index"], r["time_stamp"]
            ))
            weather_rows.append((
                r["record_id"], r["humidity"], r["temperature"], r["pressure"]
            ))
            particles_rows.append((
                r["record_id"], r["pm1_0"], r["pm2_5"], r["pm2_5_a"],
                r["pm2_5_b"], r["pm2_5_alt"], r["pm10_0"]
            ))
            
        
        # собираем уникальные координаты по sensor_index

        unique_sensors = {}
        for r in valid_sensor_data:
            unique_sensors[r["sensor_index"]] = (r["latitude"], r["longitude"])

        sensor_indexes = list(unique_sensors.keys())
        coords = list(unique_sensors.values())

        results = rg.search(coords, mode=1)  # получаем через reverse_geocoder по координатам город и страну

        coord_rows = []
        for sensor_index, (lat, lon), res in zip(sensor_indexes, coords, results):
            coord_rows.append((
                sensor_index, lat, lon, res["name"], res["admin1"], res["cc"]
            ))
        

        insert_sql_1 = """
            INSERT INTO dds.sensors (
                sensor_index, name, location_type, channel_flags, confidence
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (sensor_index) DO NOTHING;
        """

        insert_sql_2 = """
            INSERT INTO dds.records (
                record_id, sensor_index, time_stamp 
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (record_id) DO NOTHING;
        """

        insert_sql_3 = """
            INSERT INTO dds.weather_condition (
                record_id, humidity, temperature, pressure
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (record_id) DO UPDATE
                SET humidity = EXCLUDED.humidity,
                temperature = EXCLUDED.temperature,
                pressure = EXCLUDED.pressure;
        """

        insert_sql_4 = """
            INSERT INTO dds.particles (
                record_id, pm1_0, pm2_5, pm2_5_a, pm2_5_b, pm2_5_alt, pm10_0
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (record_id) DO UPDATE 
                SET pm1_0 = EXCLUDED.pm1_0,
                    pm2_5 = EXCLUDED.pm2_5,
                    pm2_5_a = EXCLUDED.pm2_5_a,
                    pm2_5_b = EXCLUDED.pm2_5_b,
                    pm2_5_alt = EXCLUDED.pm2_5_alt,
                    pm10_0 = EXCLUDED.pm10_0;
        """    

        insert_sql_5 = """
            INSERT INTO dds.sensor_location (
                sensor_index, latitude, longitude, city, admin_area, country_code
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (sensor_index) DO NOTHING;
        """


        conn = None
        try:
            conn = pg_hook.get_conn()
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS dds;")
                cur.execute(create_sql_1)
                cur.execute(create_sql_2)
                cur.execute(create_sql_3)
                cur.execute(create_sql_4)
                cur.execute(create_sql_5)
                cur.executemany(insert_sql_1, sensors_rows)
                cur.executemany(insert_sql_2, records_rows)
                cur.executemany(insert_sql_3, weather_rows)
                cur.executemany(insert_sql_4, particles_rows)
                cur.executemany(insert_sql_5, coord_rows)
            conn.commit()
        except Exception as e:
            logger.error("Ошибка при записи в Postgres: %s", e)
            if conn is not None:
                conn.rollback()
            raise
        finally:
            if conn is not None:
                conn.close()

        logger.info("Успешно вставлено записей: %d", len(valid_sensor_data))
        return "success"

    
    load_to_dds(extract_and_validate_data())




transform_data_and_load_to_dds()