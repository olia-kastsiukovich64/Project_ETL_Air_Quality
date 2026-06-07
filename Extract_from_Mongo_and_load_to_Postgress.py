
from datetime import datetime, timezone
import logging
from pymongo import MongoClient

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.operators.python import get_current_context

logger = logging.getLogger(__name__)


@dag(
    dag_id="Extract_from_Mongo_and_load_to_Postgress",
    start_date=datetime(2026, 6, 7),
    schedule="*/30 * * * *",
    catchup=True,
    tags=["Project_ETL_Air_Quality"]
)
def extract_and_save_data():

    @task
    def extract_and_prepare():
        try:
            mongo_uri = Variable.get("mongo_uri")
        except Exception as e:
            logger.error(f"Не удалось получить mongo_uri из Variables: {e}")
            raise

        # Получаем временной интервал запуска DAG
        context = get_current_context()
        window_start = context["data_interval_start"]
        window_end = context["data_interval_end"]

        # Конвертируем datetime → Unix timestamp (int), т.к. в Mongo time_stamp хранится как int
        ts_start = int(window_start.replace(tzinfo=timezone.utc).timestamp())
        ts_end = int(window_end.replace(tzinfo=timezone.utc).timestamp())

        logger.info(f"Запрос данных за период: {window_start} — {window_end}")

        try:
            with MongoClient(mongo_uri) as client:
                coll = client["source_db"]["sensors_data"]
                rows = list(coll.find({
                    "time_stamp": {"$gte": ts_start, "$lt": ts_end}
                }))
        except Exception as e:
            logger.error(f"Ошибка при чтении из MongoDB: {e}")
            raise

        if not rows:
            logger.info("Нет данных за этот период")
            return []

        # Формируем кортежи из реальных полей документа (сохраняем все данные как есть)
        prepared_rows = []
        for r in rows:
            try:
                sensor = r["sensor"]
                prepared_rows.append((
                    r.get("api_version"),
                    datetime.fromtimestamp(r["time_stamp"], tz=timezone.utc),
                    datetime.fromtimestamp(r["data_time_stamp"], tz=timezone.utc),
                    sensor.get("sensor_index"),
                    sensor.get("name"),
                    sensor.get("location_type"),
                    sensor.get("latitude"),
                    sensor.get("longitude"),
                    sensor.get("channel_flags"),
                    sensor.get("confidence"),
                    sensor.get("humidity"),
                    sensor.get("temperature"),
                    sensor.get("pressure"),
                    sensor.get("pm1.0"),
                    sensor.get("pm2.5"),
                    sensor.get("pm2.5_a"),
                    sensor.get("pm2.5_b"),
                    sensor.get("pm2.5_alt"),
                    sensor.get("pm10.0"),
                ))
            except (KeyError, TypeError) as e:
                logger.warning(f"Пропущена запись из-за ошибки структуры: {e} | Документ: {r.get('_id')}")
                continue

        logger.info(f"Подготовлено записей для вставки: {len(prepared_rows)}")
        return prepared_rows

    @task
    def load_to_postgres(prepared_rows):
        if not prepared_rows:
            logger.info("Нет данных для вставки в Postgres")
            return "no_data"

        pg_hook = PostgresHook(postgres_conn_id="warehouse_postgres_conn")

        # Создаём схему и таблицу, record_id — автоинкрементируемый первичный ключ.
        create_sql = """
            CREATE TABLE IF NOT EXISTS raw.data_sensors (
                record_id       SERIAL PRIMARY KEY,
                api_version     TEXT,
                time_stamp      TIMESTAMPTZ,
                data_time_stamp TIMESTAMPTZ,
                sensor_index    INTEGER,
                name            TEXT,
                location_type   INTEGER,
                latitude        DOUBLE PRECISION,
                longitude       DOUBLE PRECISION,
                channel_flags   INTEGER,
                confidence      INTEGER,
                humidity        INTEGER,
                temperature     INTEGER,
                pressure        DOUBLE PRECISION,
                pm1_0           REAL,
                pm2_5           REAL,
                pm2_5_a         REAL,
                pm2_5_b         REAL,
                pm2_5_alt       REAL,
                pm10_0          REAL,
                load_ts         TIMESTAMPTZ DEFAULT NOW()
            );
        """

        insert_sql = """
            INSERT INTO raw.data_sensors (
                api_version, time_stamp, data_time_stamp,
                sensor_index, name, location_type,
                latitude, longitude, channel_flags, confidence,
                humidity, temperature, pressure,
                pm1_0, pm2_5, pm2_5_a, pm2_5_b, pm2_5_alt, pm10_0
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        try:
            conn = pg_hook.get_conn()
            with conn.cursor() as cur:
                cur.execute('CREATE SCHEMA IF NOT EXISTS raw;')
                cur.execute(create_sql)
                cur.executemany(insert_sql, prepared_rows)
            conn.commit()
        except Exception as e:
            logger.error(f"Ошибка при записи в Postgres: {e}")
            conn.rollback()
            raise

        logger.info(f"Успешно вставлено записей: {len(prepared_rows)}")
        return "success"

    sensor_raw_data = extract_and_prepare()
    load_to_postgres(sensor_raw_data)


extract_and_save_data()
