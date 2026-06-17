
# ETL_Air_Quality_project

**Автор:** Olia Kastsiukovich

____

## **Оглавление**

- [Цель проекта](#цель-проекта)
- [Источники данных](#источники-данных)
- [Возможные применения](#возможные-применения)
- [Архитектура](#архитектура)
- [Этапы ETL-процесса](#этапы-etl-процесса)
- [Технологический стек](#технологический-стек)
- [Пайплайн](#пайплайн)
- [Запуск проекта](#запуск-проекта)

____

## **Цель проекта**

Проект предназначен для сбора, обработки и визуализации данных о качестве воздуха с сети датчиков PurpleAir по всему миру. Основная цель — построение ETL-пайплайна, который автоматически собирает измерения частиц (PM) и погодных параметров, рассчитывает индекс качества воздуха (AQI) и формирует аналитические витрины для мониторинга экологической обстановки в разных городах и странах.

____

## **Источники данных**

Проект использует следующие источники и библиотеки:

**PurpleAir API** — основной источник данных. Предоставляет измерения с датчиков качества воздуха: концентрации частиц (PM1.0, PM2.5, PM2.5_a/b/alt, PM10.0), температуру, влажность, давление, координаты и уровень доверия к данным. https://api.purpleair.com

**python-aqi** — библиотека для расчёта индекса качества воздуха (AQI) по стандарту US EPA на основе средних значений PM2.5.

**reverse_geocoder** — библиотека для обратного геокодирования: определение города, региона (admin_area) и кода страны по координатам датчика (latitude, longitude).

____

## **Возможные применения**

- Мониторинг качества воздуха в реальном времени по городам и странам
- Сравнение городов с наиболее высоким и низким уровнем AQI
- Анализ динамики AQI во времени (тренды)
- Выявление городов с превышением порога PM2.5 > 35
- Контроль расхождения каналов датчика (channel A vs channel B) для оценки исправности оборудования
- Исследование корреляции между влажностью и уровнем AQI

____

## **Архитектура**
<img width="2816" height="1536" alt="Gemini_Generated_Image_jrw0gejrw0gejrw0 (1)" src="https://github.com/user-attachments/assets/f1c8eb45-761c-474b-9f40-a9295f6e166f" />

Данные проходят последовательно через несколько слоёв хранения:

**PurpleAir API → MongoDB (Raw) → PostgreSQL (Raw → DDS) → ClickHouse (Marts) → Metabase**

- **MongoDB** — хранение «сырых» данных в исходном виде (JSON-документы от API).
- **PostgreSQL (Raw слой)** — структурированное хранение разобранных по полям измерений.
- **PostgreSQL (DDS слой)** — нормализованная модель данных с валидацией и геокодированием.
- **ClickHouse (Marts)** — денормализованная аналитическая витрина с агрегатами и AQI.
- **Metabase** — построение дашбордов и визуализация метрик.

____

## **Этапы ETL-процесса**

1. *Extraction (извлечение)*

Получение данных с датчиков через PurpleAir API и загрузка «сырых» документов в MongoDB.

2. *Transformation (обработка)*

- Извлечение полей из MongoDB и приведение к табличному виду в PostgreSQL.
- Валидация данных через Pydantic (диапазоны координат, температуры, влажности, уровень confidence и т.д.).
- Обратное геокодирование координат через reverse_geocoder.
- Нормализация: разбивка на справочные и фактовые таблицы DDS.

3. *Loading (загрузка)*

- Агрегация измерений по окну и сенсору, расчёт AQI через python-aqi.
- Загрузка итоговых метрик в аналитическую витрину ClickHouse.

____

## **Технологический стек**

| Компонент | Используемые технологии |
|:----------------:|:---------:|
| Язык | Python |
| Базы данных | MongoDB, PostgreSQL, ClickHouse |
| ETL / Оркестрация | Airflow |
| API | PurpleAir |
| Библиотеки | python-aqi, reverse_geocoder, Pydantic |
| Визуализация | Metabase |
| Уведомления | Telegram Bot |

____

## **Пайплайн**

Проект реализован в виде **4 DAG-ов**, связанных через расписание (cron) и Airflow Datasets.

### 1. Загрузка данных в MongoDB (Raw Layer)

*Get_APIdata_and_load_toMongo.py*

DAG обращается к PurpleAir API по списку из 65 датчиков в разных регионах и загружает «сырые» данные в MongoDB.

- **Расписание:** каждые 10 минут (`*/10 * * * *`).
- Обрабатывает ошибки соединения, таймаутов и HTTP-статусы (400, 403, 404, 429).
- **Результат:** коллекция `source_db.sensors_data` в MongoDB.

<img width="998" height="929" alt="Скрин1" src="https://github.com/user-attachments/assets/518f32b9-e819-487a-a6ef-3e37b8f75f10" />

### 2. Извлечение из MongoDB → PostgreSQL (Raw слой)

*Extract_from_Mongo_and_load_to_Postgress.py*

DAG читает данные из MongoDB за временное окно запуска и загружает их в таблицу `raw.data_sensors` в PostgreSQL.

- **Расписание:** каждые 30 минут (`*/30 * * * *`), `catchup=True`.
- Конвертирует Unix-timestamp в `TIMESTAMPTZ`, разбирает вложенный объект `sensor`.
- По завершении публикует Dataset-событие `raw_sensors` (триггер для DAG №3).
- **Результат:** структурированные измерения в `raw.data_sensors`.

<img width="1128" height="686" alt="Скрин2" src="https://github.com/user-attachments/assets/8a843f79-510a-44c1-863f-a69954534ff5" />

### 3. Трансформация Raw → DDS

*Transform_to_dds.py*

DAG берёт данные из `raw.data_sensors`, валидирует их через Pydantic-модель `SensorModel` и раскладывает в нормализованный слой DDS.

- **Расписание:** запуск по Dataset-событию `raw_sensors`.
- Валидация диапазонов (координаты, температура, влажность, confidence ≥ 60 и др.).
- Геокодирование координат через reverse_geocoder (город, регион, страна).
- **Результат:** таблицы `dds.sensors`, `dds.records`, `dds.weather_condition`, `dds.particles`, `dds.sensor_location`.
- Публикует Dataset-событие `raw_valid_sensors_data` (триггер для DAG №4).

<img width="1120" height="686" alt="Скрин3" src="https://github.com/user-attachments/assets/4a769c3b-4f9e-4fa8-97ec-fd9f22dfd02c" />

### 4. Формирование витрины в ClickHouse (Marts)

*Calculate_statistics_and_load_to_CH.py*

DAG агрегирует данные DDS по сенсору за временное окно, рассчитывает AQI через python-aqi и загружает метрики в ClickHouse.

- **Расписание:** запуск по Dataset-событию `raw_valid_sensors_data`.
- Считает средние/мин/макс значения PM, погодные показатели, число измерений и превышений PM2.5 > 35.
- Рассчитывает AQI по стандарту US EPA для каждого сенсора.
- **Результат:** таблица `marts.air_quality_stats` в ClickHouse.

<img width="1197" height="720" alt="Скрин4" src="https://github.com/user-attachments/assets/24dcf1ad-e8cc-41b7-be7d-5fbce44df528" />

____

## **Визуализация и аналитика**

Metabase используется для построения дашбордов с ключевыми метриками:

**- Air Quality — Overview** - http://localhost:3000/dashboard/2-air-quality-overview 

| Показатель | Что показывает |
|:----------------|:---------|
| Average of AQI | Средний индекс качества воздуха по сети |
| Number of Sensors | Количество активных датчиков |
| Total of Measurements | Общее число измерений |
| Distribution by AQI categories | Распределение датчиков по категориям AQI |
| Sensor geographic distribution | Карта расположения датчиков |

<img width="1061" height="870" alt="Скрин5" src="https://github.com/user-attachments/assets/423dee33-9cbf-4992-8e06-7621fce389b1" />

**- Sensor Data Overview and AQI/Humidity Correlation** http://localhost:3000/dashboard/4-sensor-data-overview-and-aqi-humidity-correlation 

| Показатель | Что показывает |
|:----------------|:---------|
| Sensor channel divergence | Расхождение каналов A/B (исправность датчика) |
| Humidity vs. AQI Correlation | Связь влажности и уровня AQI |
| PM2.5 > 35 exceedances by city | Города с превышением порога PM2.5 |

<img width="1075" height="945" alt="Скрин6" src="https://github.com/user-attachments/assets/ecd14adc-32fb-4e91-a2d9-ee1121a9e536" />

**- City Rankings & Trends**
http://localhost:3000/dashboard/3-city-aqi-analysis-dashboard 

| Показатель | Что показывает |
|:----------------|:---------|
| Cities with highest AQI | Города с наихудшим качеством воздуха |
| Cities with lowest AQI | Города с лучшим качеством воздуха |
| AQI Trends Over Time | Динамика AQI по городам во времени |

<img width="1049" height="885" alt="Скрин 7" src="https://github.com/user-attachments/assets/963b9832-9dba-460d-9a1e-1fa0aca7c644" />
____

## **Оркестрация и автоматизация**

Airflow отвечает за автоматический запуск ETL-процессов по расписанию (cron) и через Datasets. Логирование и уведомления о сбоях реализованы через Telegram Bot.

____

## **Запуск проекта**

1. Клонирование репозитория

```bash
git clone https://github.com/<your-account>/ETL_Air_Quality_project.git
```

2. Создание файла `.env` с данными для docker-compose

```bash
# PostgreSQL
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_DB=

# MongoDB
MONGO_INITDB_ROOT_USERNAME=
MONGO_INITDB_ROOT_PASSWORD=

# Airflow
AIRFLOW_WWW_USER_USERNAME=
AIRFLOW_WWW_USER_PASSWORD=

# ClickHouse
CLICKHOUSE_USER=
CLICKHOUSE_PASSWORD=
CLICKHOUSE_DB=

# Metabase
MB_DB_USER=
MB_DB_PASS=
MB_DB_DBNAME=
```

3. Запустить docker-compose

```bash
docker-compose up
```

4. Настроить переменные и подключения в Airflow

```bash
переменные (Variables):
X-API-Key =            # ключ PurpleAir API
mongo_uri =
clickhouse_host =
clickhouse_port =
clickhouse_user =
clickhouse_password =
clickhouse_database =
telegram_chat_id =
telegram_token =

подключения (Connections):
warehouse_postgres_conn
```

5. Запустить DAG-и

```bash
http://localhost:8080
```
