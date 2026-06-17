from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


# Описание Pydantic модели
class SensorModel(BaseModel):
    record_id: int = Field(gt=0)
    sensor_index: int = Field(gt=0)  # Индекс сенсора должен быть положительным
    name: str = Field(max_length=64)
    location_type: Literal[0, 1]  # 0 = Outside, 1 = Inside
    latitude: float = Field(ge=-90, le=90)     # Широта должна быть в пределах -90 до 90
    longitude: float = Field(ge=-180, le=180)   # Долгота должна быть в пределах -180 до 180
    channel_flags: int = Field(ge=0)
    confidence: int = Field(ge=60, le=100)      # Уровень доверия к данным датчика в процентах от 60
    humidity: int = Field(ge=0, le=100)         # Влажность в процентах от 0 до 100
    temperature: int = Field(ge=-76, le=176)    # Диапазон при переводе в градусы -60 /+80 °C
    pressure: float = Field(gt=0)               # Давление должно быть положительным
    pm1_0: float = Field(ge=0)                  # Концентрации частиц не могут быть отрицательными и содержат не более 2-ух знаков после запятой
    pm2_5: float = Field(ge=0, le=500)          # Концентрация частиц не должна превышать 500 иначе получаем ошибку при конвертации AQI
    pm2_5_a: float = Field(ge=0, le=500)
    pm2_5_b: float = Field(ge=0, le=500)
    pm2_5_alt: float = Field(ge=0, le=500)
    pm10_0: float = Field(ge=0)
    time_stamp: datetime