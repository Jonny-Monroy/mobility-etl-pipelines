"""Pipeline 1 · Velocidades Waze por corredor (versión desensibilizada).

En producción (Secretaría Distrital de Movilidad) este pipeline:
  * extrae rutas Waze de un índice de Elasticsearch con scroll paginado,
  * agrega la velocidad media por tramo y cuarto de hora,
  * carga el resultado a la geodatabase corporativa (Oracle/SDE) que alimenta
    los servicios de monitoreo y los tableros de comparación de velocidades.

Esta versión publica la lógica de transformación (el corazón del pipeline) con
los conectores de extracción/carga como interfaces configurables por entorno:
sin hosts, credenciales ni datos institucionales.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Protocol

import pandas as pd

CORREDORES = [
    "STGV_AUTONORTE", "STGV_CARACAS", "STGV_C80", "STGV_SUBA", "STGV_NQS",
    "STGV_AMERICAS", "STGV_CIRCUNVALAR", "STGV_A26", "STGV_A07",
    "STGV_BOYACA", "STGV_CIUDADCALI", "STGV_A68", "STGV_ALIADOSUR",
    "STGV_ACIUDADVERDE",
]
CORREDORES_BICIRUTA = ["DIM_BIC_1", "DIM_BIC_2", "DIM_BIC_3"]

DESFASE_SERVIDOR_MS = 5 * 3600 * 1000
VENTANA_MINUTOS = 15


@dataclass
class ConfigPipeline:
    corredores: list[str] = field(default_factory=lambda: CORREDORES + CORREDORES_BICIRUTA)
    desfase_servidor_ms: int = DESFASE_SERVIDOR_MS
    ventana_minutos: int = VENTANA_MINUTOS


class ExtractorRutas(Protocol):
    def extraer(self, corredores: Iterable[str],
                desde_ms: int, hasta_ms: int) -> pd.DataFrame: ...


class CargadorVelocidades(Protocol):
    def cargar(self, df: pd.DataFrame, tabla: str) -> int: ...


def transformar(df: pd.DataFrame, cfg: ConfigPipeline) -> pd.DataFrame:
    """Ajusta el desfase horario del servidor y agrega por tramo y cuarto de hora.

    Columnas de entrada : tramo, velocidad_kmh, update_time_ms (UTC)
    Columnas de salida  : tramo, fecha, cuarto_hora, velocidad_media, n_observaciones
    """
    d = df.copy()
    d["ts"] = (
        pd.to_datetime(
            d["update_time_ms"] - cfg.desfase_servidor_ms, unit="ms", utc=True
        )
        .dt.tz_localize(None)
    )
    d = d[d["tramo"].isin(cfg.corredores)]
    d["fecha"] = d["ts"].dt.strftime("%Y-%m-%d")
    d["cuarto_hora"] = d["ts"].dt.hour * (60 // cfg.ventana_minutos) + (
        d["ts"].dt.minute // cfg.ventana_minutos
    )
    return (
        d.groupby(["tramo", "fecha", "cuarto_hora"], as_index=False)
        .agg(
            velocidad_media=("velocidad_kmh", "mean"),
            n_observaciones=("velocidad_kmh", "size"),
        )
        .round({"velocidad_media": 2})
    )


def ejecutar(extractor: ExtractorRutas, cargador: CargadorVelocidades,
             cfg: ConfigPipeline, desde_ms: int, hasta_ms: int,
             tabla: str = "VEL_CORREDORES") -> int:
    """Orquestación extract → transform → load."""
    crudo = extractor.extraer(cfg.corredores, desde_ms, hasta_ms)
    agregado = transformar(crudo, cfg)
    return cargador.cargar(agregado, tabla)
