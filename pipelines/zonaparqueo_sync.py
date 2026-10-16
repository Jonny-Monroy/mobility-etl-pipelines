"""Pipeline 3 · Sincronización de Zonas de Parqueo Pago (versión desensibilizada).

En producción este pipeline se ejecuta programado por el Programador de
Tareas de Windows cada pocos minutos:
  * obtiene un token OAuth contra la API del operador,
  * descarga el catálogo de tramos (plazas, tarifas, señalización, coordenadas),
  * ajusta los tipos de campo según el diccionario de la geodatabase y
    construye la geometría de línea por coordenadas inicio/fin,
  * sincroniza (updates + inserts) el servicio hospedado del Portal GIS.

Aquí se publica la lógica de transformación de tipos y construcción de
geometrías, con autenticación y publicación como interfaces.
"""
from __future__ import annotations

import json
from typing import Protocol

import pandas as pd

MAPEO_TIPOS: dict[str, str] = {
    "plazas": "int",
    "tarifa_hora": "float",
    "es_operativo": "bool",
    "lat_inicio": "float",
    "lon_inicio": "float",
    "lat_fin": "float",
    "lon_fin": "float",
    "codigo_tramo": "str",
    "sector": "str",
}

ERRORES_PRECISION = {"plazas", "lat_inicio", "lon_inicio", "lat_fin", "lon_fin"}


class ClienteAPIParqueo(Protocol):
    def token(self) -> str: ...

    def tramos(self) -> list[dict]: ...


class PublicadorCapa(Protocol):
    def sincronizar(self, df: pd.DataFrame, id_campo: str = "codigo_tramo") -> int: ...


def normalizar_tramos(tramos: list[dict]) -> pd.DataFrame:
    """DataFrame tipado según el esquema destino + validaciones básicas."""
    df = pd.json_normalize(tramos)
    faltantes = set(MAPEO_TIPOS) - set(df.columns)
    if faltantes:
        raise ValueError(f"El payload no contiene: {sorted(faltantes)}")

    df = df[list(MAPEO_TIPOS)].copy()
    for campo, tipo in MAPEO_TIPOS.items():
        if tipo == "float":
            df[campo] = pd.to_numeric(df[campo], errors="coerce")
        elif tipo == "int":
            df[campo] = pd.to_numeric(df[campo], errors="coerce").astype("Int64")
        elif tipo == "bool":
            df[campo] = df[campo].astype(bool)
    df = df.dropna(subset=sorted(ERRORES_PRECISION))
    return df.reset_index(drop=True)


def construir_wkt(df: pd.DataFrame) -> pd.DataFrame:
    """Geometría LINESTRING WGS84 por tramo (inicio → fin)."""
    out = df.copy()
    out["wkt"] = out.apply(
        lambda r: (
            f"LINESTRING ({r.lon_inicio} {r.lat_inicio}, "
            f"{r.lon_fin} {r.lat_fin})"
        ),
        axis=1,
    )
    return out


def ejecutar(cliente: ClienteAPIParqueo, publicador: PublicadorCapa) -> int:
    tramos = cliente.tramos()
    df = construir_wkt(normalizar_tramos(tramos))
    return publicador.sincronizar(df)


def guardar_esquema_salida(df: pd.DataFrame, ruta: str) -> None:
    with open(ruta, "w", encoding="utf-8") as fh:
        json.dump({c: str(t) for c, t in df.dtypes.items()}, fh,
                  ensure_ascii=False, indent=2)
