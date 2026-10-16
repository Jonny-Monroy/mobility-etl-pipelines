"""Pipeline 2 · Estados de intersecciones semafóricas (versión desensibilizada).

En producción este pipeline:
  * descarga el reporte de estados del proveedor TSS desde una carpeta
    compartida (cuenta de servicio de Google),
  * lo parsea con expresiones regulares (secciones ALARM / OFFLINE / NOTE),
  * cruza el resultado contra la lista maestra de intersecciones,
  * lo persiste en Google Sheets y BigQuery, y
  * actualiza el campo ESTADO_OPERACION en la red semafórica de Oracle.

Aquí se publica el parser y el mapeo de estados — la lógica estable del
pipeline — con los conectores externos como interfaces.
"""
from __future__ import annotations

import re
from typing import Protocol

import pandas as pd

MAPA_ESTADOS = {
    "ALARM": "En falla",
    "OFFLINE": "Sin conexión ETB",
    "NOTE": "Operación sin conexión",
}

_PATRON_SECCION = re.compile(r"^---\s*(ALARM|OFFLINE|NOTE)\s*---", re.IGNORECASE)
_PATRON_INTERSECCION = re.compile(r"^INTERSECCION:\s*(?P<nombre>.+?)\s*,\s*"
                                  r"ID:\s*(?P<id>\d+)\s*,\s*"
                                  r"HORA:\s*(?P<hora>[\d:]+)")


class DescargadorReporte(Protocol):
    def ultima_version(self) -> str: ...


class PersistidorEstados(Protocol):
    def guardar(self, df: pd.DataFrame) -> None: ...


def parsear(texto: str) -> pd.DataFrame:
    """Convierte el texto plano del reporte TSS en un DataFrame de estados."""
    filas: list[dict] = []
    seccion_actual = None
    for linea in texto.splitlines():
        linea = linea.strip()
        m = _PATRON_SECCION.match(linea)
        if m:
            seccion_actual = m.group(1).upper()
            continue
        m = _PATRON_INTERSECCION.match(linea)
        if m and seccion_actual:
            filas.append({
                "id_interseccion": int(m.group("id")),
                "interseccion": m.group("nombre").strip(),
                "estado": MAPA_ESTADOS[seccion_actual],
                "seccion_reporte": seccion_actual,
                "hora_evento": m.group("hora"),
            })
    return pd.DataFrame(filas, columns=[
        "id_interseccion", "interseccion", "estado",
        "seccion_reporte", "hora_evento",
    ])


def consolidar(reportes: pd.DataFrame,
               maestra: pd.DataFrame) -> pd.DataFrame:
    """Cruza los estados con la lista maestra y marca los NO REPORTADOS."""
    cruce = maestra.merge(
        reportes.drop_duplicates("id_interseccion"),
        on="id_interseccion", how="left",
    )
    cruce["estado"] = cruce["estado"].fillna("Operación normal")
    return cruce


def ejecutar(descargador: DescargadorReporte,
             persistidores: list[PersistidorEstados],
             maestra: pd.DataFrame) -> pd.DataFrame:
    texto = descargador.ultima_version()
    estados = consolidar(parsear(texto), maestra)
    for p in persistidores:
        p.guardar(estados)
    return estados
