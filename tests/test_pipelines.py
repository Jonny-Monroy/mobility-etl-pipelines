"""Tests de los pipelines desensibilizados.

Ejecutar:  pytest tests/ -v
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import pytest

from fixtures.reporte_tss import REPORTE_TSS
from pipelines.estados_semaforos import parsear, consolidar
from pipelines.waze_velocidades import ConfigPipeline, transformar
from pipelines.zonaparqueo_sync import construir_wkt, normalizar_tramos


# ---------------------------------------------------------------- Pipeline 1
def test_waze_agrega_por_cuarto_hora():
    base = int(pd.Timestamp("2023-11-14 10:00:00", tz="UTC").timestamp() * 1000)
    df = pd.DataFrame([
        {"tramo": "STGV_CARACAS", "velocidad_kmh": 20.0,
         "update_time_ms": base + 0 * 60_000},
        {"tramo": "STGV_CARACAS", "velocidad_kmh": 40.0,
         "update_time_ms": base + 10 * 60_000},
        {"tramo": "STGV_CARACAS", "velocidad_kmh": 90.0,
         "update_time_ms": base + 61 * 60_000},
        {"tramo": "TRAMO_DESCONOCIDO", "velocidad_kmh": 10.0,
         "update_time_ms": base},
    ])
    out = transformar(df, ConfigPipeline())
    assert len(out) == 2
    fila = out[(out.tramo == "STGV_CARACAS") & (out.cuarto_hora == 20)]
    assert fila.velocidad_media.iloc[0] == 30.0
    assert fila.n_observaciones.iloc[0] == 2


# ---------------------------------------------------------------- Pipeline 2
def test_parser_estados_cuenta_secciones():
    estados = parsear(REPORTE_TSS)
    assert len(estados) == 6
    assert set(estados.seccion_reporte) == {"ALARM", "OFFLINE", "NOTE"}
    assert estados[estados.id_interseccion == 1001].estado.iloc[0] == "En falla"
    assert estados[estados.id_interseccion == 2003].estado.iloc[0] == "Sin conexión ETB"


def test_consolidar_marca_no_reportados():
    estados = parsear(REPORTE_TSS)
    maestra = pd.DataFrame({
        "id_interseccion": [1001, 1002, 9999],
        "interseccion": ["AK 7 # 40", "AV CARACAS # 63", "CL 170 # A 19"],
    })
    res = consolidar(estados, maestra)
    assert len(res) == 3
    assert res[res.id_interseccion == 9999].estado.iloc[0] == "Operación normal"
    assert res[res.id_interseccion == 1001].estado.iloc[0] == "En falla"


# ---------------------------------------------------------------- Pipeline 3
def test_zonaparqueo_tipos_y_wkt():
    tramos = [
        {"codigo_tramo": "ZPP-001", "sector": "CHAPINERO", "plazas": "12",
         "tarifa_hora": "7000.5", "es_operativo": True,
         "lat_inicio": 4.67, "lon_inicio": -74.05,
         "lat_fin": 4.68, "lon_fin": -74.06},
        {"codigo_tramo": "ZPP-002", "sector": "USAQUEN", "plazas": "no_disponible",
         "tarifa_hora": "5400", "es_operativo": False,
         "lat_inicio": None, "lon_inicio": -74.04,
         "lat_fin": 4.70, "lon_fin": -74.03},
    ]
    df = normalizar_tramos(tramos)
    assert len(df) == 1
    assert df.plazas.iloc[0] == 12

    wkt = construir_wkt(df).wkt.iloc[0]
    assert wkt == "LINESTRING (-74.05 4.67, -74.06 4.68)"


def test_zonaparqueo_rechaza_payload_incompleto():
    with pytest.raises(ValueError):
        normalizar_tramos([{"codigo_tramo": "ZPP-003"}])
