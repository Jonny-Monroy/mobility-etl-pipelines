# Mobility ETL Pipelines · Patrones de pipelines ETL de misión crítica

> Versiones **desensibilizadas y documentadas** de tres pipelines ETL que construí y mantuve en producción en la **Secretaría Distrital de Movilidad de Bogotá** (2024–2025). Cada módulo conserva la lógica de transformación real; los conectores (APIs, bases de datos, servicios GIS) quedan como interfaces con configuración por entorno. **Sin credenciales, sin hosts, sin datos institucionales.**

| Pipeline | Fuente → Destino (producción) | Logro |
|---|---|---|
| **1. Velocidades Waze** | Elasticsearch (scroll) → pandas → Oracle/SDE → servicios GIS | Agregación por tramo/cuarto de hora que alimenta el monitoreo de los 14 corredores principales |
| **2. Estados de semáforos** | Reporte TSS (Drive, cuenta de servicio) → parser regex → Sheets + **BigQuery** → update Oracle | Estado operativo de la red semafórica consolidado cada hora |
| **3. Zonas de Parqueo Pago** | API operador (OAuth) → staging geodatabase → servicio hospedado del Portal | Sincronización automática programada cada pocos minutos, con logs rotativos desde 2023 |

## Por qué este repo

En las entrevistas técnicas lo importante no es el acceso a la infraestructura, sino **cómo se diseñan** los pipelines: validación, tipos, idempotencia, desacoplamiento de credenciales y tests. Aquí está ese diseño:

* **Lógica de transformación pura y testeable** — `parsear()`, `transformar()`, `normalizar_tramos()` no dependen de red ni credenciales.
* **Interfaces (Protocol) para fuentes y destinos** — cambiar BigQuery por Postgres o el servicio GIS por otro no toca la lógica de negocio.
* **Correcciones de datos del mundo real** — desfase horario del servidor (+5h), duplicados del proveedor, tipos inconsistentes en el payload, intersecciones no reportadas → "Operación normal".
* **pytest incluido** — `pytest tests/ -v` (6 tests: agregación por cuarto de hora, parser de secciones, consolidación con maestra, casting de tipos, geometría WKT, payload inválido).

## Estructura

```
├── pipelines/
│   ├── waze_velocidades.py     # scroll → agregación 15 min → load
│   ├── estados_semaforos.py    # regex parser TSS → maestra → Sheets/BigQuery/Oracle
│   └── zonaparqueo_sync.py     # OAuth → tipos dict → WKT → sync hosted layer
├── fixtures/reporte_tss.py     # muestra del formato parseado
└── tests/test_pipelines.py
```

## Contexto del sistema real (desensibilizado)

```
            ┌─ Elasticsearch (rutas Waze) ─┐
Fuentes ────┤  Drive/Sheets (reporte TSS)  ├──► pandas: validar/transformar
            └─ API operador ZPP (OAuth)  ──┘        │
                                                    ▼
                                    Oracle (SDE corporativa) + BigQuery + Portal GIS
                                                    │
                                    Tableros ArcGIS Dashboards / Power BI
```

La versión *big data* de este mismo dominio, con PySpark + Delta Lake y arquitectura medallón, está en el repo [`bogota-mobility-spark`](https://github.com/Jonny-Monroy/bogota-mobility-spark).

## Ejecutar los tests

```bash
pip install pandas pytest
pytest tests/ -v
```

## Nota legal

Los nombres de corredores usados (STGV_*, DIM_BIC_*) replican convenciones estándar de tramos viales. El código fue reescrito de memoria para esta publicación; no contiene lógica propietaria de la SDM más allá de patrones de integración genéricos.

## Contacto

**Jonny Alejandro Monroy Chavez** · [jhonny.a.monroy@gmail.com](mailto:jhonny.a.monroy@gmail.com) · [LinkedIn](https://linkedin.com/in/jonny-alejandro-monroy-chavez) · [GitHub](https://github.com/Jonny-Monroy)
