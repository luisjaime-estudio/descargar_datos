# Arquitectura del proyecto

## Principios

- Ejecucion por scripts Python (`python script.py`), sin CLI unificada.
- `datos/` como salida oficial de archivos NetCDF finales.
- Separacion entre codigo reutilizable (`src/`) y puntos de entrada (`*.py`, `scripts/`).

## Capas

- `src/descargar_datos/esgf/`: busqueda y descarga en ESGF.
- `src/descargar_datos/pipeline/`: reorganizacion, limpieza y flujo de faltantes.
- `src/descargar_datos/qa/`: analisis de calidad y reportes.
- `src/descargar_datos/io/`: parseo y utilidades comunes de I/O.

## Puntos de entrada

- Scripts raiz mantienen compatibilidad operativa.
- `scripts/orquestar_pipeline.py` coordina la ejecucion completa en orden.
