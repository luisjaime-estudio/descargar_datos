# Plan de migracion y refactorizacion

Este documento define la migracion del proyecto a una arquitectura mas profesional, manteniendo `datos/` como directorio oficial de salida y conservando el modelo de ejecucion por scripts (`python script.py`).

## Objetivo

- Ordenar el repositorio por responsabilidades (codigo, datos, reportes, logs, configuracion).
- Reducir duplicacion entre scripts y mejorar mantenibilidad.
- Mantener compatibilidad con la forma actual de ejecucion por scripts Python.
- Incorporar un script de orquestacion para ejecutar el flujo completo de forma consistente.

## Decisiones acordadas

- `datos/` se mantiene como ruta final oficial para archivos `.nc`.
- La migracion conlleva refactorizacion de scripts (gradual, no disruptiva).
- No se implementara una CLI unificada.
- Se creara `orquestar_pipeline.py` como script de ejecucion del flujo completo.

## Estructura objetivo (propuesta)

```text
descargar_datos/
  README.md
  requirements.txt
  docs/
    arquitectura.md
    flujo_operativo.md
  config/
    default.yaml
  src/
    descargar_datos/
      __init__.py
      logging_config.py
      settings.py
      esgf/
        descarga.py
        exploracion.py
      pipeline/
        reorganizacion.py
        limpieza_fx.py
        faltantes_detectar.py
        faltantes_descargar.py
      qa/
        analisis_netcdf.py
        resumen_ejecutivo.py
      io/
        parsers.py
        paths.py
        csv_reportes.py
  scripts/
    orquestar_pipeline.py
  tests/
  datos/
  reportes/
    calidad/
    faltantes/
  logs/
```

## Mapeo de scripts actuales a modulos internos

- `descargar_datos.py` -> `src/descargar_datos/esgf/descarga.py`
- `explorar_esgf.py` -> `src/descargar_datos/esgf/exploracion.py`
- `reorganizar_datos.py` -> `src/descargar_datos/pipeline/reorganizacion.py`
- `eliminar_carpetas_fx.py` -> `src/descargar_datos/pipeline/limpieza_fx.py`
- `primer_vistazo.py` -> `src/descargar_datos/qa/analisis_netcdf.py`
- `resumen_ejecutivo.py` -> `src/descargar_datos/qa/resumen_ejecutivo.py`
- `anios_faltantes_modelo.py` -> `src/descargar_datos/pipeline/faltantes_detectar.py`
- `descargar_faltantes.py` -> `src/descargar_datos/pipeline/faltantes_descargar.py`

## Regla de ejecucion (sin CLI)

La ejecucion se mantiene siempre mediante scripts Python. Ejemplos:

- `python descargar_datos.py --source_id MIROC6 --directorio_salida datos`
- `python reorganizar_datos.py`
- `python eliminar_carpetas_fx.py`
- `python primer_vistazo.py`
- `python resumen_ejecutivo.py`
- `python anios_faltantes_modelo.py --directorio datos`
- `python descargar_faltantes.py --csv-faltantes anios_faltantes_modelo_ensamble.csv`
- `python scripts/orquestar_pipeline.py`

## Plan de migracion por fases

### Fase 0 - Baseline

- Crear rama de trabajo para migracion.
- Registrar comandos actuales y resultados esperados (baseline).
- Validar entorno (`venv`, `uv`, dependencias).

### Fase 1 - Estructura y utilidades comunes

- Crear `src/descargar_datos/` y submodulos.
- Extraer componentes comunes:
  - logging
  - parseo de nombres CMIP6
  - utilidades de rutas
  - escritura de reportes CSV

### Fase 2 - Descarga principal

- Migrar logica de `descargar_datos.py` a `src/descargar_datos/esgf/descarga.py`.
- Mantener salida final en `datos/`.
- Mantener cache temporal `_cache_esgf*` como area intermedia.

### Fase 3 - Reorganizacion y limpieza

- Migrar `reorganizar_datos.py` y `eliminar_carpetas_fx.py` a `src/descargar_datos/pipeline/`.
- Eliminar rutas hardcodeadas y parametrizar por argumentos de script/config.

### Fase 4 - QA y reportes

- Migrar `primer_vistazo.py` y `resumen_ejecutivo.py` a `src/descargar_datos/qa/`.
- Estandarizar salidas en `reportes/calidad/`.

### Fase 5 - Faltantes

- Migrar deteccion y descarga de faltantes a `src/descargar_datos/pipeline/faltantes_*`.
- Estandarizar salidas en `reportes/faltantes/`.

### Fase 6 - Orquestacion por script

- Crear `scripts/orquestar_pipeline.py` para coordinar pasos.
- Agregar modo `--dry-run`.
- Soportar activacion/desactivacion de etapas por flags de script.

### Fase 7 - Compatibilidad y estabilizacion

- Mantener scripts actuales como puntos de entrada.
- Cambiar internamente cada script para llamar modulos de `src/`.
- Evitar deprecacion abrupta; priorizar continuidad operativa.

### Fase 8 - Pruebas y cierre

- Agregar pruebas unitarias y smoke tests.
- Validar paridad de resultados antes/despues.
- Documentar flujo final y comandos recomendados.

## Script de orquestacion (diseno funcional)

Archivo propuesto: `scripts/orquestar_pipeline.py`

Funciones del orquestador:

- Ejecutar pipeline completo en orden logico.
- Permitir activar/desactivar etapas por flags.
- Manejar errores por etapa y registrar estado.
- Consolidar logs y generar reporte final de ejecucion.

Orden logico por defecto:

1. exploracion (opcional)
2. descarga principal
3. reorganizacion (si aplica)
4. limpieza `fx` (opcional)
5. QA inicial
6. deteccion de faltantes
7. descarga de faltantes
8. QA final

## Criterios de aceptacion

- `datos/` sigue siendo la salida oficial de `.nc`.
- Ningun script depende de rutas absolutas hardcodeadas.
- Existe script de orquestacion para flujo completo.
- Reportes separados de datos (`reportes/`).
- Los flujos siguen ejecutandose por scripts Python.

## Riesgos y mitigacion

- Riesgo: ruptura de scripts existentes.
  - Mitigacion: mantener nombres y parametros principales durante transicion.
- Riesgo: cambios silenciosos en resultados.
  - Mitigacion: pruebas de paridad por conteo, tamano, faltantes y reportes.
- Riesgo: mezcla de cache y salida final.
  - Mitigacion: convencion estricta cache temporal vs `datos/` final.

## Entregables

- Estructura modular bajo `src/`.
- Script de orquestacion funcional (`scripts/orquestar_pipeline.py`).
- Scripts existentes operativos con refactorizacion interna.
- Documentacion de arquitectura y flujo operativo.
- Pruebas basicas automatizadas.
