# Flujo operativo recomendado

## Tabla de comandos (antes vs ahora)

| Flujo | Antes | Ahora |
| --- | --- | --- |
| Exploracion ESGF | `python explorar_esgf.py` | `python explorar_esgf.py` |
| Descarga principal | `python descargar_datos.py --source_id MIROC6 --directorio_salida datos` | `python descargar_datos.py --source_id MIROC6 --directorio_salida datos` |
| Reorganizacion | `python reorganizar_datos.py` | `python reorganizar_datos.py` |
| Limpieza fx | `python eliminar_carpetas_fx.py` | `python eliminar_carpetas_fx.py` |
| QA completo | `python primer_vistazo.py` | `python primer_vistazo.py` |
| Resumen ejecutivo | `python resumen_ejecutivo.py` | `python resumen_ejecutivo.py` |
| Detectar faltantes | `python anios_faltantes_modelo.py --directorio datos` | `python anios_faltantes_modelo.py --directorio datos` |
| Descargar faltantes | `python descargar_faltantes.py --csv-faltantes anios_faltantes_modelo_ensamble.csv` | `python descargar_faltantes.py --csv-faltantes anios_faltantes_modelo_ensamble.csv` |
| Orquestacion | No existia | `python scripts/orquestar_pipeline.py` |

Notas:

- Se mantiene la forma de trabajo por scripts Python (sin CLI unificada).
- La diferencia principal es interna: ahora los scripts delegan en modulos de `src/`.

## Flujo completo (manual por scripts)

1. `python explorar_esgf.py` (opcional)
2. `python descargar_datos.py --source_id MIROC6 --directorio_salida datos`
3. `python reorganizar_datos.py`
4. `python eliminar_carpetas_fx.py` (opcional)
5. `python primer_vistazo.py`
6. `python anios_faltantes_modelo.py --directorio datos`
7. `python descargar_faltantes.py --csv-faltantes anios_faltantes_modelo_ensamble.csv`
8. `python primer_vistazo.py` (validacion final)

## Flujo automatizado

- `python scripts/orquestar_pipeline.py`
- Simulacion sin ejecutar: `python scripts/orquestar_pipeline.py --dry-run`
