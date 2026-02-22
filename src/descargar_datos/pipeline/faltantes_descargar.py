"""Descarga datos faltantes por modelo y ensamble desde un CSV."""

from __future__ import annotations

import argparse
import csv
import logging
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import intake_esgf
from intake_esgf import ESGFCatalog


PATRON_MEMBER_CARPETA = re.compile(r"^(s\d{4})-(r.+)$")


@dataclass(frozen=True)
class TareaDescarga:
    modelo: str
    ensamble: str
    anio: int

    @property
    def sub_experiment_id(self) -> str:
        return f"s{self.anio}"


def configurar_logger() -> logging.Logger:
    logger = logging.getLogger("descargar_faltantes")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                "[%(asctime)s] [%(levelname)s] %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def parsear_lista_anios(valor: str) -> list[int]:
    if not valor:
        return []
    return [int(v.strip()) for v in valor.split(",") if v.strip()]


def leer_tareas_desde_csv(ruta_csv: Path, filtro_modelo: str | None) -> list[TareaDescarga]:
    tareas: list[TareaDescarga] = []
    filtro = filtro_modelo.lower() if filtro_modelo else None

    with ruta_csv.open("r", encoding="utf-8", newline="") as archivo:
        reader = csv.DictReader(archivo)
        columnas = set(reader.fieldnames or [])
        requeridas = {"modelo", "ensamble", "lista_anios_faltantes"}
        faltantes = requeridas - columnas
        if faltantes:
            raise ValueError(
                f"CSV invalido. Faltan columnas requeridas: {sorted(faltantes)}"
            )

        for fila in reader:
            modelo = (fila.get("modelo") or "").strip()
            ensamble = (fila.get("ensamble") or "").strip()
            if not modelo or not ensamble:
                continue
            if filtro and filtro not in modelo.lower():
                continue

            for anio in parsear_lista_anios((fila.get("lista_anios_faltantes") or "").strip()):
                tareas.append(TareaDescarga(modelo=modelo, ensamble=ensamble, anio=anio))

    return tareas


def ya_existe_en_salida(tarea: TareaDescarga, directorio_salida: Path) -> bool:
    destino = directorio_salida / tarea.modelo / tarea.ensamble / tarea.sub_experiment_id
    if not destino.exists():
        return False
    return any(destino.glob("*.nc"))


def descargar_tarea(
    tarea: TareaDescarga,
    experiment_id: str,
    table_id: str,
    variable_id: str,
    grid_label: str,
    latest: bool,
) -> tuple[str, str]:
    """Descarga una tarea y devuelve (estado, detalle)."""
    params: dict[str, Any] = {
        "experiment_id": experiment_id,
        "table_id": table_id,
        "variable_id": variable_id,
        "source_id": tarea.modelo,
        "grid_label": grid_label,
        "latest": latest,
        "sub_experiment_id": tarea.sub_experiment_id,
        "variant_label": tarea.ensamble,
    }

    try:
        catalogo = ESGFCatalog()
        catalogo.search(**params)

        if len(catalogo.df) == 0:
            return ("sin_resultados", "No hay resultados en ESGF para esa combinacion.")

        intake_esgf.conf.set(break_on_error=False)
        datasets = catalogo.to_dataset_dict()

        if len(datasets) == 0:
            return (
                "sin_descarga",
                "La busqueda devolvio filas, pero no se pudo descargar.",
            )

        return ("descargado", f"Datasets descargados: {len(datasets)}")

    except Exception as exc:
        return ("error", f"{type(exc).__name__}: {exc}")


def mover_nc_cache_a_salida(cache_dir: Path, salida_dir: Path) -> tuple[int, int, int]:
    """Mueve .nc desde cache ESGF a estructura final."""
    movidos = 0
    omitidos = 0
    errores = 0

    for archivo in cache_dir.rglob("*.nc"):
        try:
            partes_nombre = archivo.name.split("_")
            if len(partes_nombre) < 3:
                errores += 1
                continue
            modelo = partes_nombre[2]

            sub_exp = None
            ensamble = None
            for parte in reversed(archivo.parts):
                match = PATRON_MEMBER_CARPETA.match(parte)
                if match:
                    sub_exp = match.group(1)
                    ensamble = match.group(2)
                    break

            if not sub_exp or not ensamble:
                errores += 1
                continue

            destino_dir = salida_dir / modelo / ensamble / sub_exp
            destino_dir.mkdir(parents=True, exist_ok=True)
            destino = destino_dir / archivo.name

            if destino.exists():
                omitidos += 1
                continue

            shutil.move(str(archivo), str(destino))
            movidos += 1

        except Exception:
            errores += 1

    return (movidos, omitidos, errores)


def guardar_reporte(resultados: list[dict[str, str | int]], salida_csv: Path) -> None:
    salida_csv.parent.mkdir(parents=True, exist_ok=True)
    campos = ["modelo", "ensamble", "anio", "estado", "detalle"]
    with salida_csv.open("w", encoding="utf-8", newline="") as archivo:
        writer = csv.DictWriter(archivo, fieldnames=campos)
        writer.writeheader()
        writer.writerows(resultados)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Descarga datos faltantes (modelo+ensamble+anio) desde CSV."
    )
    parser.add_argument(
        "--csv-faltantes",
        type=str,
        default="anios_faltantes_modelo_ensamble.csv",
        help="CSV con los anios faltantes por modelo+ensamble.",
    )
    parser.add_argument(
        "--directorio-salida",
        type=str,
        default="datos",
        help="Directorio final donde dejar los .nc reorganizados.",
    )
    parser.add_argument(
        "--directorio-cache",
        type=str,
        default="_cache_esgf_faltantes",
        help="Directorio temporal de cache para descargas ESGF.",
    )
    parser.add_argument(
        "--reporte",
        type=str,
        default="reporte_descarga_faltantes.csv",
        help="CSV de resultado por tarea.",
    )
    parser.add_argument("--experiment-id", type=str, default="dcppA-hindcast")
    parser.add_argument("--table-id", type=str, default="Amon")
    parser.add_argument("--variable-id", type=str, default="pr")
    parser.add_argument("--grid-label", type=str, default="gn")
    parser.add_argument(
        "--latest",
        action="store_true",
        default=True,
        help="Descargar solo la version mas reciente (default: true).",
    )
    parser.add_argument(
        "--filtro-modelo",
        type=str,
        default=None,
        help="Texto opcional para filtrar modelos (ej: MIROC).",
    )
    args = parser.parse_args()

    logger = configurar_logger()

    ruta_csv = Path(args.csv_faltantes)
    if not ruta_csv.exists():
        raise FileNotFoundError(f"No existe el CSV de faltantes: {ruta_csv}")

    cache_dir = Path(args.directorio_cache)
    salida_dir = Path(args.directorio_salida)
    cache_dir.mkdir(parents=True, exist_ok=True)
    salida_dir.mkdir(parents=True, exist_ok=True)

    intake_esgf.conf.set(local_cache=str(cache_dir))

    tareas = leer_tareas_desde_csv(ruta_csv, args.filtro_modelo)
    if not tareas:
        logger.info("No hay tareas para descargar.")
        return

    logger.info("Tareas a procesar: %d", len(tareas))

    resultados: list[dict[str, str | int]] = []
    for i, tarea in enumerate(tareas, start=1):
        logger.info(
            "[%d/%d] %s | %s | %s",
            i,
            len(tareas),
            tarea.modelo,
            tarea.ensamble,
            tarea.sub_experiment_id,
        )

        if ya_existe_en_salida(tarea, salida_dir):
            resultados.append(
                {
                    "modelo": tarea.modelo,
                    "ensamble": tarea.ensamble,
                    "anio": tarea.anio,
                    "estado": "ya_existe",
                    "detalle": "Ya habia al menos un .nc en destino.",
                }
            )
            continue

        estado, detalle = descargar_tarea(
            tarea=tarea,
            experiment_id=args.experiment_id,
            table_id=args.table_id,
            variable_id=args.variable_id,
            grid_label=args.grid_label,
            latest=args.latest,
        )

        movidos, omitidos, errores = mover_nc_cache_a_salida(cache_dir, salida_dir)
        detalle_final = (
            f"{detalle} | movidos={movidos}, omitidos={omitidos}, errores_mov={errores}"
        )

        resultados.append(
            {
                "modelo": tarea.modelo,
                "ensamble": tarea.ensamble,
                "anio": tarea.anio,
                "estado": estado,
                "detalle": detalle_final,
            }
        )

    ruta_reporte = Path(args.reporte)
    guardar_reporte(resultados, ruta_reporte)
    logger.info("Reporte generado: %s", ruta_reporte)


if __name__ == "__main__":
    main()
