"""
Script de reintento automático para MIROC6 / r7i1p1f1 / s2020.

Comprueba si el nodo esgf-data02.diasjp.net está accesible y,
si lo está, descarga el archivo faltante y lo mueve a la estructura final.

Uso:
    python reintentar_miroc6_r7_2020.py
    python reintentar_miroc6_r7_2020.py --max-intentos 5 --espera 3600

El script sale con código 0 si descargó correctamente, 1 si falló.
"""

from __future__ import annotations

import argparse
import logging
import re
import shutil
import socket
import time
from pathlib import Path

import intake_esgf
from intake_esgf import ESGFCatalog

MODELO = "MIROC6"
ENSAMBLE = "r7i1p1f1"
SUB_EXP = "s2020"
NODO = "esgf-data02.diasjp.net"

PATRON_MEMBER = re.compile(r"^(s\d{4})-(r.+)$")
CACHE_DIR = Path("_cache_miroc6_r7_2020")
SALIDA_DIR = Path("datos")


def configurar_logger() -> logging.Logger:
    logger = logging.getLogger("reintentar_miroc6")
    if not logger.handlers:
        h = logging.StreamHandler()
        h.setFormatter(logging.Formatter(
            "[%(asctime)s] [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        ))
        logger.addHandler(h)
    logger.setLevel(logging.INFO)
    return logger


def nodo_accesible(host: str, puerto: int = 443, timeout: int = 10) -> bool:
    """Comprueba si el nodo responde en TCP."""
    try:
        with socket.create_connection((host, puerto), timeout=timeout):
            return True
    except OSError:
        return False


def ya_descargado(salida_dir: Path) -> bool:
    destino = salida_dir / MODELO / ENSAMBLE / SUB_EXP
    return destino.exists() and any(destino.glob("*.nc"))


def intentar_descarga(logger: logging.Logger) -> bool:
    """Intenta descargar y organizar el archivo. Devuelve True si éxito."""
    CACHE_DIR.mkdir(exist_ok=True)
    SALIDA_DIR.mkdir(parents=True, exist_ok=True)
    intake_esgf.conf.set(local_cache=str(CACHE_DIR))
    intake_esgf.conf.set(break_on_error=False)

    cat = ESGFCatalog()
    cat.search(
        experiment_id="dcppA-hindcast",
        table_id="Amon",
        variable_id="pr",
        source_id=MODELO,
        variant_label=ENSAMBLE,
        sub_experiment_id=SUB_EXP,
        latest=True,
    )

    if len(cat.df) == 0:
        logger.warning("Búsqueda sin resultados.")
        return False

    datasets = cat.to_dataset_dict()
    if not datasets:
        logger.warning("Descarga fallida (nodo no sirvió el archivo).")
        return False

    # Mover archivos .nc a estructura final
    movidos = 0
    for archivo in CACHE_DIR.rglob("*.nc"):
        partes = archivo.name.split("_")
        if len(partes) < 3:
            continue
        modelo = partes[2]
        sub_exp = ensamble = None
        for parte in reversed(archivo.parts):
            m = PATRON_MEMBER.match(parte)
            if m:
                sub_exp, ensamble = m.group(1), m.group(2)
                break
        if not sub_exp:
            logger.warning("No se pudo extraer metadata de: %s", archivo)
            continue
        destino = SALIDA_DIR / modelo / ensamble / sub_exp / archivo.name
        destino.parent.mkdir(parents=True, exist_ok=True)
        if not destino.exists():
            shutil.move(str(archivo), str(destino))
            logger.info("Movido: %s", destino)
            movidos += 1
        else:
            logger.info("Ya existe: %s", destino)
            movidos += 1

    return movidos > 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reintento automático de descarga MIROC6/r7i1p1f1/s2020."
    )
    parser.add_argument(
        "--max-intentos", type=int, default=24,
        help="Número máximo de reintentos (default: 24).",
    )
    parser.add_argument(
        "--espera", type=int, default=3600,
        help="Segundos entre intentos (default: 3600 = 1h).",
    )
    args = parser.parse_args()

    logger = configurar_logger()

    if ya_descargado(SALIDA_DIR):
        logger.info("El archivo ya está descargado en %s. Nada que hacer.", SALIDA_DIR)
        return

    logger.info(
        "Iniciando reintentos para %s/%s/%s (max=%d, espera=%ds)",
        MODELO, ENSAMBLE, SUB_EXP, args.max_intentos, args.espera,
    )

    for intento in range(1, args.max_intentos + 1):
        logger.info("[%d/%d] Comprobando nodo %s...", intento, args.max_intentos, NODO)

        if not nodo_accesible(NODO):
            logger.warning("Nodo %s no accesible. Esperando %ds...", NODO, args.espera)
            if intento < args.max_intentos:
                time.sleep(args.espera)
            continue

        logger.info("Nodo accesible. Intentando descarga...")
        if intentar_descarga(logger):
            logger.info("✅ Descarga completada con éxito.")
            return

        logger.warning("Descarga fallida. Esperando %ds...", args.espera)
        if intento < args.max_intentos:
            time.sleep(args.espera)

    logger.error("❌ Se agotaron los %d intentos sin éxito.", args.max_intentos)
    raise SystemExit(1)


if __name__ == "__main__":
    main()
