"""Ayudas para escritura de reportes CSV."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any


def guardar_csv_dicts(filas: list[dict[str, Any]], campos: list[str], ruta: Path) -> None:
    """Guarda una lista de diccionarios en CSV con encabezados."""
    ruta.parent.mkdir(parents=True, exist_ok=True)
    with ruta.open("w", encoding="utf-8", newline="") as archivo:
        writer = csv.DictWriter(archivo, fieldnames=campos)
        writer.writeheader()
        writer.writerows(filas)
