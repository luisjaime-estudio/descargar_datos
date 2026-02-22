"""Punto de integracion para la descarga principal ESGF."""

from __future__ import annotations

import runpy
from pathlib import Path


def main() -> None:
    """Ejecuta el script legacy de descarga principal."""
    raiz = Path(__file__).resolve().parents[3]
    runpy.run_path(str(raiz / "descargar_datos.py"), run_name="__main__")


if __name__ == "__main__":
    main()
