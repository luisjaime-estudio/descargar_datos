"""Punto de integracion para analisis completo de calidad NetCDF."""

from __future__ import annotations

import runpy
from pathlib import Path


def main() -> None:
    """Ejecuta el script legacy `primer_vistazo.py`."""
    raiz = Path(__file__).resolve().parents[3]
    runpy.run_path(str(raiz / "primer_vistazo.py"), run_name="__main__")


if __name__ == "__main__":
    main()
