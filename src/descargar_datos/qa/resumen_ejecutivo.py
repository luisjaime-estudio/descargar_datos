"""Punto de integracion para reporte ejecutivo de calidad."""

from __future__ import annotations

import runpy
from pathlib import Path


def main() -> None:
    """Ejecuta el script legacy `resumen_ejecutivo.py`."""
    raiz = Path(__file__).resolve().parents[3]
    runpy.run_path(str(raiz / "resumen_ejecutivo.py"), run_name="__main__")


if __name__ == "__main__":
    main()
