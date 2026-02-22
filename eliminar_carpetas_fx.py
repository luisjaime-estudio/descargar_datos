"""Wrapper del script de limpieza de elementos fx."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from descargar_datos.pipeline.limpieza_fx import main


if __name__ == "__main__":
    main()
