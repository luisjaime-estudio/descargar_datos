"""Elimina carpetas fx y ficheros con patron _fx_."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def eliminar_carpetas_fx(directorio_base: str) -> None:
    """Elimina recursivamente carpetas 'fx' y archivos con '_fx_'."""
    path_base = Path(directorio_base)
    contador_carpetas = 0
    contador_ficheros = 0

    print(f"Iniciando busqueda en: {path_base}\n")

    for path in path_base.rglob("fx"):
        if path.is_dir():
            try:
                print(f"[CARPETA] Eliminando: {path}")
                shutil.rmtree(path)
                contador_carpetas += 1
            except Exception as exc:
                print(f"Error al eliminar carpeta {path}: {exc}")

    for path in path_base.rglob("*_fx_*"):
        if path.is_file():
            try:
                print(f"[FICHERO] Eliminando: {path}")
                path.unlink()
                contador_ficheros += 1
            except Exception as exc:
                print(f"Error al eliminar fichero {path}: {exc}")

    print(
        f"\nProceso finalizado.\n"
        f"  Carpetas 'fx' eliminadas : {contador_carpetas}\n"
        f"  Ficheros '_fx_' eliminados: {contador_ficheros}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Elimina carpetas fx y archivos _fx_.")
    parser.add_argument(
        "--directorio-base",
        type=str,
        default="datos",
        help="Directorio raiz para busqueda recursiva.",
    )
    args = parser.parse_args()
    eliminar_carpetas_fx(args.directorio_base)


if __name__ == "__main__":
    main()
