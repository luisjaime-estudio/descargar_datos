"""Reorganizacion de archivos NetCDF descargados desde caches ESGF."""

from __future__ import annotations

import argparse
import re
import shutil
from pathlib import Path


def reorganizar_datos(base_dir: str = ".", target_dir: str = "datos") -> None:
    """Mueve archivos .nc desde caches ESGF a estructura final en datos/."""
    base_path = Path(base_dir).resolve()
    destino = (base_path / target_dir).resolve() if not Path(target_dir).is_absolute() else Path(target_dir)
    destino.mkdir(exist_ok=True, parents=True)

    print(f"Buscando directorios en: {base_path}")

    source_dirs = [
        d
        for d in base_path.iterdir()
        if d.is_dir() and d.name.startswith("_cache_esgf")
    ]

    files: list[Path] = []
    for directorio in source_dirs:
        print(f"Escaneando directorio: {directorio.name}...")
        files.extend(list(directorio.rglob("*.nc")))

    if not files:
        print("No se encontraron archivos .nc para mover.")
        return

    print(f"Encontrados {len(files)} archivos total en {len(source_dirs)} directorios.")

    archivos_movidos = 0
    errores = 0

    member_regex = re.compile(r"^(s\d{4})-(r.+)$")

    for file_path in files:
        try:
            filename = file_path.name
            parts = filename.split("_")
            if len(parts) > 2:
                model_name = parts[2]
            else:
                print(
                    f"Advertencia: Formato de nombre no reconocido: {filename}. Saltando."
                )
                errores += 1
                continue

            year_folder = None
            variant_folder = None
            for part in reversed(file_path.parts):
                match = member_regex.match(part)
                if match:
                    year_folder = match.group(1)
                    variant_folder = match.group(2)
                    break

            if not year_folder or not variant_folder:
                print(
                    f"Advertencia: No se pudo extraer anio/variante de la ruta: {file_path}."
                )
                errores += 1
                continue

            dest_folder = destino / model_name / variant_folder / year_folder
            dest_path = dest_folder / filename
            dest_folder.mkdir(parents=True, exist_ok=True)

            if dest_path.exists():
                print(f"Advertencia: El archivo ya existe: {dest_path}. Saltando.")
                errores += 1
                continue

            shutil.move(str(file_path), str(dest_path))
            archivos_movidos += 1

        except Exception as exc:
            print(f"Error moviendo {file_path}: {exc}")
            errores += 1

    print("-" * 30)
    print("Resumen de Reorganizacion:")
    print(f"Archivos procesados: {len(files)}")
    print(f"Archivos movidos exitosamente: {archivos_movidos}")
    print(f"Errores/Saltados: {errores}")
    print("-" * 30)


def main() -> None:
    parser = argparse.ArgumentParser(description="Reorganiza archivos .nc desde caches ESGF.")
    parser.add_argument("--base-dir", type=str, default=".", help="Directorio base donde buscar caches _cache_esgf*")
    parser.add_argument("--target-dir", type=str, default="datos", help="Directorio destino final para los .nc")
    args = parser.parse_args()
    reorganizar_datos(base_dir=args.base_dir, target_dir=args.target_dir)


if __name__ == "__main__":
    main()
