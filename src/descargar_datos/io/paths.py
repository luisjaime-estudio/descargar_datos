"""Utilidades de rutas para scripts del proyecto."""

from __future__ import annotations

from pathlib import Path


def asegurar_directorio(path: Path) -> Path:
    """Crea un directorio si no existe y devuelve la ruta."""
    path.mkdir(parents=True, exist_ok=True)
    return path
