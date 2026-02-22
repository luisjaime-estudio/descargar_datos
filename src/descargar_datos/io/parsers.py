"""Funciones de parseo reutilizables para nombres/rutas CMIP6."""

from __future__ import annotations

import re
from pathlib import Path


PATRON_NOMBRE_CMIP6 = re.compile(
    r"^(?P<variable>\w+)_(?P<table>\w+)_(?P<source>[^_]+)_"
    r"(?P<experiment>[^_]+)_(?P<member>[^_]+)_(?P<grid>[^_]+)_"
    r"(?P<timerange>[^_]+)\.nc$"
)
PATRON_MEMBER_CARPETA = re.compile(r"^(s\d{4})-(r.+)$")
PATRON_ANIO_MEMBER = re.compile(r"s(?P<anio>\d{4})")


def extraer_modelo_desde_nombre(path_nc: Path) -> str | None:
    """Extrae el modelo desde nombre CMIP6 (3.er bloque)."""
    partes = path_nc.name.split("_")
    if len(partes) < 3:
        return None
    return partes[2]


def extraer_member_desde_nombre(path_nc: Path) -> str | None:
    """Extrae member_id desde nombre CMIP6 (5.o bloque)."""
    match = PATRON_NOMBRE_CMIP6.match(path_nc.name)
    if not match:
        return None
    return match.group("member")


def extraer_anio_desde_member(member_id: str) -> int | None:
    """Extrae anio desde member_id tipo sYYYY-rXiXpXfX."""
    match = PATRON_ANIO_MEMBER.search(member_id)
    if not match:
        return None
    return int(match.group("anio"))


def extraer_subexp_y_ensamble_desde_ruta(path_nc: Path) -> tuple[str, str] | None:
    """Busca carpeta sYYYY-r... en la ruta y devuelve (sub_exp, ensamble)."""
    for parte in reversed(path_nc.parts):
        match = PATRON_MEMBER_CARPETA.match(parte)
        if match:
            return match.group(1), match.group(2)
    return None
