"""Genera un CSV con anios faltantes por modelo y ensamble."""

from __future__ import annotations

import argparse
import csv
import re
from dataclasses import dataclass
from pathlib import Path


PATRON_NOMBRE = re.compile(
    r"^(?P<variable>\w+)_(?P<table>\w+)_(?P<source>[^_]+)_"
    r"(?P<experiment>[^_]+)_(?P<member>[^_]+)_(?P<grid>[^_]+)_"
    r"(?P<timerange>[^_]+)\.nc$"
)
PATRON_ANIO_MEMBER = re.compile(r"s(?P<anio>\d{4})")


@dataclass(frozen=True)
class Registro:
    modelo: str
    ensamble: str
    anio: int


def extraer_registro(path_nc: Path) -> Registro | None:
    """Extrae modelo, ensamble y anio de inicializacion desde el nombre."""
    match_nombre = PATRON_NOMBRE.match(path_nc.name)
    if not match_nombre:
        return None

    modelo = match_nombre.group("source")
    member = match_nombre.group("member")

    match_anio = PATRON_ANIO_MEMBER.search(member)
    if not match_anio:
        return None

    ensamble = member.split("-", 1)[1] if "-" in member else member
    return Registro(modelo=modelo, ensamble=ensamble, anio=int(match_anio.group("anio")))


def calcular_faltantes(
    registros: list[Registro], inicio: int | None, fin: int | None
) -> list[dict[str, str | int]]:
    """Calcula anios faltantes por combinacion modelo+ensamble."""
    por_modelo_ensamble: dict[tuple[str, str], set[int]] = {}
    for registro in registros:
        clave = (registro.modelo, registro.ensamble)
        por_modelo_ensamble.setdefault(clave, set()).add(registro.anio)

    filas: list[dict[str, str | int]] = []
    for (modelo, ensamble), anios_presentes in sorted(por_modelo_ensamble.items()):
        min_detectado = min(anios_presentes)
        max_detectado = max(anios_presentes)

        inicio_esperado = inicio if inicio is not None else min_detectado
        fin_esperado = fin if fin is not None else max_detectado

        if fin_esperado < inicio_esperado:
            raise ValueError(
                f"Rango invalido para {modelo}: inicio={inicio_esperado}, fin={fin_esperado}."
            )

        anios_esperados = set(range(inicio_esperado, fin_esperado + 1))
        faltantes = sorted(anios_esperados - anios_presentes)

        filas.append(
            {
                "modelo": modelo,
                "ensamble": ensamble,
                "anio_min_detectado": min_detectado,
                "anio_max_detectado": max_detectado,
                "anio_inicio_esperado": inicio_esperado,
                "anio_fin_esperado": fin_esperado,
                "anios_presentes": len(anios_presentes),
                "anios_esperados": len(anios_esperados),
                "anios_faltantes": len(faltantes),
                "lista_anios_faltantes": ",".join(str(anio) for anio in faltantes),
            }
        )

    return filas


def guardar_csv(filas: list[dict[str, str | int]], ruta_salida: Path) -> None:
    """Guarda el reporte en CSV."""
    ruta_salida.parent.mkdir(parents=True, exist_ok=True)

    campos = [
        "modelo",
        "ensamble",
        "anio_min_detectado",
        "anio_max_detectado",
        "anio_inicio_esperado",
        "anio_fin_esperado",
        "anios_presentes",
        "anios_esperados",
        "anios_faltantes",
        "lista_anios_faltantes",
    ]

    with ruta_salida.open("w", newline="", encoding="utf-8") as archivo:
        writer = csv.DictWriter(archivo, fieldnames=campos)
        writer.writeheader()
        writer.writerows(filas)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Genera un CSV con cuantos anios faltan por combinacion "
            "modelo+ensamble para todos los modelos detectados."
        )
    )
    parser.add_argument(
        "--directorio",
        type=str,
        default="datos",
        help="Directorio raiz donde buscar archivos .nc (default: datos).",
    )
    parser.add_argument(
        "--filtro-modelo",
        type=str,
        default=None,
        help="Texto opcional para filtrar modelos (ej: MIROC). Si no se pasa, usa todos.",
    )
    parser.add_argument(
        "--inicio",
        type=int,
        default=None,
        help="Anio inicial esperado (opcional).",
    )
    parser.add_argument(
        "--fin",
        type=int,
        default=None,
        help="Anio final esperado (opcional).",
    )
    parser.add_argument(
        "--salida",
        type=str,
        default="anios_faltantes_modelo_ensamble.csv",
        help="Ruta del CSV de salida (default: anios_faltantes_modelo_ensamble.csv).",
    )
    args = parser.parse_args()

    base = Path(args.directorio)
    if not base.exists():
        raise FileNotFoundError(f"El directorio no existe: {base}")

    filtro = args.filtro_modelo.lower() if args.filtro_modelo else None
    registros: list[Registro] = []

    for archivo_nc in base.rglob("*.nc"):
        registro = extraer_registro(archivo_nc)
        if registro is None:
            continue
        if filtro and filtro not in registro.modelo.lower():
            continue
        registros.append(registro)

    if not registros:
        if args.filtro_modelo:
            print(
                f"No se encontraron archivos .nc para modelos que contengan '{args.filtro_modelo}'."
            )
        else:
            print("No se encontraron archivos .nc con patron reconocido.")
        return

    filas = calcular_faltantes(registros, inicio=args.inicio, fin=args.fin)
    ruta_salida = Path(args.salida)
    guardar_csv(filas, ruta_salida)

    print(f"CSV generado en: {ruta_salida}")
    for fila in filas:
        print(
            f"- {fila['modelo']} | {fila['ensamble']}: {fila['anios_faltantes']} anios faltantes "
            f"(esperados {fila['anios_esperados']}, presentes {fila['anios_presentes']})"
        )


if __name__ == "__main__":
    main()
