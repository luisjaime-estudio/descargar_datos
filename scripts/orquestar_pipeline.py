"""Orquesta la ejecucion de scripts del pipeline en orden logico."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def ejecutar_comando(comando: list[str], dry_run: bool) -> int:
    """Ejecuta un comando o lo imprime en modo simulacion."""
    printable = " ".join(comando)
    print(f"[PIPELINE] {printable}")
    if dry_run:
        return 0
    resultado = subprocess.run(comando, check=False)
    return int(resultado.returncode)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Orquesta descarga, limpieza, QA y manejo de faltantes."
    )
    parser.add_argument("--dry-run", action="store_true", help="Muestra comandos sin ejecutar.")
    parser.add_argument("--explorar", action="store_true", help="Ejecuta exploracion ESGF al inicio.")
    parser.add_argument("--descargar", action="store_true", help="Ejecuta descarga principal.")
    parser.add_argument("--reorganizar", action="store_true", help="Ejecuta reorganizacion de caches.")
    parser.add_argument("--limpiar-fx", action="store_true", help="Ejecuta limpieza de fx.")
    parser.add_argument("--qa", action="store_true", help="Ejecuta QA (primer vistazo).")
    parser.add_argument(
        "--detectar-faltantes",
        action="store_true",
        help="Ejecuta deteccion de anios faltantes.",
    )
    parser.add_argument(
        "--descargar-faltantes",
        action="store_true",
        help="Ejecuta descarga de anios faltantes.",
    )
    parser.add_argument("--qa-final", action="store_true", help="Ejecuta QA final.")
    parser.add_argument(
        "--source-id",
        type=str,
        default="MIROC6",
        help="Modelo para descarga principal (si aplica).",
    )

    args = parser.parse_args()
    raiz = Path(__file__).resolve().parents[1]

    ejecutar_todo = not any(
        [
            args.explorar,
            args.descargar,
            args.reorganizar,
            args.limpiar_fx,
            args.qa,
            args.detectar_faltantes,
            args.descargar_faltantes,
            args.qa_final,
        ]
    )

    pasos: list[list[str]] = []

    if ejecutar_todo or args.explorar:
        pasos.append([sys.executable, str(raiz / "explorar_esgf.py")])
    if ejecutar_todo or args.descargar:
        pasos.append(
            [
                sys.executable,
                str(raiz / "descargar_datos.py"),
                "--source_id",
                args.source_id,
                "--directorio_salida",
                "datos",
            ]
        )
    if ejecutar_todo or args.reorganizar:
        pasos.append([sys.executable, str(raiz / "reorganizar_datos.py")])
    if ejecutar_todo or args.limpiar_fx:
        pasos.append([sys.executable, str(raiz / "eliminar_carpetas_fx.py")])
    if ejecutar_todo or args.qa:
        pasos.append([sys.executable, str(raiz / "primer_vistazo.py")])
    if ejecutar_todo or args.detectar_faltantes:
        pasos.append(
            [
                sys.executable,
                str(raiz / "anios_faltantes_modelo.py"),
                "--directorio",
                "datos",
            ]
        )
    if ejecutar_todo or args.descargar_faltantes:
        pasos.append(
            [
                sys.executable,
                str(raiz / "descargar_faltantes.py"),
                "--csv-faltantes",
                "anios_faltantes_modelo_ensamble.csv",
            ]
        )
    if ejecutar_todo or args.qa_final:
        pasos.append([sys.executable, str(raiz / "primer_vistazo.py")])

    for paso in pasos:
        codigo = ejecutar_comando(paso, dry_run=args.dry_run)
        if codigo != 0:
            raise SystemExit(codigo)

    print("[PIPELINE] Ejecucion completada.")


if __name__ == "__main__":
    main()
