import os
import shutil
from pathlib import Path


def eliminar_carpetas_fx(directorio_base: str):
    """
    Busca y elimina de forma recursiva:
      - Todas las carpetas llamadas 'fx'.
      - Todos los ficheros cuyo nombre contenga '_fx_'.
    """
    path_base = Path(directorio_base)
    contador_carpetas = 0
    contador_ficheros = 0

    print(f"Iniciando búsqueda en: {path_base}\n")

    # ── 1. Carpetas llamadas 'fx' ────────────────────────────────────────────
    for p in path_base.rglob("fx"):
        if p.is_dir():
            try:
                print(f"[CARPETA] Eliminando: {p}")
                shutil.rmtree(p)
                contador_carpetas += 1
            except Exception as e:
                print(f"  ✗ Error al eliminar carpeta {p}: {e}")

    # ── 2. Ficheros cuyo nombre contiene '_fx_' ──────────────────────────────
    for p in path_base.rglob("*_fx_*"):
        if p.is_file():
            try:
                print(f"[FICHERO] Eliminando: {p}")
                p.unlink()
                contador_ficheros += 1
            except Exception as e:
                print(f"  ✗ Error al eliminar fichero {p}: {e}")

    print(
        f"\nProceso finalizado."
        f"\n  Carpetas 'fx' eliminadas : {contador_carpetas}"
        f"\n  Ficheros '_fx_' eliminados: {contador_ficheros}"
    )


if __name__ == "__main__":
    DIRECTORIO_TRABAJO = r"f:\datos\Desktop\GIT\descargar_datos\datos"
    eliminar_carpetas_fx(DIRECTORIO_TRABAJO)
