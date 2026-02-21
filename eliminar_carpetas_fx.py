import os
import shutil
from pathlib import Path

def eliminar_carpetas_fx(directorio_base: str):
    """
    Busca y elimina todas las carpetas llamadas 'fx' de forma recursiva.
    """
    path_base = Path(directorio_base)
    contador = 0
    
    print(f"Iniciando búsqueda de carpetas 'fx' en: {path_base}")
    
    # Usamos rglob para buscar carpetas llamadas 'fx'
    for p in path_base.rglob("fx"):
        if p.is_dir():
            try:
                print(f"Eliminando: {p}")
                shutil.rmtree(p)
                contador += 1
            except Exception as e:
                print(f"Error al eliminar {p}: {e}")
                
    print(f"\nProceso finalizado. Se eliminaron {contador} carpetas 'fx'.")

if __name__ == "__main__":
    # DIRECTORIO_ACTUAL = os.getcwd()
    # Usamos la ruta absoluta proporcionada en el contexto
    DIRECTORIO_TRABAJO = r"f:\datos\Desktop\prueba_descarga"
    eliminar_carpetas_fx(DIRECTORIO_TRABAJO)
