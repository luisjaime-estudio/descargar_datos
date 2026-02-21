import os
import shutil
from pathlib import Path
import glob
import re

def reorganizar_datos():
    # Directorio base donde están las carpetas descargadas
    base_dir = Path(r"f:\datos\Desktop\prueba_descarga") 
    
    # Directorio destino
    target_dir = base_dir / "datos"
    
    # Crear directorio destino si no existe
    target_dir.mkdir(exist_ok=True)
    
    print(f"Buscando directorios en: {base_dir}")
    
    # Encontrar carpetas de origen
    source_dirs = [d for d in base_dir.iterdir() if d.is_dir() and d.name.startswith("datos_cmip6_")]
    
    files = []
    for d in source_dirs:
        print(f"Escaneando directorio: {d.name}...")
        files.extend(list(d.rglob("*.nc")))

    if not files:
        print("No se encontraron archivos .nc para mover.")
        return

    print(f"Encontrados {len(files)} archivos total en {len(source_dirs)} directorios.")
    
    archivos_movidos = 0
    errores = 0
    
    # Regex para identificar la carpeta del member_id (ej: s1960-r10i1p1f1)
    # Debe empezar por s, seguir con 4 dígitos (año), un guion y luego la variante
    member_regex = re.compile(r"^(s\d{4})-(r.+)$")

    for file_path in files:
        try:
            filename = file_path.name
            
            # 1. Extraer nombre del modelo del nombre del archivo
            # Formato típico: variable_table_model_experiment_member_grid_time.nc
            # Ejemplo: pr_Amon_MIROC6_dcppA-hindcast_s1960-r10i1p1f1_gn_196011-197012.nc
            parts = filename.split('_')
            if len(parts) > 2:
                model_name = parts[2] # MIROC6, NorCPM1, etc.
            else:
                print(f"Advertencia: Formato de nombre de archivo no reconocido: {filename}. Saltando.")
                errores += 1
                continue
            
            # 2. Extraer Año y Variante de la ruta
            # Buscamos en las partes de la ruta alguna carpeta que cumpla el patrón sYYYY-r...
            year_folder = None
            variant_folder = None
            
            # Iteramos las partes de la ruta en reverso para encontrar la carpeta member más cercana al archivo
            for part in reversed(file_path.parts):
                match = member_regex.match(part)
                if match:
                    year_folder = match.group(1) # s1960
                    variant_folder = match.group(2) # r10i1p1f1
                    break
            
            if not year_folder or not variant_folder:
                print(f"Advertencia: No se pudo extraer año/variante de la ruta: {file_path}. Saltando.")
                errores += 1
                continue
            
            # 3. Construir ruta destino
            # Estructura: datos / Modelo / Variante / Año / Archivo
            dest_folder = target_dir / model_name / variant_folder / year_folder
            dest_path = dest_folder / filename
            
            # Crear carpeta destino
            dest_folder.mkdir(parents=True, exist_ok=True)
            
            # 4. Mover archivo
            if dest_path.exists():
                print(f"Advertencia: El archivo destino ya existe: {dest_path}. Saltando para evitar sobrescribir.")
                errores += 1
            else:
                shutil.move(str(file_path), str(dest_path))
                archivos_movidos += 1
                # print(f"Movido: {filename} -> {dest_folder}") # Comentado para no saturar consola si hay muchos
                
        except Exception as e:
            print(f"Error moviendo {file_path}: {e}")
            errores += 1

    print("-" * 30)
    print("Resumen de Reorganización:")
    print(f"Archivos procesados: {len(files)}")
    print(f"Archivos movidos exitosamente: {archivos_movidos}")
    print(f"Errores/Saltados: {errores}")
    print("-" * 30)

if __name__ == "__main__":
    reorganizar_datos()
