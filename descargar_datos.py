"""
Módulo para la descarga de datos climáticos desde el catálogo ESGF.

Implementa la clase DescargadorDatosESGF que encapsula el flujo completo:
configuración → búsqueda → validación → descarga → reorganización.

Estructura de salida:
    <directorio_salida>/
        └── <Modelo>/           (ej: MIROC6)
             └── <variante>/    (ej: r1i1p1f1)
                  └── <año>/    (ej: s1960)
                       └── archivo.nc
"""

import os
import re
import csv
import shutil
import logging
import argparse
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Dict, Any, List, Optional, Set, Tuple

import intake_esgf
from intake_esgf import ESGFCatalog


@dataclass
class ConfiguracionBusqueda:
    """Parámetros de configuración para la búsqueda en ESGF."""
    experiment_id: str = "dcppA-hindcast"
    table_id: str = "Amon"
    variable_id: str = "pr"
    source_id: Optional[str] = None
    grid_label: str = "gr"
    variant_label: str = "r1i4p1f1"
    latest: bool = True

    def __post_init__(self) -> None:
        """Valida que los campos obligatorios no estén vacíos."""
        try:
            if not self.source_id or not self.source_id.strip():
                raise ValueError("'source_id' no puede estar vacío.")
            if not self.variable_id or not self.variable_id.strip():
                raise ValueError("'variable_id' no puede estar vacío.")
        except ValueError as e:
            raise ValueError(f"[VALIDACIÓN] Configuración inválida: {e}") from e

    def a_dict(self) -> Dict[str, Any]:
        """Convierte la configuración a un diccionario para intake-esgf.
        
        Filtra los campos con valor None para evitar enviar parámetros vacíos.
        """
        return {k: v for k, v in asdict(self).items() if v is not None}


class DescargadorDatosESGF:
    """Gestiona la descarga de datos climáticos desde el catálogo ESGF."""

    # Patrón para carpetas con formato sYYYY-rXXiXpXfX en la ruta del caché
    _PATRON_MEMBER: re.Pattern = re.compile(r"^(s\d{4})-(r.+)$")

    def __init__(
        self,
        directorio_cache: str = "datos_cmip6_norcpm1",
        directorio_salida: str = "datos",
        configuracion: Optional[ConfiguracionBusqueda] = None
    ) -> None:
        """
        Inicializa el descargador con la configuración necesaria.

        Args:
            directorio_cache: Nombre o ruta del directorio temporal de caché
                              de intake_esgf (estructura DRS del servidor).
            directorio_salida: Nombre o ruta del directorio de salida final
                               con estructura organizada:
                               Modelo/variante/sub_experiment/archivo.nc
            configuracion: Instancia de ConfiguracionBusqueda con los parámetros.
        """
        self._directorio_cache: str = os.path.abspath(directorio_cache)
        self._directorio_salida: Path = Path(os.path.abspath(directorio_salida))
        self._catalogo: Optional[ESGFCatalog] = None
        self._configuracion: ConfiguracionBusqueda = configuracion or ConfiguracionBusqueda()
        self._resultado_descarga: Dict[str, Any] = {}
        self._claves_fallidas: Set[str] = set()
        self._logger: logging.Logger = self._configurar_logger()

    # ------------------------------------------------------------------ #
    #  Método orquestador (público)                                       #
    # ------------------------------------------------------------------ #

    def ejecutar(self) -> None:
        """
        Orquesta el flujo completo de descarga y organización.

        Secuencia:
            1. Configurar directorio de caché local.
            2. Configurar la caché de intake_esgf.
            3. Inicializar el catálogo ESGF.
            4. Ejecutar la búsqueda con los parámetros definidos.
            5. Validar que existan resultados.
            6. Descargar los datasets encontrados.
            7. Reorganizar archivos en estructura organizada.
            8. Limpiar caché temporal.
        """
        self._logger.info(
            "--- Iniciando proceso de descarga para %s ---",
            self._configuracion.source_id,
        )
        self._logger.info("Caché temporal: %s", self._directorio_cache)
        self._logger.info("Directorio de salida: %s", self._directorio_salida)

        self._configurar_directorio_cache()
        self._configurar_cache_intake()
        self._inicializar_catalogo()
        self._ejecutar_busqueda()

        if not self._validar_resultados():
            return

        self._descargar_datasets()
        self._reorganizar_archivos()

    # ------------------------------------------------------------------ #
    #  Métodos auxiliares (privados) — Inicialización                      #
    # ------------------------------------------------------------------ #

    def _configurar_logger(self) -> logging.Logger:
        """Inicializa y retorna un logger con formato en español."""
        try:
            logger = logging.getLogger(
                f"DescargadorESGF.{self._configuracion.source_id}"
            )
            if not logger.handlers:
                handler = logging.StreamHandler()
                formato = logging.Formatter(
                    "[%(asctime)s] [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
                handler.setFormatter(formato)
                logger.addHandler(handler)
            logger.setLevel(logging.INFO)
            return logger
        except Exception as e:
            raise RuntimeError(
                f"[ERROR] Fallo al configurar el logger: {e}"
            ) from e

    def _configurar_directorio_cache(self) -> None:
        """Crea el directorio de caché local si no existe."""
        try:
            os.makedirs(self._directorio_cache, exist_ok=True)
        except OSError as e:
            self._logger.error(
                "Fallo al crear el directorio de caché '%s': %s",
                self._directorio_cache, e,
            )
            raise

    def _configurar_cache_intake(self) -> None:
        """Establece la ubicación de la caché en intake_esgf."""
        try:
            intake_esgf.conf.set(local_cache=self._directorio_cache)
        except Exception as e:
            self._logger.error(
                "Fallo al configurar la caché de intake_esgf: %s", e
            )
            raise

    # ------------------------------------------------------------------ #
    #  Métodos auxiliares (privados) — Búsqueda y validación               #
    # ------------------------------------------------------------------ #

    def _inicializar_catalogo(self) -> None:
        """Crea una nueva instancia del catálogo ESGF."""
        try:
            self._catalogo = ESGFCatalog()
        except Exception as e:
            self._logger.error("Fallo al inicializar el catálogo ESGF: %s", e)
            raise

    def _ejecutar_busqueda(self) -> None:
        """Realiza la búsqueda en el catálogo con los parámetros configurados."""
        try:
            self._catalogo.search(**self._configuracion.a_dict())
            self._logger.info("Búsqueda completada. Resultados encontrados:")
            self._logger.info("\n%s", self._catalogo.df)
        except Exception as e:
            self._logger.error(
                "Fallo durante la búsqueda en el catálogo ESGF: %r", e
            )
            raise

    def _validar_resultados(self) -> bool:
        """
        Verifica que la búsqueda haya devuelto resultados.

        Returns:
            True si hay resultados, False en caso contrario.
        """
        try:
            if len(self._catalogo.df) == 0:
                self._logger.warning(
                    "No se encontraron resultados para '%s' con estos criterios.",
                    self._configuracion.source_id
                )
                self._logger.warning(
                    "Sugerencia: Verifique si 'grid_label', 'variant_label' o "
                    "'sub_experiment_id' existen para este modelo en ESGF."
                )
                return False
            return True
        except Exception as e:
            self._logger.error(
                "Fallo al validar los resultados de búsqueda: %s", e
            )
            raise

    # ------------------------------------------------------------------ #
    #  Métodos auxiliares (privados) — Descarga (orquestador + atómicos)   #
    # ------------------------------------------------------------------ #

    def _descargar_datasets(self) -> None:
        """
        Orquesta la descarga de datasets: ejecución tolerante,
        detección de fallos, resumen y registro.
        """
        self._ejecutar_descarga_tolerante()
        self._detectar_fallos()
        self._imprimir_resumen_descarga()

        if self._claves_fallidas:
            self._registrar_fallos_csv()

    def _ejecutar_descarga_tolerante(self) -> None:
        """Descarga los datasets sin interrumpirse por errores parciales."""
        try:
            intake_esgf.conf.set(break_on_error=False)
            self._resultado_descarga = self._catalogo.to_dataset_dict()
        except Exception as e:
            self._logger.error("Ocurrió un fallo durante la descarga: %s", e)
            raise

    def _detectar_fallos(self) -> None:
        """Compara claves del catálogo con las descargadas para identificar fallos."""
        try:
            claves_catalogo: Set[str] = set(self._catalogo.df["key"])
            claves_descargadas: Set[str] = set(self._resultado_descarga.keys())
            self._claves_fallidas = claves_catalogo - claves_descargadas
        except Exception as e:
            self._logger.error(
                "Fallo al detectar datasets fallidos: %s", e
            )
            raise

    def _imprimir_resumen_descarga(self) -> None:
        """Registra un resumen con el conteo de éxitos y fallos."""
        try:
            total_descargados = len(self._resultado_descarga)
            total_fallidos = len(self._claves_fallidas)
            self._logger.info("Descarga finalizada.")
            self._logger.info("  - Datasets descargados: %d", total_descargados)
            self._logger.info("  - Datasets fallidos:    %d", total_fallidos)
            self._logger.info("  - Caché temporal en: %s", self._directorio_cache)
        except Exception as e:
            self._logger.error(
                "Fallo al imprimir el resumen de descarga: %s", e
            )
            raise

    # ------------------------------------------------------------------ #
    #  Métodos auxiliares (privados) — Registro CSV (orquestador + atóm.)  #
    # ------------------------------------------------------------------ #

    def _registrar_fallos_csv(self) -> None:
        """Orquesta el registro de fallos: construye ruta y escribe filas."""
        ruta_csv = self._construir_ruta_csv_fallos()
        self._escribir_filas_csv(ruta_csv)

    def _construir_ruta_csv_fallos(self) -> str:
        """Genera la ruta completa del archivo CSV de fallos."""
        try:
            return os.path.join(
                str(self._directorio_salida),
                f"descargas_fallidas_{self._configuracion.source_id.lower()}.csv",
            )
        except Exception as e:
            self._logger.error(
                "Fallo al construir la ruta del CSV de fallos: %s", e
            )
            raise

    def _escribir_filas_csv(self, ruta_csv: str) -> None:
        """
        Escribe las filas de datasets fallidos en el CSV.

        Args:
            ruta_csv: Ruta absoluta del archivo CSV de destino.
        """
        try:
            os.makedirs(os.path.dirname(ruta_csv), exist_ok=True)
            archivo_existe = os.path.isfile(ruta_csv)
            fecha_intento = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            with open(ruta_csv, mode="a", newline="", encoding="utf-8") as f:
                escritor = csv.writer(f)
                if not archivo_existe:
                    escritor.writerow([
                        "clave_dataset", "source_id",
                        "fecha_intento", "motivo",
                    ])
                for clave in self._claves_fallidas:
                    escritor.writerow([
                        clave,
                        self._configuracion.source_id,
                        fecha_intento,
                        "Nodo offline o sin información de acceso",
                    ])
            self._logger.info("Fallos registrados en: %s", ruta_csv)
        except OSError as e:
            self._logger.error(
                "No se pudo escribir el CSV de fallos '%s': %s", ruta_csv, e
            )

    # ------------------------------------------------------------------ #
    #  Métodos auxiliares (privados) — Reorganización (orquestador + atóm.)#
    # ------------------------------------------------------------------ #

    def _reorganizar_archivos(self) -> None:
        """
        Orquesta la reorganización de archivos .nc desde la caché temporal
        a la estructura organizada: Modelo/variante/sub_experiment/.
        """
        archivos_nc = self._buscar_archivos_nc()

        if not archivos_nc:
            self._logger.warning(
                "No se encontraron archivos .nc en la caché temporal."
            )
            return

        self._logger.info(
            "Reorganizando %d archivos .nc a estructura organizada...",
            len(archivos_nc),
        )

        self._directorio_salida.mkdir(parents=True, exist_ok=True)
        archivos_movidos, errores = self._mover_archivos(archivos_nc)
        self._imprimir_resumen_reorganizacion(archivos_movidos, errores, len(archivos_nc))
        self._limpiar_cache_temporal()

    def _buscar_archivos_nc(self) -> List[Path]:
        """Busca todos los archivos .nc en el directorio de caché temporal."""
        try:
            ruta_cache = Path(self._directorio_cache)
            return list(ruta_cache.rglob("*.nc"))
        except Exception as e:
            self._logger.error(
                "Fallo al buscar archivos .nc en la caché: %s", e
            )
            raise

    def _extraer_metadatos_ruta(self, ruta_archivo: Path) -> Optional[Tuple[str, str, str]]:
        """
        Extrae modelo, variante y sub_experiment de un archivo .nc.

        Estrategia:
            1. Modelo → 3.ª parte del nombre del archivo
               (formato: variable_table_model_experiment_member_grid_time.nc)
            2. Variante y sub_experiment → del directorio padre que
               cumpla el patrón sYYYY-rXXiXpXfX.

        Args:
            ruta_archivo: Ruta completa al archivo .nc.

        Returns:
            Tupla (modelo, variante, sub_experiment) o None si no se
            pueden extraer.
        """
        try:
            # --- Extraer modelo del nombre del archivo ---
            partes_nombre = ruta_archivo.name.split('_')
            if len(partes_nombre) < 3:
                self._logger.warning(
                    "Formato de nombre no reconocido: %s", ruta_archivo.name
                )
                return None
            modelo = partes_nombre[2]

            # --- Extraer variante y sub_experiment de la ruta ---
            sub_experiment: Optional[str] = None
            variante: Optional[str] = None

            for parte in reversed(ruta_archivo.parts):
                match = self._PATRON_MEMBER.match(parte)
                if match:
                    sub_experiment = match.group(1)  # ej: s1960
                    variante = match.group(2)        # ej: r10i1p1f1
                    break

            if not sub_experiment or not variante:
                self._logger.warning(
                    "No se pudo extraer variante/sub_experiment de: %s",
                    ruta_archivo,
                )
                return None

            return (modelo, variante, sub_experiment)

        except Exception as e:
            self._logger.error(
                "Fallo al extraer metadatos de '%s': %s", ruta_archivo, e
            )
            return None

    def _mover_archivos(self, archivos: List[Path]) -> Tuple[int, int]:
        """
        Mueve los archivos .nc a la estructura organizada.

        Args:
            archivos: Lista de rutas a archivos .nc.

        Returns:
            Tupla (archivos_movidos, errores).
        """
        movidos: int = 0
        errores: int = 0

        for archivo in archivos:
            try:
                metadatos = self._extraer_metadatos_ruta(archivo)
                if metadatos is None:
                    errores += 1
                    continue

                modelo, variante, sub_experiment = metadatos

                # Estructura: salida / Modelo / variante / sub_experiment /
                directorio_destino = (
                    self._directorio_salida / modelo / variante / sub_experiment
                )
                ruta_destino = directorio_destino / archivo.name

                directorio_destino.mkdir(parents=True, exist_ok=True)

                if ruta_destino.exists():
                    self._logger.debug(
                        "Archivo ya existe, omitiendo: %s", ruta_destino
                    )
                    errores += 1
                    continue

                shutil.move(str(archivo), str(ruta_destino))
                movidos += 1

            except Exception as e:
                self._logger.error(
                    "Error moviendo '%s': %s", archivo, e
                )
                errores += 1

        return (movidos, errores)

    def _imprimir_resumen_reorganizacion(
        self, movidos: int, errores: int, total: int
    ) -> None:
        """Registra un resumen de la reorganización de archivos."""
        try:
            self._logger.info("Reorganización finalizada.")
            self._logger.info("  - Archivos procesados:  %d", total)
            self._logger.info("  - Archivos movidos:     %d", movidos)
            self._logger.info("  - Errores/omitidos:     %d", errores)
            self._logger.info(
                "  - Estructura final en:  %s", self._directorio_salida
            )
        except Exception as e:
            self._logger.error(
                "Fallo al imprimir resumen de reorganización: %s", e
            )
            raise

    def _limpiar_cache_temporal(self) -> None:
        """
        Elimina el directorio de caché temporal de intake_esgf
        una vez reorganizados los archivos.
        """
        try:
            ruta_cache = Path(self._directorio_cache)
            if ruta_cache.exists():
                shutil.rmtree(self._directorio_cache)
                self._logger.info(
                    "Caché temporal eliminada: %s", self._directorio_cache
                )
        except Exception as e:
            self._logger.warning(
                "No se pudo eliminar la caché temporal '%s': %s",
                self._directorio_cache, e,
            )


if __name__ == "__main__":
    """
    Ejemplo de uso:
    python descargar_datos.py --source_id MIROC6 --directorio_salida datos
    """
    parser = argparse.ArgumentParser(
        description="Descarga de datos climáticos de ESGF."
    )
    parser.add_argument(
        "--source_id",
        type=str,
        default="MIROC6",
        help="ID de la fuente del modelo (ej: MIROC6, NorCPM1)",
    )
    parser.add_argument(
        "--directorio_salida",
        type=str,
        default="datos",
        help="Directorio de salida con estructura organizada (default: datos)",
    )
    args = parser.parse_args()

    try:
        config = ConfiguracionBusqueda(source_id=args.source_id)
        directorio_cache = f"_cache_esgf_{args.source_id.lower()}"

        descargador = DescargadorDatosESGF(
            directorio_cache=directorio_cache,
            directorio_salida=args.directorio_salida,
            configuracion=config,
        )
        descargador.ejecutar()
    except Exception as error_fatal:
        logging.error("Error fatal en la ejecución: %r", error_fatal)
        raise SystemExit(1) from error_fatal
