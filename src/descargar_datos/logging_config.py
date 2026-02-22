"""Configuracion comun de logging para scripts del proyecto."""

from __future__ import annotations

import logging


def configurar_logger(nombre: str, nivel: int = logging.INFO) -> logging.Logger:
    """Crea y devuelve un logger con formato estandar."""
    logger = logging.getLogger(nombre)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(nivel)
    return logger
