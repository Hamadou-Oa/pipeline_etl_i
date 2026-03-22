"""
Logging structuré pour le pipeline ETL
- Format JSON optionnel pour le monitoring
- Niveaux configurables via variable d'environnement
"""

import os
import sys
import logging


def get_logger(name: str) -> logging.Logger:
    """
    Retourne un logger configuré avec format structuré.

    Args:
        name : Module ou classe qui utilise le logger (ex: __name__)

    Returns:
        logging.Logger prêt à l'emploi
    """
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level, logging.INFO))

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.propagate = False

    return logger
