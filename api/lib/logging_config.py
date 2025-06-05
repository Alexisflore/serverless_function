"""
Configuration centralisée des logs pour Vercel
Utilise stdout au lieu de stderr pour éviter que les logs INFO soient marqués comme erreurs
"""

import logging
import sys

def configure_logging():
    """Configure le logging pour Vercel"""
    
    # Créer un handler qui utilise stdout au lieu de stderr
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    
    # Format des logs
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    stdout_handler.setFormatter(formatter)
    
    # Configuration du logger root
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Supprimer les handlers existants pour éviter les doublons
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Ajouter notre handler stdout
    root_logger.addHandler(stdout_handler)
    
    return root_logger

def get_logger(name):
    """Récupère un logger configuré pour Vercel"""
    configure_logging()
    return logging.getLogger(name) 