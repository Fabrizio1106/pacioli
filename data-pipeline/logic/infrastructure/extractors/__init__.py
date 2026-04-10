# logic/infrastructure/extractors/__init__.py

"""
Extractors Package

Contiene todos los extractores de datos RAW.

Extractores disponibles:
- SAPExtractor: Extrae de biq_raw.raw_sap_cta_239
- BankExtractor: Extrae de raw_banco_239

Uso:
---
from logic.infrastructure.extractors import SAPExtractor, BankExtractor
"""

from .sap_extractor import SAPExtractor
from .bank_extractor import BankExtractor

__all__ = ['SAPExtractor', 'BankExtractor']