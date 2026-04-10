"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.transformation
===============================================================================

Description:
    Sub-package containing source-specific data transformation services.
    Each transformer converts raw extracted data into clean, normalized records
    ready for staging, applying all source-specific business rules.

Responsibilities:
    - Expose transformer classes under a single importable namespace.
    - Provide stateless, configuration-driven transformation logic per source.

Key Components:
    - SAPTransformer: Normalizes raw SAP (FBL5N) records.
    - DinersTransformer: Cleans and hashes Diners Club vouchers.
    - GuayaquilTransformer: Transforms Banco Guayaquil AMEX vouchers.
    - PacificardTransformer: Pacificard vouchers with DataBalance cross-reference.
    - WithholdingsTransformer: SRI withholding records with series classification.
    - ManualRequestsTransformer: Manual requests with multi-reference explosion.

Notes:
    - Transformers receive DataFrames and return DataFrames; they do not touch the DB.

Dependencies:
    - pandas
    - utils.data_cleaner
    - utils.logger

===============================================================================
"""
