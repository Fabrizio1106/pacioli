"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.classification
===============================================================================

Description:
    Sub-package containing transaction classification services. Classifiers
    assign business categories, transaction types, and brand attributes to
    staging records using configurable YAML-driven rule sets.

Responsibilities:
    - Expose classifier classes under a single importable namespace.

Key Components:
    - TransactionClassifier: Applies ordered YAML rules against a search bag
      built from all relevant text fields to assign global_category, trans_type,
      and brand.

Notes:
    - Classification is idempotent; already-classified records are skipped.

Dependencies:
    - pandas
    - utils.logger

===============================================================================
"""
