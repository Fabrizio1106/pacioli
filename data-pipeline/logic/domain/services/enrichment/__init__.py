"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.enrichment
===============================================================================

Description:
    Sub-package containing all customer enrichment services. Each enricher
    represents a distinct strategy in the waterfall enrichment pipeline,
    applied in priority order to identify the customer behind each transaction.

Responsibilities:
    - Expose enricher classes for use by higher-level orchestrators.
    - Group enrichment strategies under a single importable namespace.

Key Components:
    - ManualRequestEnricher: Phase 0 — manual overrides with 100% confidence.
    - CardEnricher: Phase 1 — deterministic brand-based rules.
    - SpecificTextEnricher: Phase 2 — hardcoded YAML text-pattern rules.
    - SmartHeuristicEnricher: Phase 3 — fuzzy matching via rapidfuzz.
    - CashDepositEnricher: Phase 4 — cash deposit sequence detection.
    - SettlementEnricher: Phase 5 — settlement voucher data copy.
    - BankEnricher: SAP-to-bank reference JOIN with smart suffix matching.

Notes:
    - Enrichers operate on biq_stg.stg_bank_transactions and do not alter raw data.

Dependencies:
    - sqlalchemy
    - pandas
    - utils.logger

===============================================================================
"""
