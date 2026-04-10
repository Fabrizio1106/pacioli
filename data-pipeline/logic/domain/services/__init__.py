"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services
===============================================================================

Description:
    Public namespace for the domain services layer. Exposes the core business
    logic components responsible for transformation, enrichment, classification,
    hashing, compensation, aggregation, and reconciliation operations.

Responsibilities:
    - Declare the services sub-package for Python import resolution.
    - Group all domain service sub-modules under a single namespace.

Key Components:
    - transformation: Source-specific data transformers (SAP, Diners, etc.)
    - enrichment: Customer identification and data enrichment services.
    - classification: Transaction type and category classifiers.
    - hashing: Hash key generation with historical context.
    - compensation: Intraday and SAP compensation detection.
    - aggregation: Settlement and breakdown aggregation services.

Notes:
    - This package contains pure domain logic with no infrastructure dependencies.

Dependencies:
    - None (package initializer only)

===============================================================================
"""
