"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.compensation
===============================================================================

Description:
    Sub-package containing compensation detection services. Identifies SAP
    documents that are already settled (either historically in SAP or within
    the same trading day) so they are excluded from active reconciliation.

Responsibilities:
    - Expose compensation handler classes under a single importable namespace.

Key Components:
    - IntradayCompensationDetector: Detects ZR documents compensated by a
      matching non-ZR document on the same date (same ref + amount + date).
    - SAPCompensationHandler: Marks documents compensated in SAP as
      CLOSED_IN_SOURCE_SAP; retains them for historical hash context.

Notes:
    - Both services are pure domain logic; they do not access the database directly.

Dependencies:
    - pandas
    - utils.logger

===============================================================================
"""
