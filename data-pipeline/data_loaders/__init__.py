"""
===============================================================================
Project: PACIOLI
Module: data_loaders
===============================================================================

Description:
    Package initializer for the bronze-layer data loaders. Exposes the
    source-specific ingestion classes used by the pipeline orchestrator.

Responsibilities:
    - Mark 'data_loaders' as a Python package.

Key Components:
    - BaseLoader and its concrete implementations live in sibling modules
      (banco_loader, databalance_loader, diners_club_loader, fbl5n_loader,
      guayaquil_loader, manual_requests_loader, master_data_loader,
      pacificard_loader, retenciones_loader, sap_239_loader, webpos_loader).

===============================================================================
"""
