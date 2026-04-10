--
-- PostgreSQL database dump
--

\restrict XhkO5bSznGUqPlWjCOW1cuR9AavwjjyIVhYGLbb8oeg316qPylZwifPkfiPftCL

-- Dumped from database version 18.3
-- Dumped by pg_dump version 18.3

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

DROP TRIGGER IF EXISTS trg_stg_withholdings_updated ON biq_stg.stg_withholdings;
DROP TRIGGER IF EXISTS trg_stg_customer_portfolio_updated ON biq_stg.stg_customer_portfolio;
DROP TRIGGER IF EXISTS trg_stg_card_settlements_updated ON biq_stg.stg_card_settlements;
DROP TRIGGER IF EXISTS trg_stg_card_details_updated ON biq_stg.stg_card_details;
DROP TRIGGER IF EXISTS trg_stg_bank_transactions_updated ON biq_stg.stg_bank_transactions;
DROP TRIGGER IF EXISTS trg_hash_counter_cache_updated ON biq_stg.hash_counter_cache;
DROP TRIGGER IF EXISTS trg_card_hash_counters_updated ON biq_stg.card_hash_counters;
DROP INDEX IF EXISTS biq_stg.idx_withholdings_ruc;
DROP INDEX IF EXISTS biq_stg.idx_withholdings_recon_status;
DROP INDEX IF EXISTS biq_stg.idx_withholdings_periodo;
DROP INDEX IF EXISTS biq_stg.idx_withholdings_invoice_ref;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_vip_pending;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_sync_lookup;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_status_due;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_settlement_sug;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_settlement;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_recon_status;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_recon_group;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_partial_payment;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_parent_stg_id;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_match_hash;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_invoice_ref;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_invoice_cust;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_etl_hash;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_due_date;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_doc_date;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_customer_doc;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_cust_status;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_assignment;
DROP INDEX IF EXISTS biq_stg.idx_portfolio_active_window;
DROP INDEX IF EXISTS biq_stg.idx_parking_status;
DROP INDEX IF EXISTS biq_stg.idx_parking_hash;
DROP INDEX IF EXISTS biq_stg.idx_parking_etl_hash;
DROP INDEX IF EXISTS biq_stg.idx_parking_date;
DROP INDEX IF EXISTS biq_stg.idx_manual_stg_bank_ref;
DROP INDEX IF EXISTS biq_stg.idx_manual_stg_amount;
DROP INDEX IF EXISTS biq_stg.idx_hist_fecha;
DROP INDEX IF EXISTS biq_stg.idx_hist_desc;
DROP INDEX IF EXISTS biq_stg.idx_hist_cliente;
DROP INDEX IF EXISTS biq_stg.idx_hash_cache_updated;
DROP INDEX IF EXISTS biq_stg.idx_hash_cache_brand;
DROP INDEX IF EXISTS biq_stg.idx_hash_cache_amount;
DROP INDEX IF EXISTS biq_stg.idx_exceptions_withholding;
DROP INDEX IF EXISTS biq_stg.idx_card_settlements_etl_hash;
DROP INDEX IF EXISTS biq_stg.idx_card_hash_updated;
DROP INDEX IF EXISTS biq_stg.idx_card_details_voucher_hash;
DROP INDEX IF EXISTS biq_stg.idx_card_details_settlement_establishment_status;
DROP INDEX IF EXISTS biq_stg.idx_card_details_settlement;
DROP INDEX IF EXISTS biq_stg.idx_card_details_etl_hash;
DROP INDEX IF EXISTS biq_stg.idx_bank_settlement;
DROP INDEX IF EXISTS biq_stg.idx_bank_match_hash;
DROP INDEX IF EXISTS biq_stg.idx_bank_hash_context;
DROP INDEX IF EXISTS biq_stg.idx_bank_enrich_lookup;
DROP INDEX IF EXISTS biq_stg.idx_bank_doc_date;
DROP INDEX IF EXISTS biq_stg.idx_audit_withholding;
DROP INDEX IF EXISTS biq_stg.idx_audit_invoice;
ALTER TABLE IF EXISTS ONLY biq_stg.stg_withholdings DROP CONSTRAINT IF EXISTS uq_stg_withholdings_hash;
ALTER TABLE IF EXISTS ONLY biq_stg.stg_withholdings DROP CONSTRAINT IF EXISTS uq_stg_withholdings_clave;
ALTER TABLE IF EXISTS ONLY biq_stg.hash_counter_cache DROP CONSTRAINT IF EXISTS uq_hash_counter_cache_key;
ALTER TABLE IF EXISTS ONLY biq_stg.withholdings_exceptions DROP CONSTRAINT IF EXISTS pk_withholdings_exceptions;
ALTER TABLE IF EXISTS ONLY biq_stg.stg_withholdings DROP CONSTRAINT IF EXISTS pk_stg_withholdings;
ALTER TABLE IF EXISTS ONLY biq_stg.stg_parking_pay_breakdown DROP CONSTRAINT IF EXISTS pk_stg_parking_pay_breakdown;
ALTER TABLE IF EXISTS ONLY biq_stg.stg_manual_requests DROP CONSTRAINT IF EXISTS pk_stg_manual_requests;
ALTER TABLE IF EXISTS ONLY biq_stg.stg_historical_collection_training_dataset DROP CONSTRAINT IF EXISTS pk_stg_historical_training;
ALTER TABLE IF EXISTS ONLY biq_stg.stg_customer_portfolio DROP CONSTRAINT IF EXISTS pk_stg_customer_portfolio;
ALTER TABLE IF EXISTS ONLY biq_stg.stg_card_settlements DROP CONSTRAINT IF EXISTS pk_stg_card_settlements;
ALTER TABLE IF EXISTS ONLY biq_stg.stg_card_details DROP CONSTRAINT IF EXISTS pk_stg_card_details;
ALTER TABLE IF EXISTS ONLY biq_stg.stg_bank_transactions DROP CONSTRAINT IF EXISTS pk_stg_bank_transactions;
ALTER TABLE IF EXISTS ONLY biq_stg.hash_counter_cache DROP CONSTRAINT IF EXISTS pk_hash_counter_cache;
ALTER TABLE IF EXISTS ONLY biq_stg.dim_customers DROP CONSTRAINT IF EXISTS pk_dim_customers;
ALTER TABLE IF EXISTS ONLY biq_stg.card_hash_counters DROP CONSTRAINT IF EXISTS pk_card_hash_counters;
ALTER TABLE IF EXISTS ONLY biq_stg.audit_withholdings_applied DROP CONSTRAINT IF EXISTS pk_audit_withholdings_applied;
ALTER TABLE IF EXISTS ONLY biq_stg.audit_bank_reconciliation DROP CONSTRAINT IF EXISTS pk_audit_bank_reconciliation;
ALTER TABLE IF EXISTS biq_stg.withholdings_exceptions ALTER COLUMN exception_id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_stg.stg_withholdings ALTER COLUMN stg_id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_stg.stg_parking_pay_breakdown ALTER COLUMN stg_id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_stg.stg_manual_requests ALTER COLUMN stg_id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_stg.stg_historical_collection_training_dataset ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_stg.stg_customer_portfolio ALTER COLUMN stg_id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_stg.stg_card_settlements ALTER COLUMN stg_id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_stg.stg_card_details ALTER COLUMN stg_id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_stg.stg_bank_transactions ALTER COLUMN stg_id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_stg.hash_counter_cache ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_stg.audit_withholdings_applied ALTER COLUMN audit_id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_stg.audit_bank_reconciliation ALTER COLUMN audit_id DROP DEFAULT;
DROP SEQUENCE IF EXISTS biq_stg.withholdings_exceptions_exception_id_seq;
DROP TABLE IF EXISTS biq_stg.withholdings_exceptions;
DROP SEQUENCE IF EXISTS biq_stg.stg_withholdings_stg_id_seq;
DROP TABLE IF EXISTS biq_stg.stg_withholdings;
DROP SEQUENCE IF EXISTS biq_stg.stg_parking_pay_breakdown_stg_id_seq;
DROP TABLE IF EXISTS biq_stg.stg_parking_pay_breakdown;
DROP SEQUENCE IF EXISTS biq_stg.stg_manual_requests_stg_id_seq;
DROP TABLE IF EXISTS biq_stg.stg_manual_requests;
DROP SEQUENCE IF EXISTS biq_stg.stg_historical_collection_training_dataset_id_seq;
DROP TABLE IF EXISTS biq_stg.stg_historical_collection_training_dataset;
DROP SEQUENCE IF EXISTS biq_stg.stg_customer_portfolio_stg_id_seq;
DROP TABLE IF EXISTS biq_stg.stg_customer_portfolio;
DROP SEQUENCE IF EXISTS biq_stg.stg_card_settlements_stg_id_seq;
DROP TABLE IF EXISTS biq_stg.stg_card_settlements;
DROP SEQUENCE IF EXISTS biq_stg.stg_card_details_stg_id_seq;
DROP TABLE IF EXISTS biq_stg.stg_card_details;
DROP SEQUENCE IF EXISTS biq_stg.stg_bank_transactions_stg_id_seq;
DROP TABLE IF EXISTS biq_stg.stg_bank_transactions;
DROP SEQUENCE IF EXISTS biq_stg.hash_counter_cache_id_seq;
DROP TABLE IF EXISTS biq_stg.hash_counter_cache;
DROP TABLE IF EXISTS biq_stg.dim_customers;
DROP TABLE IF EXISTS biq_stg.card_hash_counters;
DROP SEQUENCE IF EXISTS biq_stg.audit_withholdings_applied_audit_id_seq;
DROP TABLE IF EXISTS biq_stg.audit_withholdings_applied;
DROP SEQUENCE IF EXISTS biq_stg.audit_bank_reconciliation_audit_id_seq;
DROP TABLE IF EXISTS biq_stg.audit_bank_reconciliation;
DROP SCHEMA IF EXISTS biq_stg;
--
-- Name: biq_stg; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA biq_stg;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: audit_bank_reconciliation; Type: TABLE; Schema: biq_stg; Owner: -
--

CREATE TABLE biq_stg.audit_bank_reconciliation (
    audit_id bigint NOT NULL,
    bank_stg_id bigint NOT NULL,
    portfolio_stg_ids text,
    match_method character varying(50),
    match_confidence numeric(5,2),
    amount_diff numeric(15,2),
    details text,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: audit_bank_reconciliation_audit_id_seq; Type: SEQUENCE; Schema: biq_stg; Owner: -
--

CREATE SEQUENCE biq_stg.audit_bank_reconciliation_audit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: audit_bank_reconciliation_audit_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_stg; Owner: -
--

ALTER SEQUENCE biq_stg.audit_bank_reconciliation_audit_id_seq OWNED BY biq_stg.audit_bank_reconciliation.audit_id;


--
-- Name: audit_withholdings_applied; Type: TABLE; Schema: biq_stg; Owner: -
--

CREATE TABLE biq_stg.audit_withholdings_applied (
    audit_id bigint NOT NULL,
    withholding_id integer NOT NULL,
    invoice_stg_id bigint NOT NULL,
    amount_before numeric(15,2),
    amount_after numeric(15,2),
    withholding_applied numeric(15,2),
    applied_at timestamp without time zone DEFAULT now() NOT NULL,
    applied_by character varying(50) DEFAULT 'SYSTEM'::character varying,
    reversal_flag boolean DEFAULT false
);


--
-- Name: audit_withholdings_applied_audit_id_seq; Type: SEQUENCE; Schema: biq_stg; Owner: -
--

CREATE SEQUENCE biq_stg.audit_withholdings_applied_audit_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: audit_withholdings_applied_audit_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_stg; Owner: -
--

ALTER SEQUENCE biq_stg.audit_withholdings_applied_audit_id_seq OWNED BY biq_stg.audit_withholdings_applied.audit_id;


--
-- Name: card_hash_counters; Type: TABLE; Schema: biq_stg; Owner: -
--

CREATE TABLE biq_stg.card_hash_counters (
    hash_base character varying(100) NOT NULL,
    last_counter integer DEFAULT 0 NOT NULL,
    last_updated_at timestamp without time zone DEFAULT now()
);


--
-- Name: dim_customers; Type: TABLE; Schema: biq_stg; Owner: -
--

CREATE TABLE biq_stg.dim_customers (
    customer_id character varying(20) NOT NULL,
    customer_name character varying(255),
    tax_id character varying(20),
    is_active boolean DEFAULT true,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: hash_counter_cache; Type: TABLE; Schema: biq_stg; Owner: -
--

CREATE TABLE biq_stg.hash_counter_cache (
    id integer NOT NULL,
    brand character varying(50) NOT NULL,
    batch_number character varying(20) NOT NULL,
    amount_total numeric(15,2) NOT NULL,
    last_counter integer DEFAULT 0 NOT NULL,
    last_updated_date date,
    total_occurrences integer DEFAULT 0,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now()
);


--
-- Name: hash_counter_cache_id_seq; Type: SEQUENCE; Schema: biq_stg; Owner: -
--

CREATE SEQUENCE biq_stg.hash_counter_cache_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: hash_counter_cache_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_stg; Owner: -
--

ALTER SEQUENCE biq_stg.hash_counter_cache_id_seq OWNED BY biq_stg.hash_counter_cache.id;


--
-- Name: stg_bank_transactions; Type: TABLE; Schema: biq_stg; Owner: -
--

CREATE TABLE biq_stg.stg_bank_transactions (
    stg_id bigint NOT NULL,
    etl_batch_id character varying(50),
    source_system character varying(20) DEFAULT 'SAP_FBL3N'::character varying,
    doc_date date,
    posting_date date,
    doc_number character varying(50),
    doc_type character varying(10),
    doc_reference character varying(100),
    amount_total numeric(18,2),
    amount_sign character varying(1),
    currency character varying(5),
    sap_description character varying(255),
    bank_date timestamp without time zone,
    bank_ref_1 character varying(100),
    bank_ref_2 character varying(100),
    bank_description character varying(255),
    bank_office_id character varying(50),
    trans_type character varying(50),
    global_category character varying(50),
    brand character varying(50),
    batch_number character varying(50),
    match_hash_key character varying(255),
    is_compensated_sap boolean DEFAULT false,
    is_compensated_intraday boolean DEFAULT false,
    reconcile_status character varying(20) DEFAULT 'PENDING'::character varying,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    settlement_id character varying(100),
    establishment_name character varying(100),
    count_voucher_bank integer,
    count_voucher_portfolio integer,
    final_amount_gross numeric(18,2),
    final_amount_net numeric(18,2),
    final_amount_commission numeric(18,2),
    final_amount_tax_iva numeric(18,2),
    final_amount_tax_irf numeric(18,2),
    diff_adjustment numeric(15,2),
    reconcile_reason character varying(50),
    enrich_customer_id character varying(50),
    enrich_customer_name character varying(255),
    enrich_confidence_score numeric(5,2) DEFAULT 0.00,
    enrich_inference_method character varying(50),
    enrich_notes text,
    match_confidence_score numeric(5,2) DEFAULT 0.00,
    match_method character varying(50),
    matched_portfolio_ids text,
    bank_ref_match character varying(100),
    reconciled_at timestamp without time zone,
    alternative_matches text,
    updated_at timestamp without time zone DEFAULT now()
);


--
-- Name: stg_bank_transactions_stg_id_seq; Type: SEQUENCE; Schema: biq_stg; Owner: -
--

CREATE SEQUENCE biq_stg.stg_bank_transactions_stg_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_bank_transactions_stg_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_stg; Owner: -
--

ALTER SEQUENCE biq_stg.stg_bank_transactions_stg_id_seq OWNED BY biq_stg.stg_bank_transactions.stg_id;


--
-- Name: stg_card_details; Type: TABLE; Schema: biq_stg; Owner: -
--

CREATE TABLE biq_stg.stg_card_details (
    stg_id bigint NOT NULL,
    settlement_id character varying(100),
    voucher_date date,
    card_number character varying(50),
    auth_code character varying(50),
    voucher_ref character varying(50),
    batch_number character varying(50),
    amount_gross numeric(18,2),
    amount_net numeric(18,2),
    amount_commission numeric(18,2),
    amount_tax_iva numeric(18,2),
    amount_tax_irf numeric(18,2),
    brand character varying(50),
    establishment_code character varying(50),
    establishment_name character varying(50),
    voucher_hash_key character varying(255),
    reconcile_status character varying(20) DEFAULT 'PENDING'::character varying,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    etl_hash character varying(64),
    updated_at timestamp without time zone DEFAULT now()
);


--
-- Name: stg_card_details_stg_id_seq; Type: SEQUENCE; Schema: biq_stg; Owner: -
--

CREATE SEQUENCE biq_stg.stg_card_details_stg_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_card_details_stg_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_stg; Owner: -
--

ALTER SEQUENCE biq_stg.stg_card_details_stg_id_seq OWNED BY biq_stg.stg_card_details.stg_id;


--
-- Name: stg_card_settlements; Type: TABLE; Schema: biq_stg; Owner: -
--

CREATE TABLE biq_stg.stg_card_settlements (
    stg_id bigint NOT NULL,
    etl_batch_id character varying(50),
    source_file character varying(255),
    settlement_id character varying(100),
    settlement_date date,
    brand character varying(50),
    batch_number character varying(50),
    amount_gross numeric(18,2),
    amount_net numeric(18,2),
    amount_commission numeric(18,2),
    amount_tax_iva numeric(18,2),
    amount_tax_irf numeric(18,2),
    match_hash_key character varying(255),
    reconcile_status character varying(20) DEFAULT 'PENDING'::character varying,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    count_voucher integer DEFAULT 0,
    establishment_name character varying(100),
    etl_hash character varying(64),
    updated_at timestamp without time zone DEFAULT now()
);


--
-- Name: stg_card_settlements_stg_id_seq; Type: SEQUENCE; Schema: biq_stg; Owner: -
--

CREATE SEQUENCE biq_stg.stg_card_settlements_stg_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_card_settlements_stg_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_stg; Owner: -
--

ALTER SEQUENCE biq_stg.stg_card_settlements_stg_id_seq OWNED BY biq_stg.stg_card_settlements.stg_id;


--
-- Name: stg_customer_portfolio; Type: TABLE; Schema: biq_stg; Owner: -
--

CREATE TABLE biq_stg.stg_customer_portfolio (
    stg_id bigint NOT NULL,
    sap_doc_number character varying(100),
    accounting_doc character varying(100),
    customer_code character varying(50),
    customer_name character varying(200),
    assignment character varying(100),
    invoice_ref character varying(100),
    doc_date date,
    due_date date,
    amount_outstanding numeric(18,2),
    conciliable_amount numeric(18,2),
    currency character(3),
    enrich_batch character varying(100),
    enrich_ref character varying(100),
    enrich_brand character varying(100),
    enrich_user character varying(100),
    enrich_source character varying(100),
    reconcile_group character varying(100),
    match_hash_key character varying(150),
    etl_hash character varying(150),
    reconcile_status character varying(100),
    settlement_id character varying(100),
    financial_amount_gross numeric(18,2),
    financial_amount_net numeric(18,2),
    financial_commission numeric(18,2),
    financial_tax_iva numeric(18,2),
    financial_tax_irf numeric(18,2),
    match_method character varying(100),
    match_confidence character varying(20),
    is_suggestion boolean DEFAULT false,
    sap_text character varying(200),
    gl_account character varying(20),
    internal_ref character varying(100),
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    matched_bank_refs text,
    partial_payment_flag boolean DEFAULT false,
    updated_at timestamp without time zone DEFAULT now(),
    closed_at timestamp without time zone,
    reconciled_at timestamp without time zone,
    is_partial_payment boolean DEFAULT false,
    sap_residual_amount numeric(15,2) DEFAULT NULL::numeric,
    is_manual_residual boolean DEFAULT false,
    parent_stg_id bigint
);


--
-- Name: COLUMN stg_customer_portfolio.is_partial_payment; Type: COMMENT; Schema: biq_stg; Owner: -
--

COMMENT ON COLUMN biq_stg.stg_customer_portfolio.is_partial_payment IS 'TRUE si este registro aplica un pago parcial sobre la factura original. El RPA debe abrir la pestaña Part. Restante en SAP F-28 y colocar sap_residual_amount.';


--
-- Name: COLUMN stg_customer_portfolio.sap_residual_amount; Type: COMMENT; Schema: biq_stg; Owner: -
--

COMMENT ON COLUMN biq_stg.stg_customer_portfolio.sap_residual_amount IS 'Monto que debe quedar abierto en SAP cuando is_partial_payment = TRUE. Calculado como: amount_outstanding_original - monto_aplicado_en_este_registro. NULL cuando is_partial_payment = FALSE.';


--
-- Name: stg_customer_portfolio_stg_id_seq; Type: SEQUENCE; Schema: biq_stg; Owner: -
--

CREATE SEQUENCE biq_stg.stg_customer_portfolio_stg_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_customer_portfolio_stg_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_stg; Owner: -
--

ALTER SEQUENCE biq_stg.stg_customer_portfolio_stg_id_seq OWNED BY biq_stg.stg_customer_portfolio.stg_id;


--
-- Name: stg_historical_collection_training_dataset; Type: TABLE; Schema: biq_stg; Owner: -
--

CREATE TABLE biq_stg.stg_historical_collection_training_dataset (
    id integer NOT NULL,
    fecha_transaccion timestamp without time zone,
    referencia_bancaria character varying(100),
    referencia_2 character varying(255),
    descripcion_bancaria text,
    monto_banco numeric(18,2),
    cliente_nombre_manual character varying(255),
    cliente_cod_manual character varying(50),
    factura_pagada character varying(255),
    valor_cobrado_factura numeric(18,2),
    archivo_origen character varying(255),
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: stg_historical_collection_training_dataset_id_seq; Type: SEQUENCE; Schema: biq_stg; Owner: -
--

CREATE SEQUENCE biq_stg.stg_historical_collection_training_dataset_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_historical_collection_training_dataset_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_stg; Owner: -
--

ALTER SEQUENCE biq_stg.stg_historical_collection_training_dataset_id_seq OWNED BY biq_stg.stg_historical_collection_training_dataset.id;


--
-- Name: stg_manual_requests; Type: TABLE; Schema: biq_stg; Owner: -
--

CREATE TABLE biq_stg.stg_manual_requests (
    stg_id bigint NOT NULL,
    raw_id bigint,
    request_date date,
    customer_id text,
    customer_name text,
    amount numeric(18,2),
    bank_ref text,
    payment_status text,
    invoice_ref text,
    details text,
    normalization_tag text,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: stg_manual_requests_stg_id_seq; Type: SEQUENCE; Schema: biq_stg; Owner: -
--

CREATE SEQUENCE biq_stg.stg_manual_requests_stg_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_manual_requests_stg_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_stg; Owner: -
--

ALTER SEQUENCE biq_stg.stg_manual_requests_stg_id_seq OWNED BY biq_stg.stg_manual_requests.stg_id;


--
-- Name: stg_parking_pay_breakdown; Type: TABLE; Schema: biq_stg; Owner: -
--

CREATE TABLE biq_stg.stg_parking_pay_breakdown (
    stg_id bigint NOT NULL,
    settlement_date date,
    settlement_id character varying(100),
    batch_number character varying(50),
    brand character varying(50),
    amount_gross numeric(18,2),
    amount_commission numeric(18,2),
    amount_tax_iva numeric(18,2),
    amount_tax_irf numeric(18,2),
    amount_net numeric(18,2),
    count_voucher integer DEFAULT 0,
    match_hash_key character varying(255),
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    reconcile_status character varying(20) DEFAULT 'PENDING'::character varying,
    etl_hash character varying(64)
);


--
-- Name: stg_parking_pay_breakdown_stg_id_seq; Type: SEQUENCE; Schema: biq_stg; Owner: -
--

CREATE SEQUENCE biq_stg.stg_parking_pay_breakdown_stg_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_parking_pay_breakdown_stg_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_stg; Owner: -
--

ALTER SEQUENCE biq_stg.stg_parking_pay_breakdown_stg_id_seq OWNED BY biq_stg.stg_parking_pay_breakdown.stg_id;


--
-- Name: stg_withholdings; Type: TABLE; Schema: biq_stg; Owner: -
--

CREATE TABLE biq_stg.stg_withholdings (
    stg_id integer NOT NULL,
    withholding_ref character varying(100) NOT NULL,
    clave_acceso character varying(49) NOT NULL,
    hash_id character varying(64) NOT NULL,
    customer_ruc character varying(13) NOT NULL,
    customer_name_raw character varying(255),
    customer_name_normalized character varying(255),
    customer_code_sap character varying(10),
    match_confidence character varying(50),
    invoice_ref_sustento character varying(50) NOT NULL,
    invoice_ref_clean character varying(50),
    invoice_sap_doc character varying(50),
    invoice_assignment character varying(100),
    invoice_series character varying(7),
    base_ret_renta numeric(15,2) DEFAULT 0.00,
    porcentaje_ret_renta numeric(5,2) DEFAULT 0.00,
    valor_ret_renta numeric(15,2) DEFAULT 0.00,
    base_ret_iva numeric(15,2) DEFAULT 0.00,
    porcentaje_ret_iva numeric(5,2) DEFAULT 0.00,
    valor_ret_iva numeric(15,2) NOT NULL,
    total_withholding numeric(15,2) GENERATED ALWAYS AS ((valor_ret_renta + valor_ret_iva)) STORED,
    fecha_emision_ret date NOT NULL,
    fecha_autorizacion_ret date NOT NULL,
    periodo_fiscal character varying(7) NOT NULL,
    rise boolean DEFAULT false,
    rimpe boolean DEFAULT false,
    agente_retencion boolean DEFAULT false,
    obligado_contabilidad boolean DEFAULT false,
    contribuyente_especial boolean DEFAULT false,
    eligibility_status character varying(20) DEFAULT 'PENDING'::character varying,
    ineligibility_reasons jsonb,
    reconcile_status character varying(50),
    validation_status character varying(10) DEFAULT 'PASS'::character varying,
    validation_errors jsonb,
    sap_posting_status character varying(10) DEFAULT 'PENDING'::character varying,
    sap_document_number character varying(10),
    sap_fiscal_year integer,
    sap_posting_date date,
    sap_error_message text,
    is_registrable boolean DEFAULT false,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now(),
    processed_at timestamp without time zone,
    posted_at timestamp without time zone,
    source_batch_id character varying(50),
    etl_version character varying(10) DEFAULT '1.0'::character varying,
    CONSTRAINT stg_withholdings_eligibility_status_check CHECK (((eligibility_status)::text = ANY ((ARRAY['PENDING'::character varying, 'ELIGIBLE'::character varying, 'INELIGIBLE'::character varying])::text[]))),
    CONSTRAINT stg_withholdings_sap_posting_status_check CHECK (((sap_posting_status)::text = ANY ((ARRAY['PENDING'::character varying, 'READY'::character varying, 'POSTED'::character varying, 'ERROR'::character varying])::text[]))),
    CONSTRAINT stg_withholdings_validation_status_check CHECK (((validation_status)::text = ANY ((ARRAY['PASS'::character varying, 'WARNING'::character varying, 'ERROR'::character varying])::text[])))
);


--
-- Name: stg_withholdings_stg_id_seq; Type: SEQUENCE; Schema: biq_stg; Owner: -
--

CREATE SEQUENCE biq_stg.stg_withholdings_stg_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: stg_withholdings_stg_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_stg; Owner: -
--

ALTER SEQUENCE biq_stg.stg_withholdings_stg_id_seq OWNED BY biq_stg.stg_withholdings.stg_id;


--
-- Name: withholdings_exceptions; Type: TABLE; Schema: biq_stg; Owner: -
--

CREATE TABLE biq_stg.withholdings_exceptions (
    exception_id integer NOT NULL,
    withholding_id integer NOT NULL,
    exception_type character varying(50) NOT NULL,
    exception_message text,
    resolution_status character varying(20) DEFAULT 'OPEN'::character varying,
    assigned_to character varying(50),
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    resolved_at timestamp without time zone,
    resolution_notes text,
    CONSTRAINT withholdings_exceptions_exception_type_check CHECK (((exception_type)::text = ANY ((ARRAY['CUSTOMER_NOT_FOUND'::character varying, 'INVOICE_NOT_FOUND'::character varying, 'INVALID_PERCENTAGE'::character varying, 'CALCULATION_ERROR'::character varying, 'DUPLICATE'::character varying, 'RENTA_NOT_ZERO'::character varying, 'SERIES_NOT_REGISTRABLE'::character varying, 'DATE_OUT_OF_RANGE'::character varying, 'OTHER'::character varying, 'INVALID_AMOUNT'::character varying])::text[]))),
    CONSTRAINT withholdings_exceptions_resolution_status_check CHECK (((resolution_status)::text = ANY ((ARRAY['OPEN'::character varying, 'IN_PROGRESS'::character varying, 'RESOLVED'::character varying, 'CLOSED'::character varying])::text[])))
);


--
-- Name: withholdings_exceptions_exception_id_seq; Type: SEQUENCE; Schema: biq_stg; Owner: -
--

CREATE SEQUENCE biq_stg.withholdings_exceptions_exception_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: withholdings_exceptions_exception_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_stg; Owner: -
--

ALTER SEQUENCE biq_stg.withholdings_exceptions_exception_id_seq OWNED BY biq_stg.withholdings_exceptions.exception_id;


--
-- Name: audit_bank_reconciliation audit_id; Type: DEFAULT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.audit_bank_reconciliation ALTER COLUMN audit_id SET DEFAULT nextval('biq_stg.audit_bank_reconciliation_audit_id_seq'::regclass);


--
-- Name: audit_withholdings_applied audit_id; Type: DEFAULT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.audit_withholdings_applied ALTER COLUMN audit_id SET DEFAULT nextval('biq_stg.audit_withholdings_applied_audit_id_seq'::regclass);


--
-- Name: hash_counter_cache id; Type: DEFAULT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.hash_counter_cache ALTER COLUMN id SET DEFAULT nextval('biq_stg.hash_counter_cache_id_seq'::regclass);


--
-- Name: stg_bank_transactions stg_id; Type: DEFAULT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.stg_bank_transactions ALTER COLUMN stg_id SET DEFAULT nextval('biq_stg.stg_bank_transactions_stg_id_seq'::regclass);


--
-- Name: stg_card_details stg_id; Type: DEFAULT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.stg_card_details ALTER COLUMN stg_id SET DEFAULT nextval('biq_stg.stg_card_details_stg_id_seq'::regclass);


--
-- Name: stg_card_settlements stg_id; Type: DEFAULT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.stg_card_settlements ALTER COLUMN stg_id SET DEFAULT nextval('biq_stg.stg_card_settlements_stg_id_seq'::regclass);


--
-- Name: stg_customer_portfolio stg_id; Type: DEFAULT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.stg_customer_portfolio ALTER COLUMN stg_id SET DEFAULT nextval('biq_stg.stg_customer_portfolio_stg_id_seq'::regclass);


--
-- Name: stg_historical_collection_training_dataset id; Type: DEFAULT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.stg_historical_collection_training_dataset ALTER COLUMN id SET DEFAULT nextval('biq_stg.stg_historical_collection_training_dataset_id_seq'::regclass);


--
-- Name: stg_manual_requests stg_id; Type: DEFAULT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.stg_manual_requests ALTER COLUMN stg_id SET DEFAULT nextval('biq_stg.stg_manual_requests_stg_id_seq'::regclass);


--
-- Name: stg_parking_pay_breakdown stg_id; Type: DEFAULT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.stg_parking_pay_breakdown ALTER COLUMN stg_id SET DEFAULT nextval('biq_stg.stg_parking_pay_breakdown_stg_id_seq'::regclass);


--
-- Name: stg_withholdings stg_id; Type: DEFAULT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.stg_withholdings ALTER COLUMN stg_id SET DEFAULT nextval('biq_stg.stg_withholdings_stg_id_seq'::regclass);


--
-- Name: withholdings_exceptions exception_id; Type: DEFAULT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.withholdings_exceptions ALTER COLUMN exception_id SET DEFAULT nextval('biq_stg.withholdings_exceptions_exception_id_seq'::regclass);


--
-- Name: audit_bank_reconciliation pk_audit_bank_reconciliation; Type: CONSTRAINT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.audit_bank_reconciliation
    ADD CONSTRAINT pk_audit_bank_reconciliation PRIMARY KEY (audit_id);


--
-- Name: audit_withholdings_applied pk_audit_withholdings_applied; Type: CONSTRAINT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.audit_withholdings_applied
    ADD CONSTRAINT pk_audit_withholdings_applied PRIMARY KEY (audit_id);


--
-- Name: card_hash_counters pk_card_hash_counters; Type: CONSTRAINT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.card_hash_counters
    ADD CONSTRAINT pk_card_hash_counters PRIMARY KEY (hash_base);


--
-- Name: dim_customers pk_dim_customers; Type: CONSTRAINT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.dim_customers
    ADD CONSTRAINT pk_dim_customers PRIMARY KEY (customer_id);


--
-- Name: hash_counter_cache pk_hash_counter_cache; Type: CONSTRAINT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.hash_counter_cache
    ADD CONSTRAINT pk_hash_counter_cache PRIMARY KEY (id);


--
-- Name: stg_bank_transactions pk_stg_bank_transactions; Type: CONSTRAINT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.stg_bank_transactions
    ADD CONSTRAINT pk_stg_bank_transactions PRIMARY KEY (stg_id);


--
-- Name: stg_card_details pk_stg_card_details; Type: CONSTRAINT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.stg_card_details
    ADD CONSTRAINT pk_stg_card_details PRIMARY KEY (stg_id);


--
-- Name: stg_card_settlements pk_stg_card_settlements; Type: CONSTRAINT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.stg_card_settlements
    ADD CONSTRAINT pk_stg_card_settlements PRIMARY KEY (stg_id);


--
-- Name: stg_customer_portfolio pk_stg_customer_portfolio; Type: CONSTRAINT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.stg_customer_portfolio
    ADD CONSTRAINT pk_stg_customer_portfolio PRIMARY KEY (stg_id);


--
-- Name: stg_historical_collection_training_dataset pk_stg_historical_training; Type: CONSTRAINT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.stg_historical_collection_training_dataset
    ADD CONSTRAINT pk_stg_historical_training PRIMARY KEY (id);


--
-- Name: stg_manual_requests pk_stg_manual_requests; Type: CONSTRAINT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.stg_manual_requests
    ADD CONSTRAINT pk_stg_manual_requests PRIMARY KEY (stg_id);


--
-- Name: stg_parking_pay_breakdown pk_stg_parking_pay_breakdown; Type: CONSTRAINT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.stg_parking_pay_breakdown
    ADD CONSTRAINT pk_stg_parking_pay_breakdown PRIMARY KEY (stg_id);


--
-- Name: stg_withholdings pk_stg_withholdings; Type: CONSTRAINT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.stg_withholdings
    ADD CONSTRAINT pk_stg_withholdings PRIMARY KEY (stg_id);


--
-- Name: withholdings_exceptions pk_withholdings_exceptions; Type: CONSTRAINT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.withholdings_exceptions
    ADD CONSTRAINT pk_withholdings_exceptions PRIMARY KEY (exception_id);


--
-- Name: hash_counter_cache uq_hash_counter_cache_key; Type: CONSTRAINT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.hash_counter_cache
    ADD CONSTRAINT uq_hash_counter_cache_key UNIQUE (brand, batch_number, amount_total);


--
-- Name: stg_withholdings uq_stg_withholdings_clave; Type: CONSTRAINT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.stg_withholdings
    ADD CONSTRAINT uq_stg_withholdings_clave UNIQUE (clave_acceso);


--
-- Name: stg_withholdings uq_stg_withholdings_hash; Type: CONSTRAINT; Schema: biq_stg; Owner: -
--

ALTER TABLE ONLY biq_stg.stg_withholdings
    ADD CONSTRAINT uq_stg_withholdings_hash UNIQUE (hash_id);


--
-- Name: idx_audit_invoice; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_audit_invoice ON biq_stg.audit_withholdings_applied USING btree (invoice_stg_id);


--
-- Name: idx_audit_withholding; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_audit_withholding ON biq_stg.audit_withholdings_applied USING btree (withholding_id);


--
-- Name: idx_bank_doc_date; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_bank_doc_date ON biq_stg.stg_bank_transactions USING btree (doc_date);


--
-- Name: idx_bank_enrich_lookup; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_bank_enrich_lookup ON biq_stg.stg_bank_transactions USING btree (trans_type, brand, enrich_confidence_score);


--
-- Name: idx_bank_hash_context; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_bank_hash_context ON biq_stg.stg_bank_transactions USING btree (brand, amount_total, doc_date, match_hash_key);


--
-- Name: idx_bank_match_hash; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_bank_match_hash ON biq_stg.stg_bank_transactions USING btree (match_hash_key);


--
-- Name: idx_bank_settlement; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_bank_settlement ON biq_stg.stg_bank_transactions USING btree (settlement_id);


--
-- Name: idx_card_details_etl_hash; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_card_details_etl_hash ON biq_stg.stg_card_details USING btree (etl_hash);


--
-- Name: idx_card_details_settlement; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_card_details_settlement ON biq_stg.stg_card_details USING btree (settlement_id);


--
-- Name: idx_card_details_settlement_establishment_status; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_card_details_settlement_establishment_status ON biq_stg.stg_card_details USING btree (settlement_id, establishment_name, reconcile_status);


--
-- Name: idx_card_details_voucher_hash; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_card_details_voucher_hash ON biq_stg.stg_card_details USING btree (voucher_hash_key);


--
-- Name: idx_card_hash_updated; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_card_hash_updated ON biq_stg.card_hash_counters USING btree (last_updated_at);


--
-- Name: idx_card_settlements_etl_hash; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_card_settlements_etl_hash ON biq_stg.stg_card_settlements USING btree (etl_hash);


--
-- Name: idx_exceptions_withholding; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_exceptions_withholding ON biq_stg.withholdings_exceptions USING btree (withholding_id);


--
-- Name: idx_hash_cache_amount; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_hash_cache_amount ON biq_stg.hash_counter_cache USING btree (amount_total);


--
-- Name: idx_hash_cache_brand; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_hash_cache_brand ON biq_stg.hash_counter_cache USING btree (brand);


--
-- Name: idx_hash_cache_updated; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_hash_cache_updated ON biq_stg.hash_counter_cache USING btree (updated_at);


--
-- Name: idx_hist_cliente; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_hist_cliente ON biq_stg.stg_historical_collection_training_dataset USING btree (cliente_nombre_manual);


--
-- Name: idx_hist_desc; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_hist_desc ON biq_stg.stg_historical_collection_training_dataset USING gin (to_tsvector('simple'::regconfig, COALESCE(descripcion_bancaria, ''::text)));


--
-- Name: idx_hist_fecha; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_hist_fecha ON biq_stg.stg_historical_collection_training_dataset USING btree (fecha_transaccion);


--
-- Name: idx_manual_stg_amount; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_manual_stg_amount ON biq_stg.stg_manual_requests USING btree (amount);


--
-- Name: idx_manual_stg_bank_ref; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_manual_stg_bank_ref ON biq_stg.stg_manual_requests USING btree (bank_ref);


--
-- Name: idx_parking_date; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_parking_date ON biq_stg.stg_parking_pay_breakdown USING btree (settlement_date);


--
-- Name: idx_parking_etl_hash; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_parking_etl_hash ON biq_stg.stg_parking_pay_breakdown USING btree (etl_hash);


--
-- Name: idx_parking_hash; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_parking_hash ON biq_stg.stg_parking_pay_breakdown USING btree (match_hash_key);


--
-- Name: idx_parking_status; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_parking_status ON biq_stg.stg_parking_pay_breakdown USING btree (reconcile_status);


--
-- Name: idx_portfolio_active_window; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_active_window ON biq_stg.stg_customer_portfolio USING btree (reconcile_status, updated_at, conciliable_amount);


--
-- Name: idx_portfolio_assignment; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_assignment ON biq_stg.stg_customer_portfolio USING btree (assignment);


--
-- Name: idx_portfolio_cust_status; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_cust_status ON biq_stg.stg_customer_portfolio USING btree (customer_code, reconcile_status);


--
-- Name: idx_portfolio_customer_doc; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_customer_doc ON biq_stg.stg_customer_portfolio USING btree (customer_code, doc_date);


--
-- Name: idx_portfolio_doc_date; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_doc_date ON biq_stg.stg_customer_portfolio USING btree (doc_date);


--
-- Name: idx_portfolio_due_date; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_due_date ON biq_stg.stg_customer_portfolio USING btree (due_date);


--
-- Name: idx_portfolio_etl_hash; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_etl_hash ON biq_stg.stg_customer_portfolio USING btree (etl_hash);


--
-- Name: idx_portfolio_invoice_cust; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_invoice_cust ON biq_stg.stg_customer_portfolio USING btree (invoice_ref, customer_code);


--
-- Name: idx_portfolio_invoice_ref; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_invoice_ref ON biq_stg.stg_customer_portfolio USING btree (invoice_ref);


--
-- Name: idx_portfolio_match_hash; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_match_hash ON biq_stg.stg_customer_portfolio USING btree (match_hash_key);


--
-- Name: idx_portfolio_parent_stg_id; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_parent_stg_id ON biq_stg.stg_customer_portfolio USING btree (parent_stg_id) WHERE (parent_stg_id IS NOT NULL);


--
-- Name: idx_portfolio_partial_payment; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_partial_payment ON biq_stg.stg_customer_portfolio USING btree (is_partial_payment, sap_residual_amount) WHERE (is_partial_payment = true);


--
-- Name: idx_portfolio_recon_group; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_recon_group ON biq_stg.stg_customer_portfolio USING btree (reconcile_group);


--
-- Name: idx_portfolio_recon_status; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_recon_status ON biq_stg.stg_customer_portfolio USING btree (reconcile_status);


--
-- Name: idx_portfolio_settlement; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_settlement ON biq_stg.stg_customer_portfolio USING btree (settlement_id);


--
-- Name: idx_portfolio_settlement_sug; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_settlement_sug ON biq_stg.stg_customer_portfolio USING btree (settlement_id, is_suggestion, reconcile_status);


--
-- Name: idx_portfolio_status_due; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_status_due ON biq_stg.stg_customer_portfolio USING btree (reconcile_status, due_date);


--
-- Name: idx_portfolio_sync_lookup; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_sync_lookup ON biq_stg.stg_customer_portfolio USING btree (etl_hash, reconcile_status);


--
-- Name: idx_portfolio_vip_pending; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_portfolio_vip_pending ON biq_stg.stg_customer_portfolio USING btree (reconcile_group, reconcile_status, is_suggestion) WHERE (((reconcile_group)::text = 'VIP_CARD'::text) AND ((reconcile_status)::text = 'PENDING'::text));


--
-- Name: idx_withholdings_invoice_ref; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_withholdings_invoice_ref ON biq_stg.stg_withholdings USING btree (invoice_ref_sustento);


--
-- Name: idx_withholdings_periodo; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_withholdings_periodo ON biq_stg.stg_withholdings USING btree (periodo_fiscal);


--
-- Name: idx_withholdings_recon_status; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_withholdings_recon_status ON biq_stg.stg_withholdings USING btree (reconcile_status);


--
-- Name: idx_withholdings_ruc; Type: INDEX; Schema: biq_stg; Owner: -
--

CREATE INDEX idx_withholdings_ruc ON biq_stg.stg_withholdings USING btree (customer_ruc);


--
-- Name: card_hash_counters trg_card_hash_counters_updated; Type: TRIGGER; Schema: biq_stg; Owner: -
--

CREATE TRIGGER trg_card_hash_counters_updated BEFORE UPDATE ON biq_stg.card_hash_counters FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: hash_counter_cache trg_hash_counter_cache_updated; Type: TRIGGER; Schema: biq_stg; Owner: -
--

CREATE TRIGGER trg_hash_counter_cache_updated BEFORE UPDATE ON biq_stg.hash_counter_cache FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: stg_bank_transactions trg_stg_bank_transactions_updated; Type: TRIGGER; Schema: biq_stg; Owner: -
--

CREATE TRIGGER trg_stg_bank_transactions_updated BEFORE UPDATE ON biq_stg.stg_bank_transactions FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: stg_card_details trg_stg_card_details_updated; Type: TRIGGER; Schema: biq_stg; Owner: -
--

CREATE TRIGGER trg_stg_card_details_updated BEFORE UPDATE ON biq_stg.stg_card_details FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: stg_card_settlements trg_stg_card_settlements_updated; Type: TRIGGER; Schema: biq_stg; Owner: -
--

CREATE TRIGGER trg_stg_card_settlements_updated BEFORE UPDATE ON biq_stg.stg_card_settlements FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: stg_customer_portfolio trg_stg_customer_portfolio_updated; Type: TRIGGER; Schema: biq_stg; Owner: -
--

CREATE TRIGGER trg_stg_customer_portfolio_updated BEFORE UPDATE ON biq_stg.stg_customer_portfolio FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- Name: stg_withholdings trg_stg_withholdings_updated; Type: TRIGGER; Schema: biq_stg; Owner: -
--

CREATE TRIGGER trg_stg_withholdings_updated BEFORE UPDATE ON biq_stg.stg_withholdings FOR EACH ROW EXECUTE FUNCTION public.trigger_set_updated_at();


--
-- PostgreSQL database dump complete
--

\unrestrict XhkO5bSznGUqPlWjCOW1cuR9AavwjjyIVhYGLbb8oeg316qPylZwifPkfiPftCL

