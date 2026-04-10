--
-- PostgreSQL database dump
--

\restrict DwmT74RREn7Jitm8LuzfIJBM4DefT9bcgXPRnSaK23pnEd8kQFrymXJGtAl1OcW

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

ALTER TABLE IF EXISTS ONLY biq_gold.payment_diff DROP CONSTRAINT IF EXISTS payment_diff_header_id_fkey;
ALTER TABLE IF EXISTS ONLY biq_gold.payment_detail DROP CONSTRAINT IF EXISTS payment_detail_header_id_fkey;
DROP INDEX IF EXISTS biq_gold.idx_ph_transaction_sap;
DROP INDEX IF EXISTS biq_gold.idx_ph_rpa_status;
DROP INDEX IF EXISTS biq_gold.idx_ph_batch_date;
DROP INDEX IF EXISTS biq_gold.idx_gh_transaction_sap;
DROP INDEX IF EXISTS biq_gold.idx_gh_rpa_status;
DROP INDEX IF EXISTS biq_gold.idx_gh_batch_id;
DROP INDEX IF EXISTS biq_gold.idx_gh_batch_date;
DROP INDEX IF EXISTS biq_gold.idx_gh_bank_ref;
DROP INDEX IF EXISTS biq_gold.idx_gdiff_header_id;
DROP INDEX IF EXISTS biq_gold.idx_gdiff_batch_id;
DROP INDEX IF EXISTS biq_gold.idx_gdiff_adjustment_type;
DROP INDEX IF EXISTS biq_gold.idx_gd_portfolio_stg_id;
DROP INDEX IF EXISTS biq_gold.idx_gd_invoice_ref;
DROP INDEX IF EXISTS biq_gold.idx_gd_header_id;
ALTER TABLE IF EXISTS ONLY biq_gold.payment_header DROP CONSTRAINT IF EXISTS uq_header_idempotency;
ALTER TABLE IF EXISTS ONLY biq_gold.payment_diff DROP CONSTRAINT IF EXISTS uq_diff_header_line;
ALTER TABLE IF EXISTS ONLY biq_gold.payment_detail DROP CONSTRAINT IF EXISTS uq_detail_header_line;
ALTER TABLE IF EXISTS ONLY biq_gold.payment_header DROP CONSTRAINT IF EXISTS payment_header_pkey;
ALTER TABLE IF EXISTS ONLY biq_gold.payment_diff DROP CONSTRAINT IF EXISTS payment_diff_pkey;
ALTER TABLE IF EXISTS ONLY biq_gold.payment_detail DROP CONSTRAINT IF EXISTS payment_detail_pkey;
ALTER TABLE IF EXISTS biq_gold.payment_header ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_gold.payment_diff ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_gold.payment_detail ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE IF EXISTS biq_gold.payment_header_id_seq;
DROP TABLE IF EXISTS biq_gold.payment_header;
DROP SEQUENCE IF EXISTS biq_gold.payment_diff_id_seq;
DROP TABLE IF EXISTS biq_gold.payment_diff;
DROP SEQUENCE IF EXISTS biq_gold.payment_detail_id_seq;
DROP TABLE IF EXISTS biq_gold.payment_detail;
DROP SCHEMA IF EXISTS biq_gold;
--
-- Name: biq_gold; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA biq_gold;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: payment_detail; Type: TABLE; Schema: biq_gold; Owner: -
--

CREATE TABLE biq_gold.payment_detail (
    id bigint NOT NULL,
    header_id bigint NOT NULL,
    batch_id character varying(30) NOT NULL,
    line_number smallint NOT NULL,
    portfolio_stg_id bigint NOT NULL,
    invoice_ref character varying(50),
    assignment character varying(50),
    customer_code character varying(20) NOT NULL,
    customer_name character varying(200),
    amount_gross numeric(18,2) NOT NULL,
    financial_amount_net numeric(18,2),
    is_partial_payment boolean DEFAULT false NOT NULL,
    sap_residual_amount numeric(18,2),
    gl_account character varying(20),
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: TABLE payment_detail; Type: COMMENT; Schema: biq_gold; Owner: -
--

COMMENT ON TABLE biq_gold.payment_detail IS 'Invoice lines per SAP F-28 payment.
     N rows per header_id — one per invoice.
     is_partial_payment=TRUE activates Part.Rest. in SAP.
     Split payment Pacificard: same invoice_ref in two headers
     with complementary partial amounts.';


--
-- Name: payment_detail_id_seq; Type: SEQUENCE; Schema: biq_gold; Owner: -
--

CREATE SEQUENCE biq_gold.payment_detail_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: payment_detail_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_gold; Owner: -
--

ALTER SEQUENCE biq_gold.payment_detail_id_seq OWNED BY biq_gold.payment_detail.id;


--
-- Name: payment_diff; Type: TABLE; Schema: biq_gold; Owner: -
--

CREATE TABLE biq_gold.payment_diff (
    id bigint NOT NULL,
    header_id bigint NOT NULL,
    batch_id character varying(30) NOT NULL,
    line_number smallint NOT NULL,
    sap_posting_key character varying(5) NOT NULL,
    gl_account character varying(20) NOT NULL,
    amount numeric(18,4) NOT NULL,
    adjustment_type character varying(30) NOT NULL,
    line_text character varying(100) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: TABLE payment_diff; Type: COMMENT; Schema: biq_gold; Owner: -
--

COMMENT ON TABLE biq_gold.payment_diff IS 'Commissions, taxes and adjustments for SAP F-28.
     sap_posting_key=40 for commissions and taxes always.
     sap_posting_key=40 or 50 for diff_cambiario based on sign at runtime.
     gl_account resolved from gl-accounts.yaml — no hardcoded values.
     Only has rows when bank reported commissions/taxes
     or analyst distributed a cent differential.';


--
-- Name: payment_diff_id_seq; Type: SEQUENCE; Schema: biq_gold; Owner: -
--

CREATE SEQUENCE biq_gold.payment_diff_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: payment_diff_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_gold; Owner: -
--

ALTER SEQUENCE biq_gold.payment_diff_id_seq OWNED BY biq_gold.payment_diff.id;


--
-- Name: payment_header; Type: TABLE; Schema: biq_gold; Owner: -
--

CREATE TABLE biq_gold.payment_header (
    id bigint NOT NULL,
    idempotency_hash character varying(64) NOT NULL,
    batch_id character varying(30) NOT NULL,
    batch_date date NOT NULL,
    transaction_sap character varying(10) DEFAULT 'F-28'::character varying NOT NULL,
    bank_ref_1 character varying(200) NOT NULL,
    stg_id bigint NOT NULL,
    posting_date date NOT NULL,
    doc_class character varying(5) DEFAULT 'DZ'::character varying NOT NULL,
    period smallint NOT NULL,
    company_code character varying(10) DEFAULT '8000'::character varying NOT NULL,
    currency character varying(3) DEFAULT 'USD'::character varying NOT NULL,
    reference_text character varying(255) NOT NULL,
    bank_gl_account character varying(20) DEFAULT '1110213001'::character varying NOT NULL,
    amount numeric(18,2) NOT NULL,
    customer_code character varying(20),
    customer_name character varying(200),
    multi_customer boolean DEFAULT false NOT NULL,
    match_method character varying(50),
    match_confidence numeric(5,2),
    reconcile_reason character varying(100),
    approved_by character varying(50) NOT NULL,
    approved_at timestamp with time zone NOT NULL,
    exported_by character varying(50) NOT NULL,
    exported_at timestamp with time zone DEFAULT now() NOT NULL,
    rpa_status character varying(20) DEFAULT 'PENDING_RPA'::character varying NOT NULL,
    rpa_doc_number character varying(20),
    rpa_processed_at timestamp with time zone,
    rpa_error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    rpa_retry_count integer DEFAULT 0 NOT NULL,
    rpa_locked_at timestamp with time zone,
    rpa_locked_by character varying(50),
    CONSTRAINT payment_header_rpa_status_check CHECK (((rpa_status)::text = ANY ((ARRAY['PENDING_RPA'::character varying, 'IN_PROGRESS'::character varying, 'POSTED'::character varying, 'FAILED'::character varying, 'RETRY'::character varying])::text[])))
);


--
-- Name: TABLE payment_header; Type: COMMENT; Schema: biq_gold; Owner: -
--

COMMENT ON TABLE biq_gold.payment_header IS 'SAP F-28 payment header. One row per approved bank payment.
     idempotency_hash prevents duplicate entries across pipeline re-runs.
     transaction_sap prepared for future SAP transaction types.
     bank_gl_account default 1110213001 — future: resolved from aux table.
     RPA iterates rpa_status=PENDING_RPA ordered by batch_date ASC, id ASC.';


--
-- Name: payment_header_id_seq; Type: SEQUENCE; Schema: biq_gold; Owner: -
--

CREATE SEQUENCE biq_gold.payment_header_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: payment_header_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_gold; Owner: -
--

ALTER SEQUENCE biq_gold.payment_header_id_seq OWNED BY biq_gold.payment_header.id;


--
-- Name: payment_detail id; Type: DEFAULT; Schema: biq_gold; Owner: -
--

ALTER TABLE ONLY biq_gold.payment_detail ALTER COLUMN id SET DEFAULT nextval('biq_gold.payment_detail_id_seq'::regclass);


--
-- Name: payment_diff id; Type: DEFAULT; Schema: biq_gold; Owner: -
--

ALTER TABLE ONLY biq_gold.payment_diff ALTER COLUMN id SET DEFAULT nextval('biq_gold.payment_diff_id_seq'::regclass);


--
-- Name: payment_header id; Type: DEFAULT; Schema: biq_gold; Owner: -
--

ALTER TABLE ONLY biq_gold.payment_header ALTER COLUMN id SET DEFAULT nextval('biq_gold.payment_header_id_seq'::regclass);


--
-- Name: payment_detail payment_detail_pkey; Type: CONSTRAINT; Schema: biq_gold; Owner: -
--

ALTER TABLE ONLY biq_gold.payment_detail
    ADD CONSTRAINT payment_detail_pkey PRIMARY KEY (id);


--
-- Name: payment_diff payment_diff_pkey; Type: CONSTRAINT; Schema: biq_gold; Owner: -
--

ALTER TABLE ONLY biq_gold.payment_diff
    ADD CONSTRAINT payment_diff_pkey PRIMARY KEY (id);


--
-- Name: payment_header payment_header_pkey; Type: CONSTRAINT; Schema: biq_gold; Owner: -
--

ALTER TABLE ONLY biq_gold.payment_header
    ADD CONSTRAINT payment_header_pkey PRIMARY KEY (id);


--
-- Name: payment_detail uq_detail_header_line; Type: CONSTRAINT; Schema: biq_gold; Owner: -
--

ALTER TABLE ONLY biq_gold.payment_detail
    ADD CONSTRAINT uq_detail_header_line UNIQUE (header_id, line_number);


--
-- Name: payment_diff uq_diff_header_line; Type: CONSTRAINT; Schema: biq_gold; Owner: -
--

ALTER TABLE ONLY biq_gold.payment_diff
    ADD CONSTRAINT uq_diff_header_line UNIQUE (header_id, line_number);


--
-- Name: payment_header uq_header_idempotency; Type: CONSTRAINT; Schema: biq_gold; Owner: -
--

ALTER TABLE ONLY biq_gold.payment_header
    ADD CONSTRAINT uq_header_idempotency UNIQUE (idempotency_hash);


--
-- Name: idx_gd_header_id; Type: INDEX; Schema: biq_gold; Owner: -
--

CREATE INDEX idx_gd_header_id ON biq_gold.payment_detail USING btree (header_id);


--
-- Name: idx_gd_invoice_ref; Type: INDEX; Schema: biq_gold; Owner: -
--

CREATE INDEX idx_gd_invoice_ref ON biq_gold.payment_detail USING btree (invoice_ref);


--
-- Name: idx_gd_portfolio_stg_id; Type: INDEX; Schema: biq_gold; Owner: -
--

CREATE INDEX idx_gd_portfolio_stg_id ON biq_gold.payment_detail USING btree (portfolio_stg_id);


--
-- Name: idx_gdiff_adjustment_type; Type: INDEX; Schema: biq_gold; Owner: -
--

CREATE INDEX idx_gdiff_adjustment_type ON biq_gold.payment_diff USING btree (adjustment_type);


--
-- Name: idx_gdiff_batch_id; Type: INDEX; Schema: biq_gold; Owner: -
--

CREATE INDEX idx_gdiff_batch_id ON biq_gold.payment_diff USING btree (batch_id);


--
-- Name: idx_gdiff_header_id; Type: INDEX; Schema: biq_gold; Owner: -
--

CREATE INDEX idx_gdiff_header_id ON biq_gold.payment_diff USING btree (header_id);


--
-- Name: idx_gh_bank_ref; Type: INDEX; Schema: biq_gold; Owner: -
--

CREATE INDEX idx_gh_bank_ref ON biq_gold.payment_header USING btree (bank_ref_1);


--
-- Name: idx_gh_batch_date; Type: INDEX; Schema: biq_gold; Owner: -
--

CREATE INDEX idx_gh_batch_date ON biq_gold.payment_header USING btree (batch_date);


--
-- Name: idx_gh_batch_id; Type: INDEX; Schema: biq_gold; Owner: -
--

CREATE INDEX idx_gh_batch_id ON biq_gold.payment_header USING btree (batch_id);


--
-- Name: idx_gh_rpa_status; Type: INDEX; Schema: biq_gold; Owner: -
--

CREATE INDEX idx_gh_rpa_status ON biq_gold.payment_header USING btree (rpa_status);


--
-- Name: idx_gh_transaction_sap; Type: INDEX; Schema: biq_gold; Owner: -
--

CREATE INDEX idx_gh_transaction_sap ON biq_gold.payment_header USING btree (transaction_sap);


--
-- Name: idx_ph_batch_date; Type: INDEX; Schema: biq_gold; Owner: -
--

CREATE INDEX idx_ph_batch_date ON biq_gold.payment_header USING btree (batch_date);


--
-- Name: idx_ph_rpa_status; Type: INDEX; Schema: biq_gold; Owner: -
--

CREATE INDEX idx_ph_rpa_status ON biq_gold.payment_header USING btree (rpa_status);


--
-- Name: idx_ph_transaction_sap; Type: INDEX; Schema: biq_gold; Owner: -
--

CREATE INDEX idx_ph_transaction_sap ON biq_gold.payment_header USING btree (transaction_sap);


--
-- Name: payment_detail payment_detail_header_id_fkey; Type: FK CONSTRAINT; Schema: biq_gold; Owner: -
--

ALTER TABLE ONLY biq_gold.payment_detail
    ADD CONSTRAINT payment_detail_header_id_fkey FOREIGN KEY (header_id) REFERENCES biq_gold.payment_header(id) ON DELETE CASCADE;


--
-- Name: payment_diff payment_diff_header_id_fkey; Type: FK CONSTRAINT; Schema: biq_gold; Owner: -
--

ALTER TABLE ONLY biq_gold.payment_diff
    ADD CONSTRAINT payment_diff_header_id_fkey FOREIGN KEY (header_id) REFERENCES biq_gold.payment_header(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict DwmT74RREn7Jitm8LuzfIJBM4DefT9bcgXPRnSaK23pnEd8kQFrymXJGtAl1OcW

