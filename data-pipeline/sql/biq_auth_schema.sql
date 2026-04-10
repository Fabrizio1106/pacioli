--
-- PostgreSQL database dump
--

\restrict Ir24Q09daRCE6XaOrV1rjOGPfMhaTB6AgpnzItqbgSSyb5tB3VChj0lC4gPjdiz

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

ALTER TABLE IF EXISTS ONLY biq_auth.transaction_workitems DROP CONSTRAINT IF EXISTS transaction_workitems_assigned_user_id_fkey;
ALTER TABLE IF EXISTS ONLY biq_auth.transaction_locks DROP CONSTRAINT IF EXISTS transaction_locks_locked_by_id_fkey;
ALTER TABLE IF EXISTS ONLY biq_auth.reversal_requests DROP CONSTRAINT IF EXISTS reversal_requests_reviewed_by_id_fkey;
ALTER TABLE IF EXISTS ONLY biq_auth.reversal_requests DROP CONSTRAINT IF EXISTS reversal_requests_requested_by_id_fkey;
ALTER TABLE IF EXISTS ONLY biq_auth.refresh_tokens DROP CONSTRAINT IF EXISTS refresh_tokens_user_id_fkey;
ALTER TABLE IF EXISTS ONLY biq_auth.transaction_locks DROP CONSTRAINT IF EXISTS fk_lock_workitem;
ALTER TABLE IF EXISTS ONLY biq_auth.audit_log DROP CONSTRAINT IF EXISTS audit_log_user_id_fkey;
ALTER TABLE IF EXISTS ONLY biq_auth.assignment_rules DROP CONSTRAINT IF EXISTS assignment_rules_assign_to_user_id_fkey;
DROP INDEX IF EXISTS biq_auth.idx_workitems_work_status;
DROP INDEX IF EXISTS biq_auth.idx_workitems_stg_id;
DROP INDEX IF EXISTS biq_auth.idx_workitems_assigned_user;
DROP INDEX IF EXISTS biq_auth.idx_reversal_requests_stg_id;
DROP INDEX IF EXISTS biq_auth.idx_reversal_requests_status;
DROP INDEX IF EXISTS biq_auth.idx_reversal_requests_requested_by;
DROP INDEX IF EXISTS biq_auth.idx_locks_expires_at;
DROP INDEX IF EXISTS biq_auth.idx_audit_log_user_id;
DROP INDEX IF EXISTS biq_auth.idx_audit_log_created_at;
ALTER TABLE IF EXISTS ONLY biq_auth.users DROP CONSTRAINT IF EXISTS users_username_key;
ALTER TABLE IF EXISTS ONLY biq_auth.users DROP CONSTRAINT IF EXISTS users_pkey;
ALTER TABLE IF EXISTS ONLY biq_auth.users DROP CONSTRAINT IF EXISTS users_email_key;
ALTER TABLE IF EXISTS ONLY biq_auth.transaction_workitems DROP CONSTRAINT IF EXISTS transaction_workitems_pkey;
ALTER TABLE IF EXISTS ONLY biq_auth.transaction_locks DROP CONSTRAINT IF EXISTS transaction_locks_pkey;
ALTER TABLE IF EXISTS ONLY biq_auth.reversal_requests DROP CONSTRAINT IF EXISTS reversal_requests_pkey;
ALTER TABLE IF EXISTS ONLY biq_auth.refresh_tokens DROP CONSTRAINT IF EXISTS refresh_tokens_token_hash_key;
ALTER TABLE IF EXISTS ONLY biq_auth.refresh_tokens DROP CONSTRAINT IF EXISTS refresh_tokens_pkey;
ALTER TABLE IF EXISTS ONLY biq_auth.audit_log DROP CONSTRAINT IF EXISTS audit_log_pkey;
ALTER TABLE IF EXISTS ONLY biq_auth.assignment_rules DROP CONSTRAINT IF EXISTS assignment_rules_pkey;
ALTER TABLE IF EXISTS biq_auth.users ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_auth.reversal_requests ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_auth.refresh_tokens ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_auth.audit_log ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_auth.assignment_rules ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE IF EXISTS biq_auth.users_id_seq;
DROP TABLE IF EXISTS biq_auth.users;
DROP TABLE IF EXISTS biq_auth.transaction_workitems;
DROP TABLE IF EXISTS biq_auth.transaction_locks;
DROP SEQUENCE IF EXISTS biq_auth.reversal_requests_id_seq;
DROP TABLE IF EXISTS biq_auth.reversal_requests;
DROP SEQUENCE IF EXISTS biq_auth.refresh_tokens_id_seq;
DROP TABLE IF EXISTS biq_auth.refresh_tokens;
DROP SEQUENCE IF EXISTS biq_auth.audit_log_id_seq;
DROP TABLE IF EXISTS biq_auth.audit_log;
DROP SEQUENCE IF EXISTS biq_auth.assignment_rules_id_seq;
DROP TABLE IF EXISTS biq_auth.assignment_rules;
DROP SCHEMA IF EXISTS biq_auth;
--
-- Name: biq_auth; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA biq_auth;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: assignment_rules; Type: TABLE; Schema: biq_auth; Owner: -
--

CREATE TABLE biq_auth.assignment_rules (
    id integer NOT NULL,
    rule_name character varying(100) NOT NULL,
    description text,
    trans_type character varying(50),
    global_category character varying(50),
    brand character varying(50),
    enrich_customer_id character varying(50),
    assign_to_user_id integer NOT NULL,
    priority integer DEFAULT 0 NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    is_default boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    created_by character varying(50)
);


--
-- Name: TABLE assignment_rules; Type: COMMENT; Schema: biq_auth; Owner: -
--

COMMENT ON TABLE biq_auth.assignment_rules IS 'Auto-assignment rules for bank transactions to analysts.
     Evaluated in descending priority order.
     is_default=TRUE rule catches everything that did not match.';


--
-- Name: assignment_rules_id_seq; Type: SEQUENCE; Schema: biq_auth; Owner: -
--

CREATE SEQUENCE biq_auth.assignment_rules_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: assignment_rules_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_auth; Owner: -
--

ALTER SEQUENCE biq_auth.assignment_rules_id_seq OWNED BY biq_auth.assignment_rules.id;


--
-- Name: audit_log; Type: TABLE; Schema: biq_auth; Owner: -
--

CREATE TABLE biq_auth.audit_log (
    id bigint NOT NULL,
    user_id integer,
    username character varying(50),
    action character varying(50) NOT NULL,
    resource character varying(100),
    detail jsonb,
    ip_address inet,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: audit_log_id_seq; Type: SEQUENCE; Schema: biq_auth; Owner: -
--

CREATE SEQUENCE biq_auth.audit_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: audit_log_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_auth; Owner: -
--

ALTER SEQUENCE biq_auth.audit_log_id_seq OWNED BY biq_auth.audit_log.id;


--
-- Name: refresh_tokens; Type: TABLE; Schema: biq_auth; Owner: -
--

CREATE TABLE biq_auth.refresh_tokens (
    id integer NOT NULL,
    user_id integer NOT NULL,
    token_hash character varying(255) NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    revoked_at timestamp with time zone,
    ip_address inet,
    user_agent text
);


--
-- Name: refresh_tokens_id_seq; Type: SEQUENCE; Schema: biq_auth; Owner: -
--

CREATE SEQUENCE biq_auth.refresh_tokens_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: refresh_tokens_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_auth; Owner: -
--

ALTER SEQUENCE biq_auth.refresh_tokens_id_seq OWNED BY biq_auth.refresh_tokens.id;


--
-- Name: reversal_requests; Type: TABLE; Schema: biq_auth; Owner: -
--

CREATE TABLE biq_auth.reversal_requests (
    id bigint NOT NULL,
    stg_id bigint NOT NULL,
    bank_ref_1 character varying(100) NOT NULL,
    requested_by_id integer NOT NULL,
    requested_by_name character varying(100) NOT NULL,
    request_reason text NOT NULL,
    requested_at timestamp with time zone DEFAULT now() NOT NULL,
    reviewed_by_id integer,
    reviewed_by_name character varying(100),
    review_reason text,
    reviewed_at timestamp with time zone,
    status character varying(30) DEFAULT 'PENDING_APPROVAL'::character varying NOT NULL,
    gold_rpa_status character varying(30),
    gold_batch_id character varying(50),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT reversal_requests_status_check CHECK (((status)::text = ANY ((ARRAY['PENDING_APPROVAL'::character varying, 'APPROVED'::character varying, 'REJECTED'::character varying])::text[])))
);


--
-- Name: TABLE reversal_requests; Type: COMMENT; Schema: biq_auth; Owner: -
--

COMMENT ON TABLE biq_auth.reversal_requests IS 'Tracks analyst requests to reverse approved reconciliations';


--
-- Name: reversal_requests_id_seq; Type: SEQUENCE; Schema: biq_auth; Owner: -
--

CREATE SEQUENCE biq_auth.reversal_requests_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: reversal_requests_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_auth; Owner: -
--

ALTER SEQUENCE biq_auth.reversal_requests_id_seq OWNED BY biq_auth.reversal_requests.id;


--
-- Name: transaction_locks; Type: TABLE; Schema: biq_auth; Owner: -
--

CREATE TABLE biq_auth.transaction_locks (
    bank_ref_1 character varying(200) NOT NULL,
    locked_by_id integer NOT NULL,
    locked_by_name character varying(100) NOT NULL,
    locked_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    renewed_at timestamp with time zone
);


--
-- Name: TABLE transaction_locks; Type: COMMENT; Schema: biq_auth; Owner: -
--

COMMENT ON TABLE biq_auth.transaction_locks IS 'Temporary TTL locks (5 min) to prevent simultaneous editing.
     Expired locks are ignored by queries and cleaned up periodically.';


--
-- Name: transaction_workitems; Type: TABLE; Schema: biq_auth; Owner: -
--

CREATE TABLE biq_auth.transaction_workitems (
    bank_ref_1 character varying(200) NOT NULL,
    stg_id bigint NOT NULL,
    assigned_user_id integer,
    assigned_at timestamp with time zone,
    assigned_by character varying(50),
    work_status character varying(30) DEFAULT 'PENDING_ASSIGNMENT'::character varying NOT NULL,
    approved_portfolio_ids text,
    approved_amounts jsonb,
    approved_by character varying(50),
    approved_at timestamp with time zone,
    approval_notes text,
    diff_account_code character varying(20),
    diff_amount numeric(18,4),
    is_override boolean DEFAULT false NOT NULL,
    override_reason text,
    override_by character varying(50),
    override_at timestamp with time zone,
    detected_scenario character varying(50),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    approved_commission numeric(14,2) DEFAULT NULL::numeric,
    approved_tax_iva numeric(14,2) DEFAULT NULL::numeric,
    approved_tax_irf numeric(14,2) DEFAULT NULL::numeric
);


--
-- Name: TABLE transaction_workitems; Type: COMMENT; Schema: biq_auth; Owner: -
--

COMMENT ON TABLE biq_auth.transaction_workitems IS 'Operational state of bank transactions.
     Exclusively owned by Node.js backend.
     bank_ref_1 as stable business key survives daily Python snapshots.
     Fallback key: sap_description when bank_ref_1 is NULL.';


--
-- Name: users; Type: TABLE; Schema: biq_auth; Owner: -
--

CREATE TABLE biq_auth.users (
    id integer NOT NULL,
    username character varying(50) NOT NULL,
    email character varying(100) NOT NULL,
    password_hash character varying(255) NOT NULL,
    full_name character varying(100) NOT NULL,
    role character varying(20) DEFAULT 'analyst'::character varying NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    last_login_at timestamp with time zone,
    CONSTRAINT users_role_check CHECK (((role)::text = ANY ((ARRAY['admin'::character varying, 'analyst'::character varying, 'viewer'::character varying, 'senior_analyst'::character varying])::text[])))
);


--
-- Name: users_id_seq; Type: SEQUENCE; Schema: biq_auth; Owner: -
--

CREATE SEQUENCE biq_auth.users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_auth; Owner: -
--

ALTER SEQUENCE biq_auth.users_id_seq OWNED BY biq_auth.users.id;


--
-- Name: assignment_rules id; Type: DEFAULT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.assignment_rules ALTER COLUMN id SET DEFAULT nextval('biq_auth.assignment_rules_id_seq'::regclass);


--
-- Name: audit_log id; Type: DEFAULT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.audit_log ALTER COLUMN id SET DEFAULT nextval('biq_auth.audit_log_id_seq'::regclass);


--
-- Name: refresh_tokens id; Type: DEFAULT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.refresh_tokens ALTER COLUMN id SET DEFAULT nextval('biq_auth.refresh_tokens_id_seq'::regclass);


--
-- Name: reversal_requests id; Type: DEFAULT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.reversal_requests ALTER COLUMN id SET DEFAULT nextval('biq_auth.reversal_requests_id_seq'::regclass);


--
-- Name: users id; Type: DEFAULT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.users ALTER COLUMN id SET DEFAULT nextval('biq_auth.users_id_seq'::regclass);


--
-- Name: assignment_rules assignment_rules_pkey; Type: CONSTRAINT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.assignment_rules
    ADD CONSTRAINT assignment_rules_pkey PRIMARY KEY (id);


--
-- Name: audit_log audit_log_pkey; Type: CONSTRAINT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.audit_log
    ADD CONSTRAINT audit_log_pkey PRIMARY KEY (id);


--
-- Name: refresh_tokens refresh_tokens_pkey; Type: CONSTRAINT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.refresh_tokens
    ADD CONSTRAINT refresh_tokens_pkey PRIMARY KEY (id);


--
-- Name: refresh_tokens refresh_tokens_token_hash_key; Type: CONSTRAINT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.refresh_tokens
    ADD CONSTRAINT refresh_tokens_token_hash_key UNIQUE (token_hash);


--
-- Name: reversal_requests reversal_requests_pkey; Type: CONSTRAINT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.reversal_requests
    ADD CONSTRAINT reversal_requests_pkey PRIMARY KEY (id);


--
-- Name: transaction_locks transaction_locks_pkey; Type: CONSTRAINT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.transaction_locks
    ADD CONSTRAINT transaction_locks_pkey PRIMARY KEY (bank_ref_1);


--
-- Name: transaction_workitems transaction_workitems_pkey; Type: CONSTRAINT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.transaction_workitems
    ADD CONSTRAINT transaction_workitems_pkey PRIMARY KEY (bank_ref_1);


--
-- Name: users users_email_key; Type: CONSTRAINT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.users
    ADD CONSTRAINT users_email_key UNIQUE (email);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: users users_username_key; Type: CONSTRAINT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.users
    ADD CONSTRAINT users_username_key UNIQUE (username);


--
-- Name: idx_audit_log_created_at; Type: INDEX; Schema: biq_auth; Owner: -
--

CREATE INDEX idx_audit_log_created_at ON biq_auth.audit_log USING btree (created_at DESC);


--
-- Name: idx_audit_log_user_id; Type: INDEX; Schema: biq_auth; Owner: -
--

CREATE INDEX idx_audit_log_user_id ON biq_auth.audit_log USING btree (user_id);


--
-- Name: idx_locks_expires_at; Type: INDEX; Schema: biq_auth; Owner: -
--

CREATE INDEX idx_locks_expires_at ON biq_auth.transaction_locks USING btree (expires_at);


--
-- Name: idx_reversal_requests_requested_by; Type: INDEX; Schema: biq_auth; Owner: -
--

CREATE INDEX idx_reversal_requests_requested_by ON biq_auth.reversal_requests USING btree (requested_by_id);


--
-- Name: idx_reversal_requests_status; Type: INDEX; Schema: biq_auth; Owner: -
--

CREATE INDEX idx_reversal_requests_status ON biq_auth.reversal_requests USING btree (status) WHERE ((status)::text = 'PENDING_APPROVAL'::text);


--
-- Name: idx_reversal_requests_stg_id; Type: INDEX; Schema: biq_auth; Owner: -
--

CREATE INDEX idx_reversal_requests_stg_id ON biq_auth.reversal_requests USING btree (stg_id);


--
-- Name: idx_workitems_assigned_user; Type: INDEX; Schema: biq_auth; Owner: -
--

CREATE INDEX idx_workitems_assigned_user ON biq_auth.transaction_workitems USING btree (assigned_user_id);


--
-- Name: idx_workitems_stg_id; Type: INDEX; Schema: biq_auth; Owner: -
--

CREATE INDEX idx_workitems_stg_id ON biq_auth.transaction_workitems USING btree (stg_id);


--
-- Name: idx_workitems_work_status; Type: INDEX; Schema: biq_auth; Owner: -
--

CREATE INDEX idx_workitems_work_status ON biq_auth.transaction_workitems USING btree (work_status);


--
-- Name: assignment_rules assignment_rules_assign_to_user_id_fkey; Type: FK CONSTRAINT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.assignment_rules
    ADD CONSTRAINT assignment_rules_assign_to_user_id_fkey FOREIGN KEY (assign_to_user_id) REFERENCES biq_auth.users(id);


--
-- Name: audit_log audit_log_user_id_fkey; Type: FK CONSTRAINT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.audit_log
    ADD CONSTRAINT audit_log_user_id_fkey FOREIGN KEY (user_id) REFERENCES biq_auth.users(id) ON DELETE SET NULL;


--
-- Name: transaction_locks fk_lock_workitem; Type: FK CONSTRAINT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.transaction_locks
    ADD CONSTRAINT fk_lock_workitem FOREIGN KEY (bank_ref_1) REFERENCES biq_auth.transaction_workitems(bank_ref_1) ON DELETE CASCADE;


--
-- Name: refresh_tokens refresh_tokens_user_id_fkey; Type: FK CONSTRAINT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.refresh_tokens
    ADD CONSTRAINT refresh_tokens_user_id_fkey FOREIGN KEY (user_id) REFERENCES biq_auth.users(id) ON DELETE CASCADE;


--
-- Name: reversal_requests reversal_requests_requested_by_id_fkey; Type: FK CONSTRAINT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.reversal_requests
    ADD CONSTRAINT reversal_requests_requested_by_id_fkey FOREIGN KEY (requested_by_id) REFERENCES biq_auth.users(id);


--
-- Name: reversal_requests reversal_requests_reviewed_by_id_fkey; Type: FK CONSTRAINT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.reversal_requests
    ADD CONSTRAINT reversal_requests_reviewed_by_id_fkey FOREIGN KEY (reviewed_by_id) REFERENCES biq_auth.users(id);


--
-- Name: transaction_locks transaction_locks_locked_by_id_fkey; Type: FK CONSTRAINT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.transaction_locks
    ADD CONSTRAINT transaction_locks_locked_by_id_fkey FOREIGN KEY (locked_by_id) REFERENCES biq_auth.users(id);


--
-- Name: transaction_workitems transaction_workitems_assigned_user_id_fkey; Type: FK CONSTRAINT; Schema: biq_auth; Owner: -
--

ALTER TABLE ONLY biq_auth.transaction_workitems
    ADD CONSTRAINT transaction_workitems_assigned_user_id_fkey FOREIGN KEY (assigned_user_id) REFERENCES biq_auth.users(id);


--
-- PostgreSQL database dump complete
--

\unrestrict Ir24Q09daRCE6XaOrV1rjOGPfMhaTB6AgpnzItqbgSSyb5tB3VChj0lC4gPjdiz

