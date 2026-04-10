--
-- PostgreSQL database dump
--

\restrict f4QQuUnPlA5YDcQZrp5pi39nKP9Um89zHw0JlSbBvDCmGye3Lg1meyjPmZam8Hz

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

DROP INDEX IF EXISTS biq_config.idx_window_run_id;
DROP INDEX IF EXISTS biq_config.idx_window_process_status;
DROP INDEX IF EXISTS biq_config.idx_window_periodo;
DROP INDEX IF EXISTS biq_config.idx_window_created;
DROP INDEX IF EXISTS biq_config.idx_process_config_type;
DROP INDEX IF EXISTS biq_config.idx_process_config_order;
DROP INDEX IF EXISTS biq_config.idx_logs_process;
DROP INDEX IF EXISTS biq_config.idx_logs_level;
DROP INDEX IF EXISTS biq_config.idx_logs_created;
DROP INDEX IF EXISTS biq_config.idx_logs_batch;
DROP INDEX IF EXISTS biq_config.idx_batch_started;
DROP INDEX IF EXISTS biq_config.idx_batch_process_status;
DROP INDEX IF EXISTS biq_config.idx_batch_fingerprint;
ALTER TABLE IF EXISTS ONLY biq_config.etl_process_config DROP CONSTRAINT IF EXISTS uk_process_name;
ALTER TABLE IF EXISTS ONLY biq_config.pacioli_logs DROP CONSTRAINT IF EXISTS pk_pacioli_logs;
ALTER TABLE IF EXISTS ONLY biq_config.etl_process_windows DROP CONSTRAINT IF EXISTS pk_etl_process_windows;
ALTER TABLE IF EXISTS ONLY biq_config.etl_process_config DROP CONSTRAINT IF EXISTS pk_etl_process_config;
ALTER TABLE IF EXISTS ONLY biq_config.etl_batch_executions DROP CONSTRAINT IF EXISTS pk_etl_batch_executions;
ALTER TABLE IF EXISTS biq_config.pacioli_logs ALTER COLUMN log_id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_config.etl_process_windows ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_config.etl_process_config ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE IF EXISTS biq_config.pacioli_logs_log_id_seq;
DROP TABLE IF EXISTS biq_config.pacioli_logs;
DROP SEQUENCE IF EXISTS biq_config.etl_process_windows_id_seq;
DROP TABLE IF EXISTS biq_config.etl_process_windows;
DROP SEQUENCE IF EXISTS biq_config.etl_process_config_id_seq;
DROP TABLE IF EXISTS biq_config.etl_process_config;
DROP TABLE IF EXISTS biq_config.etl_batch_executions;
DROP SCHEMA IF EXISTS biq_config;
--
-- Name: biq_config; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA biq_config;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: etl_batch_executions; Type: TABLE; Schema: biq_config; Owner: -
--

CREATE TABLE biq_config.etl_batch_executions (
    batch_id character varying(100) NOT NULL,
    process_name character varying(50) NOT NULL,
    status character varying(20) NOT NULL,
    config_fingerprint character varying(32),
    metadata jsonb,
    started_at timestamp without time zone NOT NULL,
    completed_at timestamp without time zone,
    last_heartbeat timestamp without time zone,
    duration_seconds integer,
    records_processed integer DEFAULT 0,
    records_failed integer DEFAULT 0,
    result_summary jsonb,
    error_message text,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: etl_process_config; Type: TABLE; Schema: biq_config; Owner: -
--

CREATE TABLE biq_config.etl_process_config (
    id integer NOT NULL,
    process_name character varying(100) NOT NULL,
    process_type character varying(20) NOT NULL,
    is_enabled boolean DEFAULT true,
    depends_on character varying(500),
    execution_order integer DEFAULT 0,
    description text,
    owner character varying(100),
    sla_minutes integer,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone,
    CONSTRAINT etl_process_config_process_type_check CHECK (((process_type)::text = ANY ((ARRAY['TRANSACTIONAL'::character varying, 'STATEFUL'::character varying])::text[])))
);


--
-- Name: etl_process_config_id_seq; Type: SEQUENCE; Schema: biq_config; Owner: -
--

CREATE SEQUENCE biq_config.etl_process_config_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: etl_process_config_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_config; Owner: -
--

ALTER SEQUENCE biq_config.etl_process_config_id_seq OWNED BY biq_config.etl_process_config.id;


--
-- Name: etl_process_windows; Type: TABLE; Schema: biq_config; Owner: -
--

CREATE TABLE biq_config.etl_process_windows (
    id integer NOT NULL,
    process_name character varying(100) NOT NULL,
    process_type character varying(20) NOT NULL,
    window_start date,
    window_end date,
    periodo_mes character varying(7),
    status character varying(20) DEFAULT 'PENDING'::character varying NOT NULL,
    run_id character varying(36),
    records_processed integer DEFAULT 0,
    records_failed integer DEFAULT 0,
    execution_time_seconds numeric(10,2) DEFAULT 0.00,
    config_fingerprint character varying(64),
    error_message text,
    notes text,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    started_at timestamp without time zone,
    completed_at timestamp without time zone,
    CONSTRAINT etl_process_windows_process_type_check CHECK (((process_type)::text = ANY ((ARRAY['TRANSACTIONAL'::character varying, 'STATEFUL'::character varying])::text[]))),
    CONSTRAINT etl_process_windows_status_check CHECK (((status)::text = ANY ((ARRAY['PENDING'::character varying, 'RUNNING'::character varying, 'COMPLETED'::character varying, 'FAILED'::character varying, 'SKIPPED'::character varying])::text[])))
);


--
-- Name: etl_process_windows_id_seq; Type: SEQUENCE; Schema: biq_config; Owner: -
--

CREATE SEQUENCE biq_config.etl_process_windows_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: etl_process_windows_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_config; Owner: -
--

ALTER SEQUENCE biq_config.etl_process_windows_id_seq OWNED BY biq_config.etl_process_windows.id;


--
-- Name: pacioli_logs; Type: TABLE; Schema: biq_config; Owner: -
--

CREATE TABLE biq_config.pacioli_logs (
    log_id bigint NOT NULL,
    log_level character varying(10) NOT NULL,
    process_name character varying(100),
    batch_id character varying(100),
    message text NOT NULL,
    details jsonb,
    source_file character varying(255),
    source_line integer,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    CONSTRAINT pacioli_logs_log_level_check CHECK (((log_level)::text = ANY ((ARRAY['DEBUG'::character varying, 'INFO'::character varying, 'WARNING'::character varying, 'ERROR'::character varying, 'CRITICAL'::character varying])::text[])))
);


--
-- Name: pacioli_logs_log_id_seq; Type: SEQUENCE; Schema: biq_config; Owner: -
--

CREATE SEQUENCE biq_config.pacioli_logs_log_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pacioli_logs_log_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_config; Owner: -
--

ALTER SEQUENCE biq_config.pacioli_logs_log_id_seq OWNED BY biq_config.pacioli_logs.log_id;


--
-- Name: etl_process_config id; Type: DEFAULT; Schema: biq_config; Owner: -
--

ALTER TABLE ONLY biq_config.etl_process_config ALTER COLUMN id SET DEFAULT nextval('biq_config.etl_process_config_id_seq'::regclass);


--
-- Name: etl_process_windows id; Type: DEFAULT; Schema: biq_config; Owner: -
--

ALTER TABLE ONLY biq_config.etl_process_windows ALTER COLUMN id SET DEFAULT nextval('biq_config.etl_process_windows_id_seq'::regclass);


--
-- Name: pacioli_logs log_id; Type: DEFAULT; Schema: biq_config; Owner: -
--

ALTER TABLE ONLY biq_config.pacioli_logs ALTER COLUMN log_id SET DEFAULT nextval('biq_config.pacioli_logs_log_id_seq'::regclass);


--
-- Name: etl_batch_executions pk_etl_batch_executions; Type: CONSTRAINT; Schema: biq_config; Owner: -
--

ALTER TABLE ONLY biq_config.etl_batch_executions
    ADD CONSTRAINT pk_etl_batch_executions PRIMARY KEY (batch_id);


--
-- Name: etl_process_config pk_etl_process_config; Type: CONSTRAINT; Schema: biq_config; Owner: -
--

ALTER TABLE ONLY biq_config.etl_process_config
    ADD CONSTRAINT pk_etl_process_config PRIMARY KEY (id);


--
-- Name: etl_process_windows pk_etl_process_windows; Type: CONSTRAINT; Schema: biq_config; Owner: -
--

ALTER TABLE ONLY biq_config.etl_process_windows
    ADD CONSTRAINT pk_etl_process_windows PRIMARY KEY (id);


--
-- Name: pacioli_logs pk_pacioli_logs; Type: CONSTRAINT; Schema: biq_config; Owner: -
--

ALTER TABLE ONLY biq_config.pacioli_logs
    ADD CONSTRAINT pk_pacioli_logs PRIMARY KEY (log_id);


--
-- Name: etl_process_config uk_process_name; Type: CONSTRAINT; Schema: biq_config; Owner: -
--

ALTER TABLE ONLY biq_config.etl_process_config
    ADD CONSTRAINT uk_process_name UNIQUE (process_name);


--
-- Name: idx_batch_fingerprint; Type: INDEX; Schema: biq_config; Owner: -
--

CREATE INDEX idx_batch_fingerprint ON biq_config.etl_batch_executions USING btree (config_fingerprint);


--
-- Name: idx_batch_process_status; Type: INDEX; Schema: biq_config; Owner: -
--

CREATE INDEX idx_batch_process_status ON biq_config.etl_batch_executions USING btree (process_name, status);


--
-- Name: idx_batch_started; Type: INDEX; Schema: biq_config; Owner: -
--

CREATE INDEX idx_batch_started ON biq_config.etl_batch_executions USING btree (started_at);


--
-- Name: idx_logs_batch; Type: INDEX; Schema: biq_config; Owner: -
--

CREATE INDEX idx_logs_batch ON biq_config.pacioli_logs USING btree (batch_id);


--
-- Name: idx_logs_created; Type: INDEX; Schema: biq_config; Owner: -
--

CREATE INDEX idx_logs_created ON biq_config.pacioli_logs USING btree (created_at DESC);


--
-- Name: idx_logs_level; Type: INDEX; Schema: biq_config; Owner: -
--

CREATE INDEX idx_logs_level ON biq_config.pacioli_logs USING btree (log_level);


--
-- Name: idx_logs_process; Type: INDEX; Schema: biq_config; Owner: -
--

CREATE INDEX idx_logs_process ON biq_config.pacioli_logs USING btree (process_name);


--
-- Name: idx_process_config_order; Type: INDEX; Schema: biq_config; Owner: -
--

CREATE INDEX idx_process_config_order ON biq_config.etl_process_config USING btree (execution_order);


--
-- Name: idx_process_config_type; Type: INDEX; Schema: biq_config; Owner: -
--

CREATE INDEX idx_process_config_type ON biq_config.etl_process_config USING btree (process_type);


--
-- Name: idx_window_created; Type: INDEX; Schema: biq_config; Owner: -
--

CREATE INDEX idx_window_created ON biq_config.etl_process_windows USING btree (created_at);


--
-- Name: idx_window_periodo; Type: INDEX; Schema: biq_config; Owner: -
--

CREATE INDEX idx_window_periodo ON biq_config.etl_process_windows USING btree (periodo_mes);


--
-- Name: idx_window_process_status; Type: INDEX; Schema: biq_config; Owner: -
--

CREATE INDEX idx_window_process_status ON biq_config.etl_process_windows USING btree (process_name, status);


--
-- Name: idx_window_run_id; Type: INDEX; Schema: biq_config; Owner: -
--

CREATE INDEX idx_window_run_id ON biq_config.etl_process_windows USING btree (run_id);


--
-- PostgreSQL database dump complete
--

\unrestrict f4QQuUnPlA5YDcQZrp5pi39nKP9Um89zHw0JlSbBvDCmGye3Lg1meyjPmZam8Hz

