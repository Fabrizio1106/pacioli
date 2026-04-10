--
-- PostgreSQL database dump
--

\restrict 5MrbL7ZBCnIwQcakdkg8PRsuIW095lc3vAUSaoS7ieh5wSRKOkikwKKtSCW5Xzh

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

DROP INDEX IF EXISTS biq_raw.idx_sap_periodo;
DROP INDEX IF EXISTS biq_raw.idx_manual_ref_banco;
DROP INDEX IF EXISTS biq_raw.idx_manual_fecha;
DROP INDEX IF EXISTS biq_raw.idx_db_adq_lote_valor;
DROP INDEX IF EXISTS biq_raw.idx_bco_referencia;
DROP INDEX IF EXISTS biq_raw.idx_bco_fecha_valor;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_webpos DROP CONSTRAINT IF EXISTS uq_raw_webpos_hash;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_sap_cta_239 DROP CONSTRAINT IF EXISTS uq_raw_sap_cta_239_hash;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_retenciones_sri DROP CONSTRAINT IF EXISTS uq_raw_retenciones_hash;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_pacificard DROP CONSTRAINT IF EXISTS uq_raw_pacificard_hash;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_guayaquil DROP CONSTRAINT IF EXISTS uq_raw_guayaquil_hash;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_diners_club DROP CONSTRAINT IF EXISTS uq_raw_diners_club_hash;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_databalance DROP CONSTRAINT IF EXISTS uq_raw_databalance_hash;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_customer_portfolio DROP CONSTRAINT IF EXISTS uq_raw_customer_portfolio_hash;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_banco_239 DROP CONSTRAINT IF EXISTS uq_raw_banco_239_hash;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_webpos DROP CONSTRAINT IF EXISTS pk_raw_webpos;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_sap_cta_239 DROP CONSTRAINT IF EXISTS pk_raw_sap_cta_239;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_retenciones_sri DROP CONSTRAINT IF EXISTS pk_raw_retenciones_sri;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_pacificard DROP CONSTRAINT IF EXISTS pk_raw_pacificard;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_manual_requests DROP CONSTRAINT IF EXISTS pk_raw_manual_requests;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_guayaquil DROP CONSTRAINT IF EXISTS pk_raw_guayaquil;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_diners_club DROP CONSTRAINT IF EXISTS pk_raw_diners_club;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_databalance DROP CONSTRAINT IF EXISTS pk_raw_databalance;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_customer_portfolio DROP CONSTRAINT IF EXISTS pk_raw_customer_portfolio;
ALTER TABLE IF EXISTS ONLY biq_raw.raw_banco_239 DROP CONSTRAINT IF EXISTS pk_raw_banco_239;
ALTER TABLE IF EXISTS biq_raw.raw_webpos ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_raw.raw_sap_cta_239 ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_raw.raw_retenciones_sri ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_raw.raw_pacificard ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_raw.raw_manual_requests ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_raw.raw_guayaquil ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_raw.raw_diners_club ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_raw.raw_databalance ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_raw.raw_customer_portfolio ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS biq_raw.raw_banco_239 ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE IF EXISTS biq_raw.raw_webpos_id_seq;
DROP TABLE IF EXISTS biq_raw.raw_webpos;
DROP SEQUENCE IF EXISTS biq_raw.raw_sap_cta_239_id_seq;
DROP TABLE IF EXISTS biq_raw.raw_sap_cta_239;
DROP SEQUENCE IF EXISTS biq_raw.raw_retenciones_sri_id_seq;
DROP TABLE IF EXISTS biq_raw.raw_retenciones_sri;
DROP SEQUENCE IF EXISTS biq_raw.raw_pacificard_id_seq;
DROP TABLE IF EXISTS biq_raw.raw_pacificard;
DROP SEQUENCE IF EXISTS biq_raw.raw_manual_requests_id_seq;
DROP TABLE IF EXISTS biq_raw.raw_manual_requests;
DROP SEQUENCE IF EXISTS biq_raw.raw_guayaquil_id_seq;
DROP TABLE IF EXISTS biq_raw.raw_guayaquil;
DROP SEQUENCE IF EXISTS biq_raw.raw_diners_club_id_seq;
DROP TABLE IF EXISTS biq_raw.raw_diners_club;
DROP SEQUENCE IF EXISTS biq_raw.raw_databalance_id_seq;
DROP TABLE IF EXISTS biq_raw.raw_databalance;
DROP SEQUENCE IF EXISTS biq_raw.raw_customer_portfolio_id_seq;
DROP TABLE IF EXISTS biq_raw.raw_customer_portfolio;
DROP SEQUENCE IF EXISTS biq_raw.raw_banco_239_id_seq;
DROP TABLE IF EXISTS biq_raw.raw_banco_239;
DROP SCHEMA IF EXISTS biq_raw;
--
-- Name: biq_raw; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA biq_raw;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: raw_banco_239; Type: TABLE; Schema: biq_raw; Owner: -
--

CREATE TABLE biq_raw.raw_banco_239 (
    id integer NOT NULL,
    hash_id character(64) NOT NULL,
    fecha_transaccion timestamp without time zone NOT NULL,
    referencia character varying(50),
    referencia2 character varying(50),
    descripcion text,
    signo_movimiento character(1),
    valor numeric(18,2) NOT NULL,
    saldo_contable numeric(18,2),
    saldo_disponible numeric(18,2),
    oficina character varying(100),
    cod_transaccion character varying(20),
    source_file character varying(255),
    loaded_at timestamp without time zone DEFAULT now() NOT NULL,
    batch_id character varying(50)
);


--
-- Name: raw_banco_239_id_seq; Type: SEQUENCE; Schema: biq_raw; Owner: -
--

CREATE SEQUENCE biq_raw.raw_banco_239_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: raw_banco_239_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_raw; Owner: -
--

ALTER SEQUENCE biq_raw.raw_banco_239_id_seq OWNED BY biq_raw.raw_banco_239.id;


--
-- Name: raw_customer_portfolio; Type: TABLE; Schema: biq_raw; Owner: -
--

CREATE TABLE biq_raw.raw_customer_portfolio (
    id integer NOT NULL,
    hash_id character(64) NOT NULL,
    cuenta character varying(20),
    cliente character varying(100),
    referencia character varying(50),
    asignacion character varying(50),
    fecha_documento date NOT NULL,
    importe numeric(18,2) NOT NULL,
    fecha_de_pago date NOT NULL,
    dias character varying(5),
    texto character varying(100),
    n_documento character varying(50),
    clase_de_documento character varying(2),
    moneda_local character varying(5),
    cuenta_de_mayor character varying(20),
    doc_compensacion character varying(20),
    fecha_compensacion date,
    referencia_a_factura character varying(20),
    source_file character varying(255),
    batch_id character varying(50),
    loaded_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: raw_customer_portfolio_id_seq; Type: SEQUENCE; Schema: biq_raw; Owner: -
--

CREATE SEQUENCE biq_raw.raw_customer_portfolio_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: raw_customer_portfolio_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_raw; Owner: -
--

ALTER SEQUENCE biq_raw.raw_customer_portfolio_id_seq OWNED BY biq_raw.raw_customer_portfolio.id;


--
-- Name: raw_databalance; Type: TABLE; Schema: biq_raw; Owner: -
--

CREATE TABLE biq_raw.raw_databalance (
    id integer NOT NULL,
    hash_id character(64) NOT NULL,
    id_databalance character varying(20),
    fecha_voucher date NOT NULL,
    fecha_captura date NOT NULL,
    estado character varying(20),
    mid character varying(20),
    tid character varying(20),
    nombre_comercio character varying(50),
    alias character varying(50),
    lote character varying(20),
    referencia character varying(20),
    bin character varying(10),
    valor_total numeric(18,2) NOT NULL,
    tipo_credito character varying(10),
    cuotas character varying(2),
    autorizacion character varying(10),
    tipo_tarjeta character varying(100),
    adquirente character varying(50),
    base_0 numeric(18,2) DEFAULT 0 NOT NULL,
    base_imponible numeric(18,2) DEFAULT 0 NOT NULL,
    iva numeric(18,2) DEFAULT 0 NOT NULL,
    valor_pagado numeric(18,2) DEFAULT 0 NOT NULL,
    ret_fuente numeric(18,2) DEFAULT 0 NOT NULL,
    ret_iva numeric(18,2) DEFAULT 0 NOT NULL,
    comision numeric(18,2) DEFAULT 0 NOT NULL,
    comision_iva numeric(18,2) DEFAULT 0 NOT NULL,
    comp_ret_fuente character varying(20),
    comp_ret_iva character varying(20),
    fecha_pago date,
    servicio numeric(18,2) DEFAULT 0 NOT NULL,
    propina numeric(18,2) DEFAULT 0 NOT NULL,
    interes numeric(18,2) DEFAULT 0 NOT NULL,
    ice numeric(18,2) DEFAULT 0 NOT NULL,
    otros_impuestos numeric(18,2) DEFAULT 0 NOT NULL,
    monto_fijo numeric(18,2) DEFAULT 0 NOT NULL,
    documento_pago_nc character varying(20),
    transaccion character varying(5),
    source_file character varying(255),
    batch_id character varying(50),
    loaded_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: raw_databalance_id_seq; Type: SEQUENCE; Schema: biq_raw; Owner: -
--

CREATE SEQUENCE biq_raw.raw_databalance_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: raw_databalance_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_raw; Owner: -
--

ALTER SEQUENCE biq_raw.raw_databalance_id_seq OWNED BY biq_raw.raw_databalance.id;


--
-- Name: raw_diners_club; Type: TABLE; Schema: biq_raw; Owner: -
--

CREATE TABLE biq_raw.raw_diners_club (
    id integer NOT NULL,
    hash_id character(64) NOT NULL,
    ruc_comercio text,
    fecha_del_vale date NOT NULL,
    fecha_facturacion date NOT NULL,
    fecha_del_pago date NOT NULL,
    codigo_unico character varying(10),
    nombre_del_comercio character varying(40),
    canal_de_captura character varying(3),
    marca character varying(5),
    numero_recap_o_lote character varying(10),
    numero_vale character varying(10),
    numero_tarjeta character varying(150),
    tipo_credito character varying(2),
    valor_cuota_consumo numeric(18,2) DEFAULT 0 NOT NULL,
    valor_iva_de_cuota numeric(18,2) DEFAULT 0 NOT NULL,
    valor_otros_impuestos_cuota numeric(18,2) DEFAULT 0 NOT NULL,
    valor_propina_cuota numeric(18,2) DEFAULT 0 NOT NULL,
    valor_ice_cuota numeric(18,2) DEFAULT 0 NOT NULL,
    valor_intereses_socio_cuota numeric(18,2) DEFAULT 0 NOT NULL,
    valor_bruto_cuota numeric(18,2) DEFAULT 0 NOT NULL,
    valor_comision_cuota numeric(18,2) DEFAULT 0 NOT NULL,
    porcentaje_comision numeric(18,2) DEFAULT 0 NOT NULL,
    valor_retencion_iva_cuota numeric(18,2) DEFAULT 0 NOT NULL,
    valor_retencion_irf_cuota numeric(18,2) DEFAULT 0 NOT NULL,
    valor_pago_cuota numeric(18,2) DEFAULT 0 NOT NULL,
    cuotas_trasladadas character varying(10),
    total_cuotas character varying(10),
    valor_total_consumo numeric(18,2) DEFAULT 0 NOT NULL,
    valor_total_iva numeric(18,2) DEFAULT 0 NOT NULL,
    valor_total_otros_impuestos numeric(18,2) DEFAULT 0 NOT NULL,
    valor_total_de_la_propina numeric(18,2) DEFAULT 0 NOT NULL,
    valor_total_del_ice numeric(18,2) DEFAULT 0 NOT NULL,
    valor_total_bruto numeric(18,2) DEFAULT 0 NOT NULL,
    valor_total_comision numeric(18,2) DEFAULT 0 NOT NULL,
    valor_total_retencion_iva numeric(18,2) DEFAULT 0 NOT NULL,
    valor_total_retencion_irf numeric(18,2) DEFAULT 0 NOT NULL,
    valor_total_pago numeric(18,2) DEFAULT 0 NOT NULL,
    valor_pendiente_del_vale numeric(18,2) DEFAULT 0 NOT NULL,
    comprobante_retencion character varying(20),
    comprobante_de_pago character varying(20),
    factura character varying(20),
    estado_del_vale character varying(20),
    source_file character varying(255),
    batch_id character varying(50),
    loaded_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: raw_diners_club_id_seq; Type: SEQUENCE; Schema: biq_raw; Owner: -
--

CREATE SEQUENCE biq_raw.raw_diners_club_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: raw_diners_club_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_raw; Owner: -
--

ALTER SEQUENCE biq_raw.raw_diners_club_id_seq OWNED BY biq_raw.raw_diners_club.id;


--
-- Name: raw_guayaquil; Type: TABLE; Schema: biq_raw; Owner: -
--

CREATE TABLE biq_raw.raw_guayaquil (
    id integer NOT NULL,
    hash_id character(64) NOT NULL,
    moneda character varying(5),
    recap character varying(10),
    referencia character varying(10),
    fecha_transaccion date NOT NULL,
    ruc character varying(13),
    comercio character varying(10),
    comercio_descripcion character varying(50),
    razon_social character varying(50),
    tarjeta character varying(50),
    neto numeric(18,2) DEFAULT 0 NOT NULL,
    impuesto numeric(18,2) DEFAULT 0 NOT NULL,
    servicio numeric(18,2) DEFAULT 0 NOT NULL,
    total numeric(18,2) DEFAULT 0 NOT NULL,
    comision numeric(18,2) DEFAULT 0 NOT NULL,
    comision_iva numeric(18,2) DEFAULT 0 NOT NULL,
    retencion_fte numeric(18,2) DEFAULT 0 NOT NULL,
    retencion_iva numeric(18,2) DEFAULT 0 NOT NULL,
    a_pagar numeric(18,2) DEFAULT 0 NOT NULL,
    tipo_transaccion character varying(20),
    tipo_diferido character varying(10),
    tipo_documento character varying(10),
    marca_descripcion character varying(20),
    banco character varying(50),
    tipo_cuenta_comercio character varying(50),
    cuenta_comercio character varying(50),
    fecha_liquida date NOT NULL,
    autorizacion character varying(50),
    origen character varying(50),
    source_file character varying(255),
    batch_id character varying(50),
    loaded_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: raw_guayaquil_id_seq; Type: SEQUENCE; Schema: biq_raw; Owner: -
--

CREATE SEQUENCE biq_raw.raw_guayaquil_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: raw_guayaquil_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_raw; Owner: -
--

ALTER SEQUENCE biq_raw.raw_guayaquil_id_seq OWNED BY biq_raw.raw_guayaquil.id;


--
-- Name: raw_manual_requests; Type: TABLE; Schema: biq_raw; Owner: -
--

CREATE TABLE biq_raw.raw_manual_requests (
    id bigint NOT NULL,
    hash_id character(64),
    fecha date,
    cod_cliente text,
    cliente character varying(255),
    valor numeric(18,2) DEFAULT 0.00,
    ref_banco character varying(500),
    estado_pago text,
    emision_fac text,
    estado_fac text,
    detalle character varying(500),
    sol_sac text,
    doc_comp text,
    factura text,
    observaciones character varying(500),
    source_file character varying(255),
    batch_id character varying(100),
    loaded_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: raw_manual_requests_id_seq; Type: SEQUENCE; Schema: biq_raw; Owner: -
--

CREATE SEQUENCE biq_raw.raw_manual_requests_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: raw_manual_requests_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_raw; Owner: -
--

ALTER SEQUENCE biq_raw.raw_manual_requests_id_seq OWNED BY biq_raw.raw_manual_requests.id;


--
-- Name: raw_pacificard; Type: TABLE; Schema: biq_raw; Owner: -
--

CREATE TABLE biq_raw.raw_pacificard (
    id integer NOT NULL,
    hash_id character(64) NOT NULL,
    estacion character varying(50),
    fecha_pago date NOT NULL,
    numero_recap character varying(20),
    recap_999999 character varying(20),
    tipo_credito character varying(20),
    forma_pago character varying(20),
    numero_cuenta character varying(20),
    beneficiario character varying(50),
    numero_tarjeta character varying(20),
    consumo_tarifa_12_pct numeric(18,2) DEFAULT 0 NOT NULL,
    consumo_tarifa_0_pct numeric(18,2) DEFAULT 0 NOT NULL,
    valor_iva numeric(18,2) DEFAULT 0 NOT NULL,
    valor_ice numeric(18,2) DEFAULT 0 NOT NULL,
    valor_no_comisionable numeric(18,2) DEFAULT 0 NOT NULL,
    valor_transaccion numeric(18,2) DEFAULT 0 NOT NULL,
    valor_comision numeric(18,2) DEFAULT 0 NOT NULL,
    retencion_iva numeric(18,2) DEFAULT 0 NOT NULL,
    retencion_fuente numeric(18,2) DEFAULT 0 NOT NULL,
    valor_de_pago numeric(18,2) DEFAULT 0 NOT NULL,
    cuota_pagada character varying(5),
    valor_pendiente numeric(18,2) DEFAULT 0 NOT NULL,
    comprob_de_retencion character varying(20),
    numero_sri_autorizacion character varying(20),
    fecha_vigencia_autorizacion date NOT NULL,
    source_file character varying(255),
    batch_id character varying(50),
    loaded_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: raw_pacificard_id_seq; Type: SEQUENCE; Schema: biq_raw; Owner: -
--

CREATE SEQUENCE biq_raw.raw_pacificard_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: raw_pacificard_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_raw; Owner: -
--

ALTER SEQUENCE biq_raw.raw_pacificard_id_seq OWNED BY biq_raw.raw_pacificard.id;


--
-- Name: raw_retenciones_sri; Type: TABLE; Schema: biq_raw; Owner: -
--

CREATE TABLE biq_raw.raw_retenciones_sri (
    id integer NOT NULL,
    batch_id character varying(50),
    hash_id character varying(64),
    razon_social_emisor text,
    ruc_emisor text,
    rise text,
    rimpe text,
    agente_retencion text,
    obligado_contabilidad text,
    contribuyente_especial text,
    tipo_comp_sustento text,
    num_comp_sustento text,
    base_ret_renta double precision,
    porcentaje_ret_renta double precision,
    valor_ret_renta double precision,
    base_ret_iva double precision,
    porcentaje_ret_iva double precision,
    valor_ret_iva double precision,
    base_ret_isd double precision,
    porcentaje_ret_isd text,
    valor_ret_isd double precision,
    fecha_emision_sustento date,
    serie_comprobante_ret text,
    num_secuencial_ret text,
    fecha_emision_ret date,
    fecha_autorizacion_ret date,
    periodo_fiscal text,
    clave_acceso text,
    estado_sri text,
    num_autorizacion text,
    info_adicional text,
    nombre_archivo_origen text,
    loaded_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: raw_retenciones_sri_id_seq; Type: SEQUENCE; Schema: biq_raw; Owner: -
--

CREATE SEQUENCE biq_raw.raw_retenciones_sri_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: raw_retenciones_sri_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_raw; Owner: -
--

ALTER SEQUENCE biq_raw.raw_retenciones_sri_id_seq OWNED BY biq_raw.raw_retenciones_sri.id;


--
-- Name: raw_sap_cta_239; Type: TABLE; Schema: biq_raw; Owner: -
--

CREATE TABLE biq_raw.raw_sap_cta_239 (
    id integer NOT NULL,
    hash_id character(64) NOT NULL,
    sociedad character varying(10) DEFAULT '1000'::character varying,
    ejercicio text NOT NULL,
    posicion character varying(10) NOT NULL,
    num_documento character varying(20) NOT NULL,
    clase_documento character varying(5) NOT NULL,
    fecha_documento date NOT NULL,
    asignacion character varying(50),
    division character varying(10),
    clave_contabilizacion character varying(5),
    importe_ml numeric(18,2) NOT NULL,
    moneda_local character varying(5),
    indicador_impuestos character varying(5),
    doc_compensacion character varying(20),
    status_partida character varying(20),
    texto text,
    source_file character varying(255),
    loaded_at timestamp without time zone DEFAULT now() NOT NULL,
    batch_id character varying(50)
);


--
-- Name: raw_sap_cta_239_id_seq; Type: SEQUENCE; Schema: biq_raw; Owner: -
--

CREATE SEQUENCE biq_raw.raw_sap_cta_239_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: raw_sap_cta_239_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_raw; Owner: -
--

ALTER SEQUENCE biq_raw.raw_sap_cta_239_id_seq OWNED BY biq_raw.raw_sap_cta_239.id;


--
-- Name: raw_webpos; Type: TABLE; Schema: biq_raw; Owner: -
--

CREATE TABLE biq_raw.raw_webpos (
    id integer NOT NULL,
    hash_id character(64) NOT NULL,
    tipo_pago character varying(100),
    fecha date NOT NULL,
    hora character varying(20),
    nota_de_credito character varying(50),
    factura character varying(50),
    cliente character varying(150),
    ruc_cliente character varying(50),
    lote character varying(20),
    tarjeta_vip character varying(100),
    numero_de_referencia character varying(50),
    referencia_de_pago character varying(150),
    usuario character varying(50),
    total numeric(18,2) NOT NULL,
    clave_de_acceso character varying(100),
    autorizacion character varying(100),
    estacion character varying(100),
    source_file character varying(255),
    batch_id character varying(50),
    loaded_at timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: raw_webpos_id_seq; Type: SEQUENCE; Schema: biq_raw; Owner: -
--

CREATE SEQUENCE biq_raw.raw_webpos_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: raw_webpos_id_seq; Type: SEQUENCE OWNED BY; Schema: biq_raw; Owner: -
--

ALTER SEQUENCE biq_raw.raw_webpos_id_seq OWNED BY biq_raw.raw_webpos.id;


--
-- Name: raw_banco_239 id; Type: DEFAULT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_banco_239 ALTER COLUMN id SET DEFAULT nextval('biq_raw.raw_banco_239_id_seq'::regclass);


--
-- Name: raw_customer_portfolio id; Type: DEFAULT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_customer_portfolio ALTER COLUMN id SET DEFAULT nextval('biq_raw.raw_customer_portfolio_id_seq'::regclass);


--
-- Name: raw_databalance id; Type: DEFAULT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_databalance ALTER COLUMN id SET DEFAULT nextval('biq_raw.raw_databalance_id_seq'::regclass);


--
-- Name: raw_diners_club id; Type: DEFAULT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_diners_club ALTER COLUMN id SET DEFAULT nextval('biq_raw.raw_diners_club_id_seq'::regclass);


--
-- Name: raw_guayaquil id; Type: DEFAULT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_guayaquil ALTER COLUMN id SET DEFAULT nextval('biq_raw.raw_guayaquil_id_seq'::regclass);


--
-- Name: raw_manual_requests id; Type: DEFAULT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_manual_requests ALTER COLUMN id SET DEFAULT nextval('biq_raw.raw_manual_requests_id_seq'::regclass);


--
-- Name: raw_pacificard id; Type: DEFAULT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_pacificard ALTER COLUMN id SET DEFAULT nextval('biq_raw.raw_pacificard_id_seq'::regclass);


--
-- Name: raw_retenciones_sri id; Type: DEFAULT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_retenciones_sri ALTER COLUMN id SET DEFAULT nextval('biq_raw.raw_retenciones_sri_id_seq'::regclass);


--
-- Name: raw_sap_cta_239 id; Type: DEFAULT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_sap_cta_239 ALTER COLUMN id SET DEFAULT nextval('biq_raw.raw_sap_cta_239_id_seq'::regclass);


--
-- Name: raw_webpos id; Type: DEFAULT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_webpos ALTER COLUMN id SET DEFAULT nextval('biq_raw.raw_webpos_id_seq'::regclass);


--
-- Name: raw_banco_239 pk_raw_banco_239; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_banco_239
    ADD CONSTRAINT pk_raw_banco_239 PRIMARY KEY (id);


--
-- Name: raw_customer_portfolio pk_raw_customer_portfolio; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_customer_portfolio
    ADD CONSTRAINT pk_raw_customer_portfolio PRIMARY KEY (id);


--
-- Name: raw_databalance pk_raw_databalance; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_databalance
    ADD CONSTRAINT pk_raw_databalance PRIMARY KEY (id);


--
-- Name: raw_diners_club pk_raw_diners_club; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_diners_club
    ADD CONSTRAINT pk_raw_diners_club PRIMARY KEY (id);


--
-- Name: raw_guayaquil pk_raw_guayaquil; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_guayaquil
    ADD CONSTRAINT pk_raw_guayaquil PRIMARY KEY (id);


--
-- Name: raw_manual_requests pk_raw_manual_requests; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_manual_requests
    ADD CONSTRAINT pk_raw_manual_requests PRIMARY KEY (id);


--
-- Name: raw_pacificard pk_raw_pacificard; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_pacificard
    ADD CONSTRAINT pk_raw_pacificard PRIMARY KEY (id);


--
-- Name: raw_retenciones_sri pk_raw_retenciones_sri; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_retenciones_sri
    ADD CONSTRAINT pk_raw_retenciones_sri PRIMARY KEY (id);


--
-- Name: raw_sap_cta_239 pk_raw_sap_cta_239; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_sap_cta_239
    ADD CONSTRAINT pk_raw_sap_cta_239 PRIMARY KEY (id);


--
-- Name: raw_webpos pk_raw_webpos; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_webpos
    ADD CONSTRAINT pk_raw_webpos PRIMARY KEY (id);


--
-- Name: raw_banco_239 uq_raw_banco_239_hash; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_banco_239
    ADD CONSTRAINT uq_raw_banco_239_hash UNIQUE (hash_id);


--
-- Name: raw_customer_portfolio uq_raw_customer_portfolio_hash; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_customer_portfolio
    ADD CONSTRAINT uq_raw_customer_portfolio_hash UNIQUE (hash_id);


--
-- Name: raw_databalance uq_raw_databalance_hash; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_databalance
    ADD CONSTRAINT uq_raw_databalance_hash UNIQUE (hash_id);


--
-- Name: raw_diners_club uq_raw_diners_club_hash; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_diners_club
    ADD CONSTRAINT uq_raw_diners_club_hash UNIQUE (hash_id);


--
-- Name: raw_guayaquil uq_raw_guayaquil_hash; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_guayaquil
    ADD CONSTRAINT uq_raw_guayaquil_hash UNIQUE (hash_id);


--
-- Name: raw_pacificard uq_raw_pacificard_hash; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_pacificard
    ADD CONSTRAINT uq_raw_pacificard_hash UNIQUE (hash_id);


--
-- Name: raw_retenciones_sri uq_raw_retenciones_hash; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_retenciones_sri
    ADD CONSTRAINT uq_raw_retenciones_hash UNIQUE (hash_id);


--
-- Name: raw_sap_cta_239 uq_raw_sap_cta_239_hash; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_sap_cta_239
    ADD CONSTRAINT uq_raw_sap_cta_239_hash UNIQUE (hash_id);


--
-- Name: raw_webpos uq_raw_webpos_hash; Type: CONSTRAINT; Schema: biq_raw; Owner: -
--

ALTER TABLE ONLY biq_raw.raw_webpos
    ADD CONSTRAINT uq_raw_webpos_hash UNIQUE (hash_id);


--
-- Name: idx_bco_fecha_valor; Type: INDEX; Schema: biq_raw; Owner: -
--

CREATE INDEX idx_bco_fecha_valor ON biq_raw.raw_banco_239 USING btree (fecha_transaccion, valor);


--
-- Name: idx_bco_referencia; Type: INDEX; Schema: biq_raw; Owner: -
--

CREATE INDEX idx_bco_referencia ON biq_raw.raw_banco_239 USING btree (referencia);


--
-- Name: idx_db_adq_lote_valor; Type: INDEX; Schema: biq_raw; Owner: -
--

CREATE INDEX idx_db_adq_lote_valor ON biq_raw.raw_databalance USING btree (adquirente, lote, valor_total);


--
-- Name: idx_manual_fecha; Type: INDEX; Schema: biq_raw; Owner: -
--

CREATE INDEX idx_manual_fecha ON biq_raw.raw_manual_requests USING btree (fecha);


--
-- Name: idx_manual_ref_banco; Type: INDEX; Schema: biq_raw; Owner: -
--

CREATE INDEX idx_manual_ref_banco ON biq_raw.raw_manual_requests USING btree (ref_banco);


--
-- Name: idx_sap_periodo; Type: INDEX; Schema: biq_raw; Owner: -
--

CREATE INDEX idx_sap_periodo ON biq_raw.raw_sap_cta_239 USING btree (ejercicio, fecha_documento);


--
-- PostgreSQL database dump complete
--

\unrestrict 5MrbL7ZBCnIwQcakdkg8PRsuIW095lc3vAUSaoS7ieh5wSRKOkikwKKtSCW5Xzh

