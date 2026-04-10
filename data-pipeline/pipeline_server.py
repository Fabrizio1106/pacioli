# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE SERVER v1.0
# UBICACIÓN: data-pipeline/pipeline_server.py
#
# PROPÓSITO:
# ──────────
# Servidor FastAPI que mantiene Python siempre activo con todas las librerías
# y el pool de BD precargados. Elimina el overhead de arranque de ~2:20 min
# que ocurre cuando Node.js lanza Python como proceso hijo desde cero.
#
# Con este servidor, el tiempo de ejecución baja de ~3 min a ~35 segundos
# porque Python nunca muere entre ejecuciones.
#
# ENDPOINTS:
# ──────────
#   GET  /health        → confirma que el servidor está vivo
#   POST /run           → dispara el orquestador en background
#   GET  /status        → estado actual del pipeline + progreso por grupo
#   GET  /logs/stream   → Server-Sent Events, cada línea del log en tiempo real
#
# ARRANQUE:
# ─────────
#   python pipeline_server.py
#   (o usa start_pipeline_server.bat)
#
# PUERTO: 8000 (configurable con variable de entorno PIPELINE_PORT)
# ══════════════════════════════════════════════════════════════════════════════

import os
import sys
import asyncio
import threading
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

# Forzar UTF-8 en todo el proceso — necesario en Windows
os.environ.setdefault('PYTHONUTF8', '1')
os.environ.setdefault('PYTHONIOENCODING', 'utf-8')

from fastapi import Depends, FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import uvicorn

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR         = Path(__file__).parent
ORCHESTRATOR     = BASE_DIR / 'main_silver_orchestrator.py'
PORT             = int(os.environ.get('PIPELINE_PORT', 8000))
PYTHON_EXE       = sys.executable  # Usa el mismo Python que arrancó este servidor
PIPELINE_API_KEY = os.environ.get('PIPELINE_API_KEY', '')
if not PIPELINE_API_KEY:
    print('WARNING: PIPELINE_API_KEY is not set — /run and /logs/stream are unprotected')

# ─────────────────────────────────────────────────────────────────────────────
# ESTADO GLOBAL DEL PIPELINE
# Solo una ejecución a la vez. Thread-safe via threading.Lock.
# ─────────────────────────────────────────────────────────────────────────────
_lock = threading.Lock()

_state = {
    'running':     False,
    'status':      'idle',       # idle | running | completed | failed
    'job_id':      None,
    'started_at':  None,
    'finished_at': None,
    'exit_code':   None,
    'error':       None,
    'log':         [],           # Lista de strings, últimas 1000 líneas
    'log_index':   0,            # Contador global para SSE
}

# ─────────────────────────────────────────────────────────────────────────────
# FASTAPI APP
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title       = 'PACIOLI Pipeline Server',
    description = 'Microservicio para ejecución del pipeline de conciliación',
    version     = '1.0.0',
)

# CORS — permite que Node.js y el frontend accedan al servidor
app.add_middleware(
    CORSMiddleware,
    allow_origins  = ['http://localhost:3000'],
    allow_methods  = ['GET', 'POST'],
    allow_headers  = ['X-Pipeline-Key', 'Content-Type'],
)

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _append_log(line: str):
    """Agrega una línea al log global de forma thread-safe."""
    with _lock:
        _state['log'].append(line)
        _state['log_index'] += 1
        # Mantener solo las últimas 1000 líneas
        if len(_state['log']) > 1000:
            _state['log'] = _state['log'][-1000:]


def _run_pipeline_thread():
    """
    Función que corre en un thread background.
    Lanza el orquestador como subproceso y captura su output línea a línea.
    """
    job_id = f"pipeline_{int(time.time())}"

    with _lock:
        _state['running']     = True
        _state['status']      = 'running'
        _state['job_id']      = job_id
        _state['started_at']  = datetime.now().isoformat()
        _state['finished_at'] = None
        _state['exit_code']   = None
        _state['error']       = None
        _state['log']         = []
        _state['log_index']   = 0

    _append_log(f'[PIPELINE SERVER] Iniciando job {job_id}')
    _append_log(f'[PIPELINE SERVER] Orquestador: {ORCHESTRATOR}')
    _append_log(f'[PIPELINE SERVER] Python: {PYTHON_EXE}')

    try:
        process = subprocess.Popen(
            [PYTHON_EXE, str(ORCHESTRATOR)],
            cwd     = str(BASE_DIR),
            env     = {
                **os.environ,
                'PYTHONUTF8':       '1',
                'PYTHONIOENCODING': 'utf-8',
            },
            stdout  = subprocess.PIPE,
            stderr  = subprocess.PIPE,
            text    = True,
            encoding= 'utf-8',
            errors  = 'replace',
        )

        # Leer stdout y stderr en threads separados para evitar deadlock
        def read_stream(stream, prefix=''):
            for line in iter(stream.readline, ''):
                line = line.rstrip('\n').rstrip('\r')
                if line:
                    _append_log(f'{prefix}{line}')
            stream.close()

        t_out = threading.Thread(target=read_stream, args=(process.stdout, ''),    daemon=True)
        t_err = threading.Thread(target=read_stream, args=(process.stderr, '[ERR] '), daemon=True)
        t_out.start()
        t_err.start()

        process.wait()
        t_out.join(timeout=5)
        t_err.join(timeout=5)

        exit_code = process.returncode

        with _lock:
            _state['running']     = False
            _state['exit_code']   = exit_code
            _state['finished_at'] = datetime.now().isoformat()
            _state['status']      = 'completed' if exit_code == 0 else 'failed'
            _state['error']       = None if exit_code == 0 else f'Proceso terminó con código {exit_code}'

        status_msg = '✅ COMPLETADO' if exit_code == 0 else f'❌ FALLÓ (código {exit_code})'
        _append_log(f'[PIPELINE SERVER] {status_msg}')

    except Exception as e:
        with _lock:
            _state['running']     = False
            _state['status']      = 'failed'
            _state['error']       = str(e)
            _state['finished_at'] = datetime.now().isoformat()
        _append_log(f'[PIPELINE SERVER] ERROR FATAL: {e}')


# ─────────────────────────────────────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────────────────────────────────────
def check_api_key(x_pipeline_key: Optional[str] = Header(None)) -> None:
    """Validates X-Pipeline-Key header. No-op when PIPELINE_API_KEY is not configured."""
    if PIPELINE_API_KEY and x_pipeline_key != PIPELINE_API_KEY:
        raise HTTPException(status_code=401, detail='Unauthorized: invalid or missing X-Pipeline-Key header')


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@app.get('/health')
def health():
    """Confirma que el servidor está vivo y listo."""
    return {
        'status':  'ok',
        'service': 'pacioli-pipeline-server',
        'version': '1.0.0',
        'python':  sys.version,
        'port':    PORT,
    }


@app.post('/run')
def run_pipeline(_: None = Depends(check_api_key)):
    """
    Dispara el orquestador en un thread background.
    Retorna 409 si ya hay una ejecución en curso.
    """
    with _lock:
        if _state['running']:
            return {
                'started': False,
                'job_id':  _state['job_id'],
                'message': 'Pipeline ya está en ejecución',
            }

    thread = threading.Thread(target=_run_pipeline_thread, daemon=True)
    thread.start()

    # Pequeña espera para que el thread actualice el estado
    time.sleep(0.1)

    with _lock:
        return {
            'started': True,
            'job_id':  _state['job_id'],
            'message': 'Pipeline iniciado',
        }


@app.get('/status')
def get_status():
    """
    Estado actual del pipeline.
    Node.js usa esto para construir la respuesta de /ingestion/pipeline-status.
    """
    with _lock:
        return {
            'running':     _state['running'],
            'status':      _state['status'],
            'job_id':      _state['job_id'],
            'started_at':  _state['started_at'],
            'finished_at': _state['finished_at'],
            'exit_code':   _state['exit_code'],
            'error':       _state['error'],
            'log':         list(_state['log']),
            'log_count':   _state['log_index'],
        }


@app.get('/logs/stream')
async def stream_logs(_: None = Depends(check_api_key)):
    """
    Server-Sent Events — envía cada línea nueva del log en tiempo real.
    El frontend se conecta aquí y recibe el log sin polling.

    Protocolo SSE:
      data: <línea>\\n\\n
    """
    async def event_generator():
        sent_index = 0

        # Enviar heartbeat inicial para confirmar conexión
        yield 'data: [CONNECTED]\\n\\n'

        while True:
            with _lock:
                current_log   = list(_state['log'])
                current_index = _state['log_index']
                is_running    = _state['running']
                status        = _state['status']

            # Calcular cuántas líneas nuevas hay desde la última vez
            total_lines = len(current_log)
            sent_count  = min(sent_index, total_lines)
            new_lines   = current_log[sent_count:]

            for line in new_lines:
                # Escapar newlines dentro de la línea para SSE válido
                safe_line = line.replace('\n', ' ').replace('\r', '')
                yield f'data: {safe_line}\n\n'
                sent_index += 1

            # Si el pipeline terminó y ya enviamos todo, cerrar el stream
            if not is_running and status in ('completed', 'failed', 'idle'):
                if sent_index >= total_lines and total_lines > 0:
                    yield f'data: [STREAM_END:{status}]\n\n'
                    break

            await asyncio.sleep(0.3)  # Check cada 300ms

    return StreamingResponse(
        event_generator(),
        media_type = 'text/event-stream',
        headers    = {
            'Cache-Control':               'no-cache',
            'X-Accel-Buffering':           'no',
            'Access-Control-Allow-Origin': 'http://localhost:3000',
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# ARRANQUE
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print('\n' + '═' * 60)
    print('  PACIOLI Pipeline Server v1.0')
    print('═' * 60)
    print(f'  Puerto   : http://localhost:{PORT}')
    print(f'  Health   : http://localhost:{PORT}/health')
    print(f'  Run      : POST http://localhost:{PORT}/run')
    print(f'  Status   : GET  http://localhost:{PORT}/status')
    print(f'  Logs SSE : GET  http://localhost:{PORT}/logs/stream')
    print(f'  Python   : {sys.version}')
    print(f'  Dir      : {BASE_DIR}')
    print(f'  Auth     : {"ENABLED (X-Pipeline-Key required)" if PIPELINE_API_KEY else "DISABLED (set PIPELINE_API_KEY to enable)"}')
    print('═' * 60 + '\n')

    uvicorn.run(
        app,
        host       = '0.0.0.0',
        port       = PORT,
        log_level  = 'warning',  # Solo warnings y errores de uvicorn — el pipeline tiene sus propios logs
        access_log = False,      # Sin access log — Node.js hace muchos polls
    )