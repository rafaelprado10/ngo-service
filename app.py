import logging
import os
import sys
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv
from flask import Flask, request, jsonify

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)

load_dotenv()

app = Flask(__name__)

SERVICE_NAME = "ngo-service"
HOSTNAME = os.getenv("HOSTNAME", os.uname().nodename)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "host=localhost port=5432 dbname=ngo_service "
    "user=postgres password=postgres",
)


def generate_tid():
    return str(time.time_ns())


def log_operation(tid, operation):
    log.info(
        "%s | %s | %s | %s | %s",
        datetime.now(timezone.utc).isoformat(),
        SERVICE_NAME,
        HOSTNAME,
        tid,
        operation,
    )


if not DATABASE_URL:
    log.critical("Erro: DATABASE_URL não definida.")
    sys.exit(1)

try:
    pool = SimpleConnectionPool(1, 10, dsn=DATABASE_URL)
    log.info("Pool de conexões com o PostgreSQL (ngo-service) inicializado.")
except Exception as e:
    log.critical(f"Erro ao conectar ao PostgreSQL: {e}")
    sys.exit(1)


@app.route('/health')
def health():
    tid = generate_tid()
    log_operation(tid, "GET /health started")
    response = {"status": "ok", "service": SERVICE_NAME}
    log_operation(tid, "health.ok")
    return jsonify(response)


@app.route('/ngos', methods=['POST'])
def create_ngo():
    start = time.time()
    tid = generate_tid()

    log_operation(tid, f"{request.method} {request.path} started")
    log_operation(
        tid,
        (
            f"request method={request.method} "
            f"path={request.path} "
            f"remote={request.remote_addr}"
        ),
    )

    data = request.get_json()
    if not data or not all(
        k in data for k in ('name', 'email', 'cause', 'city')
    ):
        log_operation(tid, "request.validation_failed missing_fields")
        return jsonify({"error": "Campos obrigatórios ausentes"}), 400

    conn = pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                (
                    "INSERT INTO ngos (name, email, cause, city) "
                    "VALUES (%s, %s, %s, %s) RETURNING *"
                ),
                (
                    data['name'],
                    data['email'],
                    data['cause'],
                    data['city'],
                ),
            )
            new_ngo = cur.fetchone()
            conn.commit()

            log_operation(
                tid,
                (
                    f"ngo.created name={data['name']} "
                    f"email={data['email']} "
                    f"duration_ms={(time.time() - start) * 1000:.0f}"
                ),
            )
            return jsonify(new_ngo), 201
    except psycopg2.IntegrityError:
        conn.rollback()
        log_operation(tid, "ngo.create_conflict email_already_exists")
        return jsonify({"error": "E-mail já cadastrado"}), 409
    except Exception as e:
        conn.rollback()
        log_operation(tid, f"Error creating NGO: {e}")
        log.error("Erro ao criar ONG: %s", e)
        return jsonify({"error": "Erro interno"}), 500
    finally:
        pool.putconn(conn)


@app.route('/ngos', methods=['GET'])
def get_ngos():
    start = time.time()
    tid = generate_tid()

    log_operation(tid, f"{request.method} {request.path} started")
    log_operation(
        tid,
        (
            f"request method={request.method} "
            f"path={request.path} "
            f"remote={request.remote_addr}"
        ),
    )

    conn = pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM ngos ORDER BY id DESC")
            items = cur.fetchall()

            log_operation(
                tid,
                (
                    f"ngos.list count={len(items)} "
                    f"duration_ms={(time.time() - start) * 1000:.0f}"
                ),
            )
            return jsonify(items), 200
    except Exception as e:
        log_operation(tid, f"Error listing NGOs: {e}")
        log.error("Erro ao buscar ONGs: %s", e)
        return jsonify({"error": "Erro interno"}), 500
    finally:
        pool.putconn(conn)


if __name__ == '__main__':
    port = int(os.getenv("PORT", 8081))
    app.run(host='0.0.0.0', port=port)
