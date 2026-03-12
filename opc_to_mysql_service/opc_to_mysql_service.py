#!/usr/bin/env python3
"""
opc_to_mysql_service.py

Servicio base (sin API) que:
- Carga tags desde tags.json (key -> nodeid string, ej: "OPC-DATA.of": "ns=4;i=24")
- Lee todos los tags por OPC UA (Security=None)
- Guarda las lecturas en MySQL

Dependencias:
  pip install asyncua mysql-connector-python dotenv

Ejecución:
  python opc_to_mysql_service.py

Config por variables de entorno (recomendado):
  OPC_ENDPOINT=opc.tcp://192.168.0.89:4840
  TAGS_JSON=tags.json
  POLL_SECONDS=2

  MYSQL_HOST=127.0.0.1
  MYSQL_PORT=3306
  MYSQL_DB=mi_db
  MYSQL_USER=mi_user
  MYSQL_PASS=mi_pass
  """

from __future__ import annotations

import os
import json
import time
import signal
import logging
import datetime as dt
from decimal import Decimal, ROUND_HALF_UP
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import mysql.connector
from mysql.connector import pooling
from asyncua import Client

print(">>> script arrancó")
# -------------------------
# Config
# -------------------------

OPC_ENDPOINT = os.getenv("OPC_ENDPOINT", "opc.tcp://192.168.1.10:4840")
TAGS_JSON = Path(os.getenv("TAGS_JSON", "tags.json"))
POLL_SECONDS = float(os.getenv("POLL_SECONDS", "2"))

MYSQL_HOST = os.getenv("MYSQL_HOST", "192.168.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB = os.getenv("MYSQL_DB", "M594")
MYSQL_USER = os.getenv("MYSQL_USER", "m594")
MYSQL_PASS = os.getenv("MYSQL_PASS", "594")

MYSQL_TABLE = os.getenv("MYSQL_TABLE", "paradaspreensamble")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("opc_to_mysql")


# -------------------------
# Mapping OPC -> columnas MySQL
# -------------------------
# Ajustá las keys para que coincidan con tu tags.json (las de la izquierda)
# Por ejemplo si tu tags.json trae "OPC-DATA.pt.curva" y eso debe ir a "Producto", lo mapeás acá.
TAG_TO_COL = {
    "OPC-DATA.Producto": "Producto",
    "OPC-DATA.tipo": "tipo",
    "OPC-DATA.CicloReal": "CicloReal",           # si viene float
    "OPC-DATA.CicloReal_x100": "CicloReal",      # si viene escalado x100
    "OPC-DATA.PiezasTotales": "PiezasTotales",
    "OPC-DATA.PiezasGood": "PiezasGood",
    "OPC-DATA.PiezasBad": "PiezasBad",
    "OPC-DATA.A": "A",
    "OPC-DATA.P": "P",
    "OPC-DATA.Q": "Q",
    "OPC-DATA.OEE": "OEE",
    "OPC-DATA.FPY": "FPY",
    "OPC-DATA.Tyeld": "Tyeld",
    "OPC-DATA.T_Producido": "T_Producido",
    "OPC-DATA.T_Parado_Fallas": "T_Parado_Fallas",
    "OPC-DATA.T_Parado_mat": "T_Parado_mat",
    "OPC-DATA.T_Disponible": "T_Disponible",
    "OPC-DATA.Estado": "Estado",
    "OPC-DATA.turno": "turno",
    # Fecha la setea el servicio (hoy). Si la querés del PLC, agregá tag y mapeala a "Fecha".
}


# -------------------------
# Tipos / utilidades
# -------------------------

@dataclass(frozen=True)
class TagDef:
    key: str
    nodeid: str


def load_tags_from_json(path: Path) -> Dict[str, TagDef]:
    if not path.exists():
        raise FileNotFoundError(f"No existe {path}.")
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not data:
        raise ValueError("tags.json inválido o vacío (se espera dict key->nodeid).")
    return {k: TagDef(k, v) for k, v in data.items() if isinstance(k, str) and isinstance(v, str)}


def to_int_or_none(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, (int,)):
        return int(v)
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str) and v.strip() != "":
        try:
            return int(float(v))
        except Exception:
            return None
    return None


def to_usint(v: Any, default: int = 0) -> int:
    n = to_int_or_none(v)
    if n is None:
        return default
    if n < 0:
        return 0
    if n > 255:
        return 255
    return n


def to_decimal_10_2_from_value(v: Any) -> Decimal:
    """
    Devuelve Decimal(10,2) para MySQL.
    - Si v es int grande tipo x100 (ej 1234 => 12.34) lo detectamos por la key (ver build_row)
    - Si v es float: se redondea a 2 decimales.
    """
    if v is None:
        # MySQL CicloReal es NOT NULL; devolvemos 0.00 si no vino.
        return Decimal("0.00")

    if isinstance(v, (int,)):
        # interpretamos como 0.00.. pero normalmente se usa x100, manejado en build_row.
        return Decimal(v).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)

    if isinstance(v, float):
        return Decimal(str(v)).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)

    if isinstance(v, str):
        try:
            return Decimal(v).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)
        except Exception:
            return Decimal("0.00")

    return Decimal(str(v)).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)


# -------------------------
# MySQL
# -------------------------

def make_mysql_pool() -> pooling.MySQLConnectionPool:
    return pooling.MySQLConnectionPool(
        pool_name="opc_pool",
        pool_size=5,
        pool_reset_session=True,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB,
        autocommit=False,
    )


def ensure_table(pool: pooling.MySQLConnectionPool) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{MYSQL_TABLE}` (
      `Id_func` int(11) NOT NULL AUTO_INCREMENT,
      `Fecha` date NOT NULL,
      `Producto` int(11) DEFAULT NULL,
      `tipo` tinyint(11) NOT NULL,
      `CicloReal` decimal(10,2) NOT NULL,
      `PiezasTotales` int(11) DEFAULT NULL,
      `PiezasGood` int(11) DEFAULT NULL,
      `PiezasBad` int(11) DEFAULT NULL,
      `A` int(11) DEFAULT NULL,
      `P` int(11) DEFAULT NULL,
      `Q` int(11) DEFAULT NULL,
      `OEE` int(11) DEFAULT NULL,
      `FPY` int(11) DEFAULT NULL,
      `Tyeld` int(11) DEFAULT NULL,
      `T_Producido` int(11) DEFAULT NULL,
      `T_Parado_Fallas` int(11) DEFAULT NULL,
      `T_Parado_mat` int(11) DEFAULT NULL,
      `T_Disponible` int(11) DEFAULT NULL,
      `Estado` int(11) DEFAULT NULL,
      `turno` tinyint(4) NOT NULL,
      PRIMARY KEY (`Id_func`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    cn = pool.get_connection()
    try:
        cur = cn.cursor()
        cur.execute(ddl)
        cn.commit()
    finally:
        cn.close()


def insert_row(pool: pooling.MySQLConnectionPool, row: Dict[str, Any]) -> None:
    cols = [
        "Fecha", "Producto", "tipo", "CicloReal",
        "PiezasTotales", "PiezasGood", "PiezasBad",
        "A", "P", "Q", "OEE", "FPY", "Tyeld",
        "T_Producido", "T_Parado_Fallas", "T_Parado_mat", "T_Disponible",
        "Estado", "turno",
    ]

    sql = f"""
    INSERT INTO `{MYSQL_TABLE}` ({",".join("`"+c+"`" for c in cols)})
    VALUES ({",".join(["%s"] * len(cols))})
    """

    values = [row.get(c) for c in cols]

    cn = pool.get_connection()
    try:
        cur = cn.cursor()
        cur.execute(sql, values)
        cn.commit()
    except Exception:
        cn.rollback()
        raise
    finally:
        cn.close()


# -------------------------
# OPC UA
# -------------------------

async def read_all_tags(endpoint: str, tags: Dict[str, TagDef]) -> Dict[str, Any]:
    """
    Lee todos los tags en una sola sesión.
    Devuelve: { tag_key: value }
    """
    out: Dict[str, Any] = {}
    async with Client(url=endpoint) as client:
        for key, td in tags.items():
            node = client.get_node(td.nodeid)
            out[key] = await node.read_value()
    return out


def build_row_from_values(values: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convierte valores OPC (según TAG_TO_COL) a una fila lista para MySQL.
    Fecha se setea con la fecha actual por defecto.
    """
    row: Dict[str, Any] = {}

    # Fecha NOT NULL
    row["Fecha"] = dt.date.today()

    # Defaults NOT NULL
    row["tipo"] = 0
    row["turno"] = 0
    row["CicloReal"] = Decimal("0.00")

    # Mapear tags -> columnas
    for tag_key, val in values.items():
        col = TAG_TO_COL.get(tag_key)
        if not col:
            continue

        if col == "CicloReal":
            # Si el tag es *_x100 lo convertimos /100 exacto
            if tag_key.endswith("_x100"):
                n = to_int_or_none(val)
                if n is None:
                    row["CicloReal"] = Decimal("0.00")
                else:
                    row["CicloReal"] = (Decimal(n) / Decimal(100)).quantize(Decimal("0.00"), rounding=ROUND_HALF_UP)
            else:
                row["CicloReal"] = to_decimal_10_2_from_value(val)

        elif col in ("tipo", "turno"):
            row[col] = to_usint(val, default=0)

        elif col == "Fecha":
            # Si algún día decidís leer Fecha del PLC: aceptar DATE o string 'YYYY-MM-DD'
            if isinstance(val, dt.date):
                row["Fecha"] = val
            elif isinstance(val, str):
                try:
                    row["Fecha"] = dt.date.fromisoformat(val[:10])
                except Exception:
                    pass

        else:
            # int(11) DEFAULT NULL
            row[col] = to_int_or_none(val)

    return row


# -------------------------
# Servicio / loop
# -------------------------

_stop = False

def _handle_stop(sig, frame):
    global _stop
    _stop = True
    log.info("Señal de parada recibida (%s). Cerrando...", sig)

def install_signal_handlers():
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(s, _handle_stop)
        except Exception:
            pass


async def read_all_and_store_once(pool: pooling.MySQLConnectionPool, tags: Dict[str, TagDef]) -> Dict[str, Any]:
    """
    Lee todos los tags y guarda 1 fila en proceso_box2.
    Devuelve la fila insertada (para log/debug).
    """
    values = await read_all_tags(OPC_ENDPOINT, tags)
    row = build_row_from_values(values)
    insert_row(pool, row)
    return row


async def run_service():
    log.info("OPC endpoint: %s", OPC_ENDPOINT)
    log.info("Tags JSON: %s", TAGS_JSON.resolve())
    log.info("MySQL: %s:%s db=%s table=%s", MYSQL_HOST, MYSQL_PORT, MYSQL_DB, MYSQL_TABLE)
    log.info("Polling: %ss", POLL_SECONDS)

    tags = load_tags_from_json(TAGS_JSON)
    log.info("Tags cargados: %d", len(tags))

    pool = make_mysql_pool()
    ensure_table(pool)

    while not _stop:
        t0 = time.time()
        try:
            row = await read_all_and_store_once(pool, tags)
            log.info("INSERT OK | Fecha=%s tipo=%s turno=%s CicloReal=%s",
                     row["Fecha"], row["tipo"], row["turno"], row["CicloReal"])
        except Exception as e:
            log.exception("Ciclo FALLÓ: %s: %s", type(e).__name__, e)

        dt_elapsed = time.time() - t0
        sleep_s = max(0.0, POLL_SECONDS - dt_elapsed)
        if sleep_s > 0:
            time.sleep(sleep_s)

    log.info("Servicio detenido.")


if __name__ == "__main__":
    import asyncio
    install_signal_handlers()
    asyncio.run(run_service())