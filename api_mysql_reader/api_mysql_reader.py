#!/usr/bin/env python3
"""
api_mysql_reader.py

API (solo lectura) para consultar los datos guardados en MySQL (tabla proceso_box2).

Dependencias:
  pip install fastapi uvicorn mysql-connector-python dotenv

Ejecutar:
  uvicorn api_mysql_reader:app --host 0.0.0.0 --port 8080

Config por entorno: 
  MYSQL_HOST, MYSQL_PORT, MYSQL_DB, MYSQL_USER, MYSQL_PASS, MYSQL_TABLE
"""

from __future__ import annotations

import os
import datetime as dt
from typing import Optional, List, Dict, Any

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from fastapi import FastAPI, HTTPException, Query
from mysql.connector import pooling

MYSQL_HOST = os.getenv("MYSQL_HOST", "192.168.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_DB = os.getenv("MYSQL_DB", "m594")
MYSQL_USER = os.getenv("MYSQL_USER", "m594")
MYSQL_PASS = os.getenv("MYSQL_PASS", "594")
MYSQL_TABLE = os.getenv("MYSQL_TABLE", "paradaspreensamble")

app = FastAPI(title="Proceso Pre Ensamble - MySQL Read API")

pool: pooling.MySQLConnectionPool | None = None


def _make_pool() -> pooling.MySQLConnectionPool:
    return pooling.MySQLConnectionPool(
        pool_name="api_pool",
        pool_size=10,
        pool_reset_session=True,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB,
        autocommit=True,
    )


def _row_to_dict(cols: List[str], row: tuple) -> Dict[str, Any]:
    out = {}
    for c, v in zip(cols, row):
        if isinstance(v, (dt.date, dt.datetime)):
            out[c] = v.isoformat()
        else:
            out[c] = v
    return out


@app.on_event("startup")
def startup():
    global pool
    pool = _make_pool()


@app.get("/health")
def health():
    # Ping simple a MySQL
    assert pool is not None
    cn = pool.get_connection()
    try:
        cur = cn.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        return {
            "ok": True,
            "mysql": f"{MYSQL_HOST}:{MYSQL_PORT}",
            "db": MYSQL_DB,
            "table": MYSQL_TABLE,
        }
    finally:
        cn.close()


@app.get("/latest")
def latest(n: int = Query(10, ge=1, le=1000)):
    """
    Devuelve las últimas N filas por Id_func descendente.
    """
    assert pool is not None
    cn = pool.get_connection()
    try:
        cur = cn.cursor()
        cur.execute(
            f"""
            SELECT
              Id_func, Fecha, Producto, tipo, CicloReal,
              PiezasTotales, PiezasGood, PiezasBad,
              A, P, Q, OEE, FPY, Tyeld,
              T_Producido, T_Parado_Fallas, T_Parado_mat, T_Disponible,
              Estado, turno
            FROM `{MYSQL_TABLE}`
            ORDER BY Id_func DESC
            LIMIT %s
            """,
            (n,),
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return {
            "count": len(rows),
            "items": [_row_to_dict(cols, r) for r in rows],
        }
    finally:
        cn.close()


@app.get("/by-id/{id_func}")
def by_id(id_func: int):
    """
    Devuelve una fila por Id_func.
    """
    assert pool is not None
    cn = pool.get_connection()
    try:
        cur = cn.cursor()
        cur.execute(
            f"""
            SELECT
              Id_func, Fecha, Producto, tipo, CicloReal,
              PiezasTotales, PiezasGood, PiezasBad,
              A, P, Q, OEE, FPY, Tyeld,
              T_Producido, T_Parado_Fallas, T_Parado_mat, T_Disponible,
              Estado, turno
            FROM `{MYSQL_TABLE}`
            WHERE Id_func = %s
            """,
            (id_func,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, detail="Id_func no encontrado")
        cols = [d[0] for d in cur.description]
        return _row_to_dict(cols, row)
    finally:
        cn.close()


@app.get("/range")
def range_query(
    date_from: str = Query(..., description="YYYY-MM-DD"),
    date_to: str = Query(..., description="YYYY-MM-DD (inclusive)"),
    producto: Optional[int] = Query(None),
    turno: Optional[int] = Query(None, ge=0, le=255),
    tipo: Optional[int] = Query(None, ge=0, le=255),
    limit: int = Query(500, ge=1, le=5000),
    order: str = Query("asc", pattern="^(asc|desc)$"),
):
    """
    Filtra por rango de fechas (inclusive), y opcionalmente Producto/turno/tipo.
    """
    try:
        d1 = dt.date.fromisoformat(date_from)
        d2 = dt.date.fromisoformat(date_to)
    except ValueError:
        raise HTTPException(400, detail="date_from/date_to deben ser YYYY-MM-DD")

    if d2 < d1:
        raise HTTPException(400, detail="date_to debe ser >= date_from")

    where = ["Fecha >= %s", "Fecha <= %s"]
    params: list = [d1, d2]

    if producto is not None:
        where.append("Producto = %s")
        params.append(producto)
    if turno is not None:
        where.append("turno = %s")
        params.append(turno)
    if tipo is not None:
        where.append("tipo = %s")
        params.append(tipo)

    where_sql = " AND ".join(where)
    order_sql = "ASC" if order == "asc" else "DESC"

    assert pool is not None
    cn = pool.get_connection()
    try:
        cur = cn.cursor()
        cur.execute(
            f"""
            SELECT
              Id_func, Fecha, Producto, tipo, CicloReal,
              PiezasTotales, PiezasGood, PiezasBad,
              A, P, Q, OEE, FPY, Tyeld,
              T_Producido, T_Parado_Fallas, T_Parado_mat, T_Disponible,
              Estado, turno
            FROM `{MYSQL_TABLE}`
            WHERE {where_sql}
            ORDER BY Fecha {order_sql}, Id_func {order_sql}
            LIMIT %s
            """,
            (*params, limit),
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return {
            "count": len(rows),
            "items": [_row_to_dict(cols, r) for r in rows],
        }
    finally:
        cn.close()


@app.get("/stats/daily")
def stats_daily(
    date_from: str = Query(..., description="YYYY-MM-DD"),
    date_to: str = Query(..., description="YYYY-MM-DD (inclusive)"),
):
    """
    Resumen diario simple: conteo de registros + sumas de piezas.
    """
    try:
        d1 = dt.date.fromisoformat(date_from)
        d2 = dt.date.fromisoformat(date_to)
    except ValueError:
        raise HTTPException(400, detail="date_from/date_to deben ser YYYY-MM-DD")

    assert pool is not None
    cn = pool.get_connection()
    try:
        cur = cn.cursor()
        cur.execute(
            f"""
            SELECT
              Fecha,
              COUNT(*) AS registros,
              COALESCE(SUM(PiezasTotales),0) AS sum_total,
              COALESCE(SUM(PiezasGood),0) AS sum_good,
              COALESCE(SUM(PiezasBad),0) AS sum_bad
            FROM `{MYSQL_TABLE}`
            WHERE Fecha >= %s AND Fecha <= %s
            GROUP BY Fecha
            ORDER BY Fecha ASC
            """,
            (d1, d2),
        )
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return {
            "count": len(rows),
            "items": [_row_to_dict(cols, r) for r in rows],
        }
    finally:
        cn.close()