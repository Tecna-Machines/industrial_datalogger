"""
ejecución: uvicorn main02:app --host 0.0.0.0 --port 8000
"""
import asyncio
import json
from pathlib import Path
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from asyncua import Client

ENDPOINT = "opc.tcp://192.168.0.89:4840"
TAGS_FILE = Path("tags.json")

app = FastAPI(title="S7-1200 OPC UA REST (on-demand)")

TAGS: dict[str, str] = {}  # key -> nodeid string


class ReadRequest(BaseModel):
    tags: list[str]


def load_tags():
    global TAGS
    if not TAGS_FILE.exists():
        raise RuntimeError(f"No existe {TAGS_FILE}")
    new_tags = json.loads(TAGS_FILE.read_text(encoding="utf-8"))

    if not isinstance(new_tags, dict):
        raise RuntimeError("Formato inválido en tags.json")

    TAGS = new_tags

async def read_node(nodeid: str):
    async with Client(url=ENDPOINT) as client:
        node = client.get_node(nodeid)
        return await node.read_value()


@app.on_event("startup")
async def startup():
    load_tags()


@app.get("/tags")
def list_tags():
    return {"count": len(TAGS), "tags": sorted(TAGS.keys())}


@app.get("/read/{tag_key}")
async def read_one(tag_key: str):
    nodeid = TAGS.get(tag_key)
    if not nodeid:
        raise HTTPException(404, detail=f"Tag desconocido: {tag_key}")
    try:
        val = await read_node(nodeid)
        return {"tag": tag_key, "nodeid": nodeid, "value": val}
    except Exception as e:
        raise HTTPException(502, detail=f"OPC read failed: {type(e).__name__}: {e}")


@app.post("/read")
async def read_many(req: ReadRequest):
    unknown = [t for t in req.tags if t not in TAGS]
    if unknown:
        raise HTTPException(400, detail={"unknown_tags": unknown})

    try:
        tasks = [read_node(TAGS[t]) for t in req.tags]
        values = await asyncio.gather(*tasks)
        return {t: v for t, v in zip(req.tags, values)}
    except Exception as e:
        raise HTTPException(502, detail=f"OPC read failed: {type(e).__name__}: {e}")


@app.post("/reload-tags")
async def reload_tags():
    try:
        load_tags()
        return {
            "status": "ok",
            "count": len(TAGS),
            "message": "tags.json recargado correctamente"
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error recargando tags: {type(e).__name__}: {e}"
        )
