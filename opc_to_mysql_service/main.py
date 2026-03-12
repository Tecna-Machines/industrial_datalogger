import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from asyncua import Client
import asyncio

OPC_ENDPOINT = os.getenv("OPC_ENDPOINT", "opc.tcp://192.168.0.89:4840")
OPC_USER = os.getenv("OPC_USER", "")
OPC_PASS = os.getenv("OPC_PASS", "")

# Mapa simple nombre -> NodeId (ajustá estos NodeIds a los tuyos)
TAGS = {
	"of": 'ns=3;i=24',
	#"pt": 'ns=3;s="OPC-DATA"."pt"',
}

app = FastAPI(title="PLC OPC UA REST (On-demand)")

class ReadRequest(BaseModel):
    tags: list[str]

async def opc_read(nodeid: str):
    # Security=None => no certificados, conexión directa
    async with Client(url=OPC_ENDPOINT) as client:
        node = client.get_node(nodeid)
        return await node.read_value()

@app.get("/health")
def health():
    return {"ok": True, "endpoint": OPC_ENDPOINT}

@app.get("/tags")
def list_tags():
    return {"tags": list(TAGS.keys())}

@app.get("/tags/{name}")
async def read_tag(name: str):
    nodeid = TAGS.get(name)
    if not nodeid:
        raise HTTPException(status_code=404, detail="Unknown tag")

    try:
        val = await opc_read(nodeid)
        return {"name": name, "value": val}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OPC read failed: {type(e).__name__}: {e}")

@app.post("/read")
async def read_many(req: ReadRequest):
    unknown = [t for t in req.tags if t not in TAGS]
    if unknown:
        raise HTTPException(status_code=400, detail={"unknown_tags": unknown})

    try:
        # lecturas en paralelo
        tasks = [opc_read(TAGS[t]) for t in req.tags]
        values = await asyncio.gather(*tasks)
        return {t: v for t, v in zip(req.tags, values)}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"OPC read failed: {type(e).__name__}: {e}")
