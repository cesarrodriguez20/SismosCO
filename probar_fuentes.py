import asyncio
import json
from datetime import datetime, timedelta, timezone

import ingesta as I
from config import FUENTES, WS_EMSC


def col(ts):
    return (I.utc(ts) - timedelta(hours=5)).strftime("%d/%m %H:%M")


desde = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=48)
for f, url in FUENTES.items():
    try:
        evs = I.consultar(f, url, desde)
        print(f"{f}: {len(evs)} sismos en 48 h")
        for e in sorted(evs, key=lambda x: x["ts"])[-5:]:
            print(f"   {col(e['ts'])} (hora Colombia)  M{e['mag']:.1f}  {e['prof']:.0f} km  {e['lugar']}")
    except Exception as ex:
        print(f"{f}: ERROR {type(ex).__name__}: {ex}")


async def ws():
    import websockets
    async with websockets.connect(WS_EMSC, ping_interval=20) as w:
        print("EMSC websocket: conectado, esperando un mensaje (hasta 60 s)")
        try:
            m = json.loads(await asyncio.wait_for(w.recv(), 60))
            p = m.get("data", {}).get("properties", {})
            print("   mensaje recibido:", m.get("action"), p.get("flynn_region"), p.get("mag"), p.get("time"))
        except asyncio.TimeoutError:
            print("   sin mensajes en 60 s (conexion abierta)")


try:
    asyncio.run(ws())
except Exception as ex:
    print(f"EMSC websocket: ERROR {type(ex).__name__}: {ex}")
