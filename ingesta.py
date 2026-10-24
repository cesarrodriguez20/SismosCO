import asyncio, json, math, os, queue, threading, time, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone

from config import (BBOX, COLOMBIA_GEOJSON, COLOMBIA_URL, FUENTES, HEADERS, HORAS_INICIAL, INTERVALO_S, INTERVALO_SGC_S, MAG_MIN, SGC_URL, SOLO_COLOMBIA, STREAM_DIR, TMP_DIR, VISTOS, WS_EMSC, Color as C,
                    escribir_json, leer_json, preparar_carpetas)

COLA, _ya, recientes = queue.Queue(), {}, []


def utc(s):
    return datetime.fromisoformat(s.replace("Z", "").split(".")[0])


def en_bbox(lat, lon):
    return BBOX["minlatitude"] <= lat <= BBOX["maxlatitude"] and BBOX["minlongitude"] <= lon <= BBOX["maxlongitude"]

_poligonos = None

def descargar_colombia():
    if COLOMBIA_GEOJSON.exists() and COLOMBIA_GEOJSON.stat().st_size > 1000:
        return True
    try:
        import urllib.request
        print("Descargando límites de Colombia (geoBoundaries)...")
        req = urllib.request.Request(COLOMBIA_URL, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=60) as r:
            COLOMBIA_GEOJSON.write_bytes(r.read())
        print(f"  OK límites guardados en {COLOMBIA_GEOJSON}")
        return True
    except Exception as e:
        print(f"  AVISO No se pudieron descargar los límites ({e}).")
        return False

def cargar_colombia():
    global _poligonos
    if _poligonos is not None:
        return _poligonos
    if not SOLO_COLOMBIA:
        _poligonos = []
        return _poligonos
    if not descargar_colombia():
        _poligonos = []
        return _poligonos
    from shapely.geometry import shape
    with open(COLOMBIA_GEOJSON, encoding="utf-8") as f:
        g = json.load(f)
    _poligonos = [shape(x["geometry"]) for x in g.get("features", [])]
    print(f"  OK filtro Colombia activo con {len(_poligonos)} polígono(s)")
    return _poligonos

def en_colombia(lat, lon):
    if not SOLO_COLOMBIA:
        return True
    poligonos = cargar_colombia()
    if not poligonos:
        return False
    from shapely.geometry import Point
    p = Point(lon, lat)
    return any(pol.contains(p) or pol.touches(p) for pol in poligonos)

def parse_sgc(txt):
    out = []
    try:
        g = json.loads(txt)
    except ValueError:
        return out
    for f in g.get("features", []):
        p = f.get("properties", {})
        try:
            if str(p.get("agency", "")).upper() != "SGC":
                continue
            lat = p.get("latitude")
            lon = p.get("longitude")
            if lat is None or lon is None:
                coords = f.get("geometry", {}).get("coordinates", [])
                if len(coords) >= 2:
                    lat, lon = float(coords[0]), float(coords[1])
                else:
                    continue
            lat, lon = float(lat), float(lon)
            if not en_colombia(lat, lon):
                continue
            ts = p.get("utcTime") or p.get("timeUtc") or p.get("time")
            mag = p.get("mag", p.get("magnitude"))
            if ts is None or mag is None:
                continue
            out.append({"fuente": "SGC", "id": "SGC-" + str(p.get("id") or f.get("id")),
                        "ts": utc(str(ts)).isoformat(timespec="seconds"), "lat": lat, "lon": lon,
                        "prof": abs(float(p.get("depthKm", p.get("depth", 0)) or 0)),
                        "mag": float(mag), "tipo_mag": str(p.get("magType", p.get("magnitudeType", ""))),
                        "lugar": str(p.get("place", p.get("closer_towns", "Colombia"))),
                        "estado": str(p.get("status", p.get("reviewStatus", "")))})
        except (KeyError, ValueError, TypeError):
            continue
    return out

def consultar_sgc():
    req = urllib.request.Request(SGC_URL, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=25) as r:
        return parse_sgc(r.read().decode("utf-8", "replace"))


    return BBOX["minlatitude"] <= lat <= BBOX["maxlatitude"] and BBOX["minlongitude"] <= lon <= BBOX["maxlongitude"]


def km(a, b):
    p = math.pi / 180
    h = math.sin((b["lat"] - a["lat"]) * p / 2) ** 2 + math.cos(a["lat"] * p) * math.cos(b["lat"] * p) * math.sin((b["lon"] - a["lon"]) * p / 2) ** 2
    return 12742 * math.asin(math.sqrt(h))


def parse_texto(txt, fuente):
    out = []
    for l in txt.splitlines():
        p = l.split("|")
        if l.startswith("#") or len(p) < 13:
            continue
        try:
            lat, lon = float(p[2]), float(p[3])
            if not en_colombia(lat, lon):
                continue
            out.append({"fuente": fuente, "id": p[0].strip(), "ts": utc(p[1]).isoformat(timespec="seconds"), "lat": lat, "lon": lon,
                        "prof": float(p[4] or 0), "mag": float(p[10]), "tipo_mag": p[9], "lugar": p[12].strip()})
        except ValueError:
            continue
    return out


def parse_json(txt, fuente):
    out = []
    for f in json.loads(txt).get("features", []):
        p = f.get("properties", {})
        try:
            lat, lon = float(p["lat"]), float(p["lon"])
            if not en_colombia(lat, lon):
                continue
            out.append({"fuente": fuente, "id": str(p.get("unid") or f.get("id")), "ts": utc(p["time"]).isoformat(timespec="seconds"), "lat": lat,
                        "lon": lon, "prof": abs(float(p.get("depth") or 0)), "mag": float(p["mag"]), "tipo_mag": p.get("magtype", ""), "lugar": p.get("flynn_region", "")})
        except (KeyError, ValueError, TypeError):
            continue
    return out


def consultar(fuente, url, desde):
    error = None
    for fmt in (["json", "text"] if fuente == "EMSC" else ["text"]):
        q = {"format": fmt, "orderby": "time-asc", "minmagnitude": MAG_MIN, "starttime": desde.strftime("%Y-%m-%dT%H:%M:%S"), **BBOX}
        req = urllib.request.Request(url + "?" + urllib.parse.urlencode(q), headers=HEADERS)
        if (fuente, fmt) not in _ya:
            print(f"{C.D}  GET {req.full_url}{C.X}"); _ya[(fuente, fmt)] = True
        try:
            with urllib.request.urlopen(req, timeout=25) as r:
                txt = r.read().decode("utf-8", "replace")
            evs = parse_json(txt, fuente) if fmt == "json" else parse_texto(txt, fuente)
            if evs:
                return evs
            error = None
        except Exception as ex:
            error = ex
    if error:
        raise error
    return []


def hora_col(ts):
    return (utc(ts) - timedelta(hours=5)).strftime("%d/%m %H:%M")


def desde_ws(m):
    if m.get("action") == "update":
        return None
    d = m.get("data", m); p = d.get("properties", {})
    try:
        lat, lon, mag = float(p["lat"]), float(p["lon"]), float(p["mag"])
        if mag < MAG_MIN or not en_colombia(lat, lon):
            return None
        return {"fuente": "EMSC", "id": str(p.get("unid") or d.get("id")), "ts": utc(p["time"]).isoformat(timespec="seconds"), "lat": lat, "lon": lon,
                "prof": abs(float(p.get("depth") or 0)), "mag": mag, "tipo_mag": p.get("magtype", ""), "lugar": p.get("flynn_region", ""), "ws": True}
    except (KeyError, ValueError, TypeError):
        return None


def hilo_ws():
    try:
        import websockets
    except ImportError:
        print(f"{C.Y}AVISO Sin websocket de EMSC: ejecuta  pip install websockets  (se sigue con consultas cada {INTERVALO_S} s){C.X}")
        return

    async def run():
        while True:
            try:
                async with websockets.connect(WS_EMSC, ping_interval=20) as ws:
                    print(f"{C.G}OK EMSC websocket conectado (aviso inmediato){C.X}")
                    async for msg in ws:
                        try:
                            COLA.put(json.loads(msg))
                        except ValueError:
                            pass
            except Exception as ex:
                print(f"{C.Y}AVISO EMSC websocket caído ({type(ex).__name__}); reintento en 15 s{C.X}")
                await asyncio.sleep(15)
    asyncio.run(run())


def duplicado(e, vistos):
    if (e["fuente"], e["id"]) in vistos:
        return True
    t = utc(e["ts"])
    return any(o["fuente"] == e["fuente"] and abs((utc(o["ts"]) - t).total_seconds()) <= 120 and km(o, e) <= 60 for o in recientes)


def main():
    preparar_carpetas()
    estado = leer_json(VISTOS, {"vistos": [], "offset": 0, "n_archivo": 0})
    vistos, offset, n_archivo = set(map(tuple, estado["vistos"])) if estado["vistos"] and isinstance(estado["vistos"][0], list) else set(), estado["offset"], estado["n_archivo"]
    desde = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=HORAS_INICIAL)
    print(f"{C.G}{C.BOLD}$ python ingesta.py{C.X}  {C.D}fuentes: {', '.join(FUENTES)} + EMSC websocket · consulta cada {INTERVALO_S}s · M>={MAG_MIN}{C.X}")
    threading.Thread(target=hilo_ws, daemon=True).start()
    primera, ultimo, ultimo_sgc = True, 0.0, 0.0
    while True:
        nuevos = []
        if time.time() - ultimo >= INTERVALO_S:
            ultimo = time.time()
            for f, url in FUENTES.items():
                try:
                    todos = consultar(f, url, desde)
                    for e in todos:
                        e["en_vivo"] = not primera
                    nuevos += todos
                    ult = max(todos, key=lambda x: x["ts"]) if todos else None
                    print(f"{C.D}[{time.strftime('%H:%M:%S')}] {f}: {len(todos)} recibidos"
                          + (f" | más reciente {hora_col(ult['ts'])} (hora Colombia) M{ult['mag']:.1f} {ult['lugar']}" if ult else "") + C.X)
                except Exception as ex:
                    print(f"{C.Y}[{time.strftime('%H:%M:%S')}] AVISO {f} no respondió ({type(ex).__name__}: {ex}){C.X}")
            try:
                if time.time() - ultimo_sgc >= INTERVALO_SGC_S:
                    ultimo_sgc = time.time()
                    sgc = consultar_sgc()
                    for e in sgc:
                        e["en_vivo"] = not primera
                    nuevos += sgc
                    print(f"{C.D}[{time.strftime('%H:%M:%S')}] SGC: {len(sgc)} sismos de Colombia (últimos 5 días){C.X}")
            except Exception as ex:
                print(f"{C.Y}[{time.strftime('%H:%M:%S')}] AVISO SGC no respondió ({type(ex).__name__}: {ex}){C.X}")
            desde, primera = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=24), False
        while not COLA.empty():
            e = desde_ws(COLA.get())
            if e:
                e["en_vivo"] = True; nuevos.append(e)
        lote = []
        for e in sorted(nuevos, key=lambda x: x["ts"]):
            if duplicado(e, vistos):
                continue
            vistos.add((e["fuente"], e["id"])); recientes.append(e); del recientes[:-400]
            lote.append(e)
        if lote:
            ahora = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")
            for e in lote:
                e["recibido"], e["offset"] = ahora, offset; offset += 1
                e.pop("ws", None)
            n_archivo += 1
            nombre = f"sismo_{n_archivo:06d}.json"
            tmp = TMP_DIR / nombre
            tmp.write_text("\n".join(json.dumps(e, ensure_ascii=False) for e in lote), encoding="utf-8")
            os.replace(tmp, STREAM_DIR / nombre)
            escribir_json(VISTOS, {"vistos": sorted(vistos)[-5000:], "offset": offset, "n_archivo": n_archivo})
            for e in lote[-5:]:
                print(f"  {C.B}->{C.X} {e['fuente']:4} M{e['mag']:.1f} {e['prof']:.0f} km {e['lugar']}")
        time.sleep(2)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{C.Y}Ingesta detenida.{C.X}")
