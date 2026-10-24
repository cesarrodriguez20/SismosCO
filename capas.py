import json, urllib.request

from config import BBOX, FALLAS, FALLAS_URL, preparar_carpetas


def fallas():
    print("Descargando fallas activas (GEM)...")
    g = json.loads(urllib.request.urlopen(FALLAS_URL, timeout=120).read())
    def pts(c):
        if c and isinstance(c[0], (int, float)): yield c
        else:
            for x in c: yield from pts(x)
    ok = [f for f in g["features"] if any(BBOX["minlongitude"] <= x <= BBOX["maxlongitude"] and BBOX["minlatitude"] <= y <= BBOX["maxlatitude"]
                                          for x, y, *_ in pts(f["geometry"]["coordinates"]))]
    for f in ok:
        p = f["properties"]; f["properties"] = {"name": p.get("name") or "sin nombre", "slip_type": p.get("slip_type") or "", "catalog": p.get("catalog_name") or ""}
    FALLAS.write_text(json.dumps({"type": "FeatureCollection", "features": ok}, separators=(",", ":")), encoding="utf-8")
    print(f"  OK {len(ok)} fallas en {FALLAS}")


if __name__ == "__main__":
    preparar_carpetas()
    try:
        fallas()
    except Exception as e:
        print(f"  ERROR No se pudo actualizar fallas ({e}); se conserva la copia incluida.")
