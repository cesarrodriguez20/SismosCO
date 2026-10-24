import json, os, re, sys, time, unicodedata
from pathlib import Path

BASE = Path(__file__).resolve().parent


if os.name == "nt":
    HADOOP_HOME = BASE / "hadoop"
    os.environ["HADOOP_HOME"] = str(HADOOP_HOME)
    os.environ["PATH"] = str(HADOOP_HOME / "bin") + os.pathsep + os.environ.get("PATH", "")
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass


WINUTILS_URLS = [
    "https://github.com/kontext-tech/winutils/raw/master/hadoop-3.4.0-win10-x64/bin/{f}",
    "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/{f}",
    "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/{f}",
]


def _binario_ok(p: Path) -> bool:
    try:
        return p.stat().st_size > 20_000 and p.read_bytes()[:2] == b"MZ"
    except OSError:
        return False


def asegurar_winutils():
    if os.name != "nt":
        return True
    import urllib.request
    carpeta = BASE / "hadoop" / "bin"
    carpeta.mkdir(parents=True, exist_ok=True)
    for f in ("winutils.exe", "hadoop.dll"):
        destino = carpeta / f
        if _binario_ok(destino):
            continue
        print(f"Descargando {f} (necesario para Spark en Windows)...", flush=True)
        for url in WINUTILS_URLS:
            try:
                urllib.request.urlretrieve(url.format(f=f), destino)
                if _binario_ok(destino):
                    print(f"  OK {f} listo ({url.split('/raw/')[0]})", flush=True)
                    break
            except Exception:
                pass
        if not _binario_ok(destino):
            print(f"\nERROR No se pudo descargar {f}. Descárgalo a mano desde\n"
                  f"  https://github.com/kontext-tech/winutils (carpeta hadoop-3.4.0-win10-x64/bin)\n"
                  f"  y cópialo en: {carpeta}\n")
            return False
    return True


def ruta_spark(p) -> str:
    return str(p).replace("\\", "/")
DATA = BASE / "data"
STREAM_DIR, TMP_DIR, CHECKPOINTS, SALIDA = DATA / "stream", DATA / "tmp", DATA / "checkpoints", DATA / "salida"
EVENTOS, METRICAS, VENTANAS = DATA / "eventos.jsonl", DATA / "metricas.json", DATA / "ventanas.json"
VISTOS, FALLAS = DATA / "vistos.json", DATA / "fallas.geojson"

TRIGGER, MAX_FILES_POR_LOTE, PUERTO_WEB = "2 seconds", 10, 8000
INTERVALO_S = 30
HORAS_INICIAL = 120
MAG_MIN = 2.0
BBOX = dict(minlatitude=-4.3, maxlatitude=13.0, minlongitude=-79.1, maxlongitude=-66.9)
COLOMBIA_GEOJSON = DATA / "colombia.geojson"
COLOMBIA_URL = "https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/gbOpen/COL/ADM0/geoBoundaries-COL-ADM0_simplified.geojson"
SOLO_COLOMBIA = True
SGC_URL = os.getenv("SGC_URL", "https://archive.sgc.gov.co/feed/v1.0.1/summary/five_days_all.json")
INTERVALO_SGC_S = 60
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)", "Accept": "application/json,text/plain,*/*"}

FUENTES = {
    "USGS": os.getenv("USGS_URL", "https://earthquake.usgs.gov/fdsnws/event/1/query"),
    "EMSC": os.getenv("EMSC_URL", "https://www.seismicportal.eu/fdsnws/event/1/query"),
}
WS_EMSC = os.getenv("WS_EMSC", "wss://www.seismicportal.eu/standing_order/websocket")
FALLAS_URL = ("https://raw.githubusercontent.com/GEMScienceTools/gem-global-active-faults/master/"
              "geojson/gem_active_faults_harmonized.geojson")

ZONAS = {
    "Nido de Bucaramanga": (6.4, 7.2, -73.4, -72.7),
    "Subducción Pacífico/Chocó": (2.0, 8.5, -79.5, -76.8),
    "Tolima / Eje Cafetero": (3.3, 5.6, -76.3, -74.5),
    "Nariño / Cauca": (0.5, 3.0, -78.8, -76.3),
    "Piedemonte Llanero": (3.0, 6.5, -74.5, -72.0),
}
DEPTH_SUP, DEPTH_INT = 33, 70


def preparar_carpetas():
    for d in (DATA, STREAM_DIR, TMP_DIR, CHECKPOINTS, SALIDA):
        d.mkdir(parents=True, exist_ok=True)


def leer_json(ruta, defecto):
    try:
        with open(ruta, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return defecto


def escribir_json(ruta, obj):
    tmp = TMP_DIR / f"{Path(ruta).name}.{os.getpid()}.{time.time_ns()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
    for _ in range(50):            
        try:
            os.replace(tmp, ruta)
            return
        except PermissionError:
            time.sleep(0.02)
    try:
        os.remove(tmp)
    except OSError:
        pass


def agregar_jsonl(ruta, objs):
    with open(ruta, "a", encoding="utf-8") as f:
        for o in objs:
            f.write(json.dumps(o, ensure_ascii=False) + "\n")


def leer_jsonl_desde(ruta, desde):
    try:
        with open(ruta, encoding="utf-8") as f:
            lineas = f.readlines()
    except OSError:
        return [], 0
    out = []
    for l in lineas[desde:]:
        try:
            out.append(json.loads(l))
        except ValueError:
            pass
    return out, len(lineas)


class Color:
    R, G, Y, B, M, C, D, X, BOLD = ("\033[91m", "\033[92m", "\033[93m", "\033[94m", "\033[95m", "\033[96m",
                                    "\033[90m", "\033[0m", "\033[1m")
    ALERTA = "\033[41m\033[97m\033[1m"
    LOTE = "\033[44m\033[97m\033[1m"


os.system("")
