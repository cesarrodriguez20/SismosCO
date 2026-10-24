import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from config import BASE, EVENTOS, FALLAS, METRICAS, PUERTO_WEB, VENTANAS, leer_json, leer_jsonl_desde, preparar_carpetas

INDEX = BASE / "web" / "index.html"


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def responder(self, cuerpo, tipo="application/json", codigo=200):
        data = cuerpo if isinstance(cuerpo, bytes) else json.dumps(cuerpo, ensure_ascii=False).encode()
        self.send_response(codigo)
        self.send_header("Content-Type", f"{tipo}; charset=utf-8"); self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path in ("/", "/index.html"):
            return self.responder(INDEX.read_bytes(), "text/html")
        if url.path == "/api/fallas":
            return self.responder(FALLAS.read_bytes() if FALLAS.exists() else b'{"type":"FeatureCollection","features":[]}', "application/geo+json")
        if url.path == "/api/estado":
            desde = int(parse_qs(url.query).get("e", ["-1"])[0])
            if desde < 0:
                desde = max(0, leer_jsonl_desde(EVENTOS, 0)[1] - 400)
            evs, total = leer_jsonl_desde(EVENTOS, desde)
            return self.responder({"eventos": evs, "e": total, "metricas": leer_json(METRICAS, {}), "ventanas": leer_json(VENTANAS, [])})
        self.responder({"error": "no encontrado"}, codigo=404)


if __name__ == "__main__":
    preparar_carpetas()
    print(f"\033[92m\033[1m$ python servidor_web.py\033[0m  ->  abre  \033[96mhttp://localhost:{PUERTO_WEB}\033[0m")
    ThreadingHTTPServer(("0.0.0.0", PUERTO_WEB), Handler).serve_forever()
