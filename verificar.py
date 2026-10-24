import os, re, shutil, socket, subprocess, sys

from config import BASE, PUERTO_WEB, asegurar_winutils

OK, MAL = "\033[92mOK\033[0m", "\033[91mERROR\033[0m"
todo_bien = True


def check(nombre, cond, ayuda=""):
    global todo_bien
    print(f" {OK if cond else MAL} {nombre}")
    if not cond:
        todo_bien = False
        if ayuda:
            print(f"     -> {ayuda}")


print("\n\033[1mVerificando LiveMod...\033[0m\n")

v = sys.version_info
check(f"Python {v.major}.{v.minor}.{v.micro}", v >= (3, 9), "Instala Python 3.10 o superior desde python.org")

java = shutil.which("java")
ver_java = 0
if java:
    out = subprocess.run([java, "-version"], capture_output=True, text=True).stderr
    m = re.search(r'version "(\d+)', out)
    ver_java = int(m.group(1)) if m else 0
check(f"Java {ver_java or 'no encontrado'}", ver_java >= 17,
      "Instala Java 17 o 21 (Adoptium Temurin) y reinicia VS Code para que tome el PATH")

try:
    import pyspark
    check(f"PySpark {pyspark.__version__}", int(pyspark.__version__.split('.')[0]) >= 4 or ver_java < 21,
          "Con Java 21 usa PySpark 4:  pip install -r requirements.txt")
except ImportError:
    check("PySpark", False, "pip install -r requirements.txt")

if os.name == "nt":
    check("winutils.exe y hadoop.dll (Hadoop para Windows)", asegurar_winutils(),
          "Revisa tu conexión a internet o cópialos a mano en la carpeta hadoop\\bin")
    check("Ruta del proyecto sin espacios ni OneDrive", " " not in str(BASE) and "onedrive" not in str(BASE).lower(),
          f"Recomendado mover la carpeta a C:\\livemod-spark (ahora está en {BASE})")

s = socket.socket()
libre = s.connect_ex(("127.0.0.1", PUERTO_WEB)) != 0
s.close()
check(f"Puerto {PUERTO_WEB} libre para la web", libre, "Cierra lo que use ese puerto o cambia PUERTO_WEB en config.py")

print()
if todo_bien:
    print("\033[92m\033[1mTodo listo.\033[0m Abre 3 terminales en VS Code y ejecuta en este orden:")
    print("   1)  python monitor_spark.py")
    print("   2)  python servidor_web.py")
    print("   3)  python ingesta.py")
    print(f"   y abre  http://localhost:{PUERTO_WEB}\n")
else:
    print("\033[93mCorrige lo marcado con ERROR y vuelve a ejecutar:  python verificar.py\033[0m\n")
