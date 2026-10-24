import math, time
from datetime import datetime
from functools import reduce

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType, TimestampType

from config import (CHECKPOINTS, DEPTH_INT, DEPTH_SUP, EVENTOS, MAX_FILES_POR_LOTE, METRICAS, SALIDA, STREAM_DIR, TRIGGER, VENTANAS,
                    ZONAS, Color as C, agregar_jsonl, asegurar_winutils, escribir_json, leer_json, preparar_carpetas, ruta_spark)

preparar_carpetas()
if not asegurar_winutils():
    raise SystemExit(1)
spark = (SparkSession.builder.appName("Sismicidad-Streaming").master("local[2]")
         .config("spark.driver.memory", "1g").config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.showConsoleProgress", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

esquema = StructType([StructField(k, t) for k, t in [("offset", LongType()), ("fuente", StringType()), ("id", StringType()),
    ("ts", TimestampType()), ("lat", DoubleType()), ("lon", DoubleType()), ("prof", DoubleType()), ("mag", DoubleType()),
    ("tipo_mag", StringType()), ("lugar", StringType()),
    ("recibido", StringType()), ("en_vivo", BooleanType())]])
sismos = spark.readStream.schema(esquema).option("maxFilesPerTrigger", MAX_FILES_POR_LOTE).json(ruta_spark(STREAM_DIR))

zona = reduce(lambda acc, kv: acc.when(F.col("lat").between(kv[1][0], kv[1][1]) & F.col("lon").between(kv[1][2], kv[1][3]), kv[0]),
              ZONAS.items(), F.when(F.lit(False), ""))
clasificado = (sismos
    .withColumn("nivel", F.when(F.col("mag") >= 5, "ROJO").when(F.col("mag") >= 4, "NARANJA").when(F.col("mag") >= 3, "AMARILLO").otherwise("VERDE"))
    .withColumn("tipo_prof", F.when(F.col("prof") < DEPTH_SUP, "superficial").when(F.col("prof") < DEPTH_INT, "intermedio").otherwise("profundo"))
    .withColumn("zona", zona.otherwise("Otra región")))

M = leer_json(METRICAS, {})
totales = M.get("totales", {"lotes": 0, "eventos": 0, "USGS": 0, "EMSC": 0, "confirmados": 0, "mag_max": 0, "ROJO": 0, "NARANJA": 0, "AMARILLO": 0, "VERDE": 0})
historial, recientes = M.get("historial", []), []


def km(a, b):
    p = math.pi / 180
    h = math.sin((b["lat"] - a["lat"]) * p / 2) ** 2 + math.cos(a["lat"] * p) * math.cos(b["lat"] * p) * math.sin((b["lon"] - a["lon"]) * p / 2) ** 2
    return 12742 * math.asin(math.sqrt(h))


def procesar_lote(df, batch_id):
    t0 = time.time()
    filas = [r.asDict() for r in df.orderBy("ts").collect()]
    if not filas:
        return
    (df.withColumn("lote", F.lit(batch_id)).write.mode("append").partitionBy("fuente").parquet(ruta_spark(SALIDA / "historial")))
    eventos = []
    for r in filas:
        r["ts"] = r["ts"].strftime("%Y-%m-%dT%H:%M:%S")
        par = next((o for o in recientes if o["fuente"] != r["fuente"] and abs((datetime.fromisoformat(o["ts"]) - datetime.fromisoformat(r["ts"])).total_seconds()) <= 90 and km(o, r) <= 100), None)
        r["confirmado"] = bool(par)
        r["dif_mag"] = round(r["mag"] - par["mag"], 2) if par else None
        recientes.append(r); del recientes[:-300]
        eventos.append({"tipo": "sismo", "lote": batch_id, **r})
        totales[r["fuente"]] = totales.get(r["fuente"], 0) + 1; totales[r["nivel"]] += 1; totales["confirmados"] += int(bool(par))
        totales["mag_max"] = max(totales["mag_max"], r["mag"])
    totales["lotes"] += 1; totales["eventos"] += len(filas)
    lote = {"lote": batch_id, "filas": len(filas), "duracion_ms": int((time.time() - t0) * 1000), "hora": datetime.now().strftime("%H:%M:%S")}
    historial.append(lote); del historial[:-30]
    agregar_jsonl(EVENTOS, eventos)
    escribir_json(METRICAS, {"totales": totales, "historial": historial, "actualizado": lote["hora"]})
    print(f"\n{C.LOTE} LOTE #{batch_id} {C.X} {C.C}{len(filas)} sismos · {lote['duracion_ms']} ms{C.X}")
    for e in eventos[-8:]:
        col = C.R if e["nivel"] in ("ROJO", "NARANJA") else C.G
        print(f"  {col}M{e['mag']:.1f}{C.X} {e['fuente']:4} {e['prof']:.0f} km · {e['zona']} · {e['lugar']}{' OK confirmado' if e['confirmado'] else ''}")


ventanas = {}


def guardar_ventanas(df, batch_id):
    for r in df.collect():
        v = ventanas.setdefault(r["inicio"], {"inicio": r["inicio"], "USGS": 0, "EMSC": 0, "mag_max": 0})
        v[r["fuente"]] = r["n"]; v["mag_max"] = max(v["mag_max"], r["mag_max"] or 0)
    for k in sorted(ventanas)[:-24]:
        del ventanas[k]
    escribir_json(VENTANAS, [ventanas[k] for k in sorted(ventanas)])


por_hora = (clasificado.withWatermark("ts", "6 hours").groupBy(F.window("ts", "1 hour"), "fuente")
            .agg(F.count("*").alias("n"), F.max("mag").alias("mag_max"))
            .select(F.date_format("window.start", "MM-dd HH:mm").alias("inicio"), "fuente", "n", "mag_max"))

print(f"{C.G}{C.BOLD}$ python monitor_spark.py   (Apache Spark {spark.version}){C.X}\n{C.D}Spark UI: http://localhost:4040 · esperando sismos...{C.X}")
q1 = (clasificado.writeStream.queryName("sismos").foreachBatch(procesar_lote)
      .option("checkpointLocation", ruta_spark(CHECKPOINTS / "sismos")).trigger(processingTime=TRIGGER).start())
q2 = (por_hora.writeStream.queryName("ventanas").outputMode("update").foreachBatch(guardar_ventanas)
      .option("checkpointLocation", ruta_spark(CHECKPOINTS / "ventanas")).trigger(processingTime="5 seconds").start())
try:
    spark.streams.awaitAnyTermination()
except (KeyboardInterrupt, Exception):
    print(f"\n{C.Y}Spark detenido; el checkpoint conserva el avance.{C.X}")
    try:
        q1.stop(); q2.stop(); spark.stop()
    except Exception:
        pass
