# Monitoreo de la sismicidad en tiempo casi real

Pipeline con Apache Spark Structured Streaming que reúne los catálogos USGS y EMSC, los unifica y los muestra en un mapa con fallas activas.

## Archivos
- `ingesta.py`: websocket de EMSC y consulta de USGS y EMSC cada 30 s. Escribe lotes en `data/stream/`.
- `monitor_spark.py`: clasifica por magnitud, profundidad y zona; cruza redes; guarda historial en Parquet.
- `servidor_web.py`: sirve el mapa en http://localhost:8000.
- `capas.py`: descarga las fallas activas (GEM) recortadas a Colombia.
- `probar_fuentes.py`: prueba qué direcciones responden.
- `verificar.py`: revisa Python, Java, winutils y el puerto.

## Fuente principal: SGC

El sistema consulta el feed público de los últimos cinco días del SGC cada 60 segundos y acepta solo eventos con `agency = SGC` dentro de Colombia. USGS y EMSC se conservan para comparación, también filtrados por territorio colombiano.

## Ejecución
```
pip install -r requirements.txt
python verificar.py
python capas.py
```
Después, cada uno en su terminal y en este orden:
```
python monitor_spark.py
python servidor_web.py
python ingesta.py
```
`python limpiar.py` borra `data/` (luego ejecutar `python capas.py`).

## Mapa
- Un solo marcador por sismo aunque varias redes lo reporten (90 s y 100 km). Prioridad USGS, EMSC.
- Pulso sísmico, actividad de 24 h frente a lo habitual y gráfico por día.
- Aviso y zonas más expuestas para sismos desde la magnitud elegida: MMI = 1.09·M + 5.07 − 3.69·log10(d) (Bakun, calibrado en California). Estimación referencial; población y coordenadas de ~60 ciudades aproximadas (`CIU` en `web/index.html`).
- Panel de latencia entre redes y descarga CSV.

## Notas
- Las horas se muestran en hora de Colombia (UTC-5). `python probar_fuentes.py` muestra los últimos sismos que devuelve cada fuente.
- No es predicción ni alerta temprana.
- 4 GB de RAM: Spark corre con `local[2]` y 1 GB.
