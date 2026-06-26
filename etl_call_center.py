#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CME Solutions - Pipeline Big Data: Call Center Analytics
========================================================
ETL con PySpark sobre YARN. Sigue 4 pasos: EXTRACT -> TRANSFORM -> ANALISIS -> LOAD.

  EXTRACT   : lee el CSV crudo desde HDFS.
  TRANSFORM : limpia los datos y calcula los KPIs por fila.
  ANALISIS  : 6 consultas SQL pequeñas (una por pregunta de negocio).
  LOAD      : guarda 3 resultados en formato Parquet de vuelta en HDFS.

Entrada : hdfs://100.118.243.53:9000/datasets/call_center.csv   (1251 filas, 9 columnas)
Salida  : hdfs://.../processed/call_center_kpis.parquet      (todas las filas + KPIs)
          hdfs://.../processed/call_center_resumen.parquet   (1 fila: KPIs globales)
          hdfs://.../processed/call_center_corr.parquet      (KPIs por rango de espera)

Cómo ejecutarlo (sobre YARN, desde caleb-hp):
  spark-submit --master yarn --deploy-mode client \
      --driver-memory 1g --executor-memory 1g etl_call_center.py
"""
from pyspark.sql import SparkSession, functions as F

# Rutas en HDFS: de dónde leemos y a dónde escribimos.
HDFS   = "hdfs://100.118.243.53:9000"
SRC    = f"{HDFS}/datasets/call_center.csv"   # archivo de entrada
OUTDIR = f"{HDFS}/processed"                   # carpeta de salida


# ---- Funciones auxiliares de conversión ----

def hms_to_sec(col):
    """Convierte una hora en texto 'H:MM:SS' a un número entero de segundos.
    Ejemplo: '0:02:45' -> 165 segundos."""
    p = F.split(col, ":")                      # parte el texto por los ':'
    return (p.getItem(0).cast("int") * 3600    # horas  -> segundos
            + p.getItem(1).cast("int") * 60    # minutos -> segundos
            + p.getItem(2).cast("int")).cast("int")


def pct_to_num(col):
    """Convierte un porcentaje en texto '94.01%' a número 94.01."""
    return F.regexp_replace(col, "%", "").cast("double")  # quita el '%' y lo vuelve número


def main():
    # Crea la sesión de Spark (el punto de entrada para todo el trabajo).
    spark = (SparkSession.builder
             .appName("CME-CallCenter-ETL")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")     # muestra solo avisos importantes

    # ==================== 1) EXTRACT ====================
    # Leemos el CSV. inferSchema=false (NO adivinar tipos) es a propósito:
    # si Spark adivina, interpreta las horas '0:00:17' como fecha/hora y se rompe.
    # Por eso leemos TODO como texto y convertimos a mano en el paso TRANSFORM.
    raw = (spark.read
           .option("header", "true")
           .option("inferSchema", "false")
           .csv(SRC))

    # ==================== 2) TRANSFORM ====================
    # Renombramos las columnas del CSV a nombres sin espacios (más fáciles de usar).
    rename = {
        "Index": "Index",
        "Incoming Calls": "Incoming_Calls",
        "Answered Calls": "Answered_Calls",
        "Answer Rate": "Answer_Rate_raw",
        "Abandoned Calls": "Abandoned_Calls",
        "Answer Speed (AVG)": "Answer_Speed_AVG",
        "Talk Duration (AVG)": "Talk_Duration_AVG",
        "Waiting Time (AVG)": "Waiting_Time_AVG",
        "Service Level (20 Seconds)": "Service_Level_raw",
    }
    for old, new in rename.items():
        if old in raw.columns:
            raw = raw.withColumnRenamed(old, new)

    # Creamos columnas nuevas a partir del texto crudo.
    df = (raw
          # a) Texto -> número entero (las leímos como texto por inferSchema=false)
          .withColumn("Index",           F.col("Index").cast("int"))
          .withColumn("Incoming_Calls",  F.col("Incoming_Calls").cast("int"))
          .withColumn("Answered_Calls",  F.col("Answered_Calls").cast("int"))
          .withColumn("Abandoned_Calls", F.col("Abandoned_Calls").cast("int"))
          # b) Horas 'HH:MM:SS' -> segundos
          .withColumn("Answer_Speed_sec",  hms_to_sec(F.col("Answer_Speed_AVG")))
          .withColumn("Talk_Duration_sec", hms_to_sec(F.col("Talk_Duration_AVG")))
          .withColumn("Waiting_Time_sec",  hms_to_sec(F.col("Waiting_Time_AVG")))
          # c) Porcentajes en texto '94.01%' -> número
          .withColumn("Answer_Rate_pct", pct_to_num(F.col("Answer_Rate_raw")))
          .withColumn("NS_20s_pct",      pct_to_num(F.col("Service_Level_raw")))
          # d) KPIs calculados para CADA día (cada fila)
          .withColumn("Tasa_Abandono_pct",   # % de llamadas que se abandonaron ese día
                      F.round(F.col("Abandoned_Calls") * 100.0 / F.col("Incoming_Calls"), 2))
          .withColumn("Tasa_Respuesta_pct",  # % de llamadas que se atendieron
                      F.round(F.col("Answered_Calls") * 100.0 / F.col("Incoming_Calls"), 2))
          .withColumn("ASA_seg",  F.col("Answer_Speed_sec"))                       # velocidad de respuesta
          .withColumn("AHT_seg",  F.col("Talk_Duration_sec") + F.col("Answer_Speed_sec"))  # tiempo total de manejo
          # e) Clasificamos cada día según cuánto se esperó. Lo hacemos AQUÍ (no en SQL)
          #    para que la consulta de segmentación quede corta y limpia.
          .withColumn("Rango_Espera",
                      F.when(F.col("Waiting_Time_sec") <= 30,  "0-30s")
                       .when(F.col("Waiting_Time_sec") <= 60,  "31-60s")
                       .when(F.col("Waiting_Time_sec") <= 120, "61-120s")
                       .when(F.col("Waiting_Time_sec") <= 300, "121-300s")
                       .otherwise(">300s")))

    # Registramos el DataFrame como tabla 'call_center' para poder consultarla con SQL.
    df.createOrReplaceTempView("call_center")
    # La guardamos en memoria (cache) porque las 6 consultas la van a leer varias veces.
    spark.catalog.cacheTable("call_center")

    # ==================== 3) ANALISIS SQL (6 consultas pequeñas) ====================
    # Cada consulta responde UNA sola pregunta. Así son fáciles de leer y de revisar.

    # Consulta 1 — ¿Cuántas llamadas hubo en total?
    q1_volumenes = spark.sql("""
        SELECT COUNT(*)             AS Total_Registros,
               SUM(Incoming_Calls)  AS Total_Llamadas_Entrantes,
               SUM(Answered_Calls)  AS Total_Llamadas_Respondidas,
               SUM(Abandoned_Calls) AS Total_Llamadas_Abandonadas
        FROM call_center
    """)

    # Consulta 2 — ¿Qué porcentaje se abandonó y qué porcentaje se atendió?
    q2_tasas = spark.sql("""
        SELECT ROUND(SUM(Abandoned_Calls)*100.0/SUM(Incoming_Calls), 2) AS Tasa_Abandono_Global,
               ROUND(SUM(Answered_Calls)*100.0/SUM(Incoming_Calls), 2)  AS Tasa_Respuesta_Global
        FROM call_center
    """)

    # Consulta 3 — ¿Cuánto se tardó en promedio? (responder, manejar y esperar)
    q3_tiempos = spark.sql("""
        SELECT ROUND(AVG(ASA_seg), 1)          AS ASA_Promedio_Seg,
               ROUND(AVG(AHT_seg), 1)          AS AHT_Promedio_Seg,
               ROUND(AVG(Waiting_Time_sec), 1) AS Espera_Promedio_Seg
        FROM call_center
    """)

    # Consulta 4 — Nivel de servicio: % de llamadas contestadas en menos de 20 segundos.
    q4_ns = spark.sql("""
        SELECT ROUND(AVG(NS_20s_pct), 1) AS NS_20s_Promedio
        FROM call_center
    """)

    # Consulta 5 — ¿Se relacionan el tiempo de espera y el abandono? (CORR: -1 a 1)
    q5_corr = spark.sql("""
        SELECT ROUND(CORR(Waiting_Time_sec, Abandoned_Calls), 3) AS Corr_Espera_vs_Abandono
        FROM call_center
    """)

    # Consulta 6 — Los mismos KPIs pero AGRUPADOS por rango de espera.
    # Como 'Rango_Espera' ya se calculó en el TRANSFORM, la consulta es un GROUP BY simple.
    q6_rangos = spark.sql("""
        SELECT Rango_Espera,
               COUNT(*)                         AS Registros,
               ROUND(AVG(Tasa_Abandono_pct), 2) AS Tasa_Abandono_Promedio,
               ROUND(AVG(ASA_seg), 1)           AS ASA_Promedio,
               ROUND(AVG(AHT_seg), 1)           AS AHT_Promedio,
               ROUND(AVG(NS_20s_pct), 1)        AS NS_20s_Promedio,
               SUM(Incoming_Calls)              AS Total_Llamadas,
               SUM(Abandoned_Calls)             AS Total_Abandonadas
        FROM call_center
        GROUP BY Rango_Espera
        ORDER BY MIN(Waiting_Time_sec)
    """)

    # Juntamos las 5 primeras consultas (cada una da 1 fila) en un solo "resumen" global.
    # CROSS JOIN pega las columnas de todas en una única fila.
    for nm, qv in [("v_volumenes", q1_volumenes), ("v_tasas", q2_tasas),
                   ("v_tiempos", q3_tiempos), ("v_ns", q4_ns), ("v_corr", q5_corr)]:
        qv.createOrReplaceTempView(nm)
    resumen = spark.sql("""
        SELECT * FROM v_volumenes
        CROSS JOIN v_tasas CROSS JOIN v_tiempos CROSS JOIN v_ns CROSS JOIN v_corr
    """)
    corr_rangos = q6_rangos

    # Mostramos los resultados en consola (útil para verificar antes de guardar).
    print("\n========== RESUMEN GLOBAL ==========")
    resumen.show(truncate=False)
    print("========== POR RANGO DE ESPERA ==========")
    corr_rangos.show(truncate=False)

    # ==================== 4) LOAD ====================
    # Guardamos los 3 resultados en HDFS como Parquet (formato columnar, comprimido con Snappy).
    # coalesce(1) = escribir un solo archivo por dataset (más fácil de mover/leer).
    (df.coalesce(1).write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(f"{OUTDIR}/call_center_kpis.parquet"))       # detalle: todas las filas + KPIs
    (resumen.coalesce(1).write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(f"{OUTDIR}/call_center_resumen.parquet"))    # 1 fila: KPIs globales
    (corr_rangos.coalesce(1).write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(f"{OUTDIR}/call_center_corr.parquet"))       # KPIs por rango de espera

    print("\nETL COMPLETADO. Parquets escritos en", OUTDIR)
    spark.stop()   # cierra la sesión y libera los recursos del clúster


if __name__ == "__main__":
    main()
