# Código — Call Center Analytics (CME Solutions)

Pipeline de Big Data sobre el clúster Hadoop 3.3.6 + Spark 3.5.4 (3 nodos).

## Archivos

| Archivo | Capa de la arquitectura | Descripción |
|---|---|---|
| `etl_call_center.py` | **Procesamiento (Spark sobre YARN)** | Job ETL en PySpark. Lee el CSV desde HDFS, limpia y transforma, calcula los 5 KPIs y la correlación con Spark SQL, y escribe 3 datasets en Parquet a HDFS. |
| `dashboard_call_center.py` | **Visualización (Streamlit)** | Dashboard BI (Streamlit + Plotly, tema oscuro) que lee los Parquet y muestra los 5 KPIs y 6 gráficos. |

## 1. ETL — `etl_call_center.py`

Implementa el patrón **EXTRACT → TRANSFORM → LOAD**:

- **EXTRACT:** lee `hdfs://100.118.243.53:9000/datasets/call_center.csv` con `inferSchema=false`
  (las horas `0:00:17` no deben inferirse como timestamp).
- **TRANSFORM:** convierte tiempos `H:MM:SS` a segundos, porcentajes `76.28%` a número, y calcula
  por fila: `Tasa_Abandono_pct`, `Tasa_Respuesta_pct`, `ASA_seg`, `AHT_seg`, `NS_20s_pct`.
- **ANÁLISIS (Spark SQL):** seis consultas pequeñas, cada una responde una pregunta de negocio
  (1. volúmenes · 2. tasas · 3. tiempos · 4. NS 20s · 5. correlación · 6. segmentación por rango).
  El rango de espera se calcula en el TRANSFORM para que la consulta 6 sea un `GROUP BY` simple.
- **LOAD:** escribe en Parquet + Snappy a `hdfs://.../processed/`:
  `call_center_kpis.parquet`, `call_center_resumen.parquet`, `call_center_corr.parquet`.

### Ejecución (sobre YARN)

```bash
# Desde caleb-hp. SPARK_LOCAL_IP / driver.host fuerzan la IP de Tailscale
# porque caleb-hp tiene una interfaz VPN (Fortinet) que los executors remotos no alcanzan.
SPARK_LOCAL_IP=100.85.60.127 \
sudo -u hadoop env \
  JAVA_HOME=/opt/hadoop/jdk HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop \
  YARN_CONF_DIR=/opt/hadoop/etc/hadoop SPARK_HOME=/opt/spark PYSPARK_PYTHON=python3 \
  SPARK_LOCAL_IP=100.85.60.127 \
  /opt/spark/bin/spark-submit \
    --master yarn --deploy-mode client \
    --driver-memory 1g --executor-memory 1g --num-executors 3 \
    --conf spark.driver.host=100.85.60.127 \
    --conf spark.driver.bindAddress=100.85.60.127 \
    etl_call_center.py
```

## 2. Dashboard — `dashboard_call_center.py`

```bash
# 1) Traer los Parquet de HDFS a ./dashboard_data/
hdfs dfs -copyToLocal -f /processed/call_center_kpis.parquet    ./dashboard_data/
hdfs dfs -copyToLocal -f /processed/call_center_resumen.parquet ./dashboard_data/
hdfs dfs -copyToLocal -f /processed/call_center_corr.parquet    ./dashboard_data/

# 2) Levantar el dashboard
streamlit run dashboard_call_center.py --server.port 8501 --server.address 0.0.0.0
# Acceso: http://100.85.60.127:8501
```

> El dashboard espera los Parquet en la carpeta `dashboard_data/` junto al script.

## Resultados (datos reales)

- Tasa de Abandono: **10.93%** · Tasa de Respuesta: **89.07%**
- ASA: **24.9 s** · AHT: **182.4 s** · NS 20s: **70.9%**
- Correlación espera–abandono: **r = 0.724**
- Hallazgo: el abandono salta a **16.17%** superados los 300 s de espera.
