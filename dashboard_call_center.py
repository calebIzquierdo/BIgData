#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CME Solutions - Dashboard BI: Call Center Analytics
===================================================
Tablero interactivo hecho con Streamlit + Plotly (tema oscuro estilo Power BI).
Lee los 3 Parquet que generó el ETL (copiados de HDFS) desde la carpeta ./dashboard_data
y muestra 5 tarjetas KPI y 6 gráficos.

Cómo ejecutarlo:
  streamlit run dashboard_call_center.py --server.port 8501 --server.address 0.0.0.0
  Luego abrir en el navegador:  http://100.85.60.127:8501
"""
import os
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ----------------------- Configuración -----------------------
# Carpeta donde están los Parquet (al lado de este script).
HERE      = os.path.dirname(os.path.abspath(__file__))
DATA_DIR  = os.path.join(HERE, "dashboard_data")

# Paleta de colores del tablero (tema oscuro).
BG     = "#1a1d23"   # fondo de la página
CARD   = "#21252b"   # fondo de las tarjetas y gráficos
GRID   = "#2c313a"   # líneas de la cuadrícula
TXT    = "#e6e6e6"   # color del texto
BLUE   = "#4a9eff"
GREEN  = "#26a69a"   # verde = bueno (respondidas)
RED    = "#ef5350"   # rojo = malo (abandonadas)
AMBER  = "#ffa726"
PURPLE = "#ab47bc"

# Configuración general de la página (título, ícono, ancho completo).
st.set_page_config(page_title="CME · Call Center Analytics",
                   page_icon="📞", layout="wide",
                   initial_sidebar_state="collapsed")

# CSS para darle el estilo oscuro y el diseño de las tarjetas KPI.
st.markdown(f"""
<style>
  .stApp {{ background:{BG}; color:{TXT}; }}
  #MainMenu, footer, header {{ visibility:hidden; }}
  .block-container {{ padding-top:1.5rem; max-width:1500px; }}
  .kpi {{ background:{CARD}; border:1px solid {GRID}; border-radius:14px;
          padding:18px 20px; transition:transform .15s ease, border-color .15s ease; }}
  .kpi:hover {{ transform:translateY(-3px); border-color:{BLUE}; }}
  .kpi .lbl {{ font-size:.78rem; color:#9aa3af; letter-spacing:.04em;
               text-transform:uppercase; margin-bottom:6px; }}
  .kpi .val {{ font-size:2.0rem; font-weight:700; line-height:1; }}
  .kpi .sub {{ font-size:.74rem; color:#7f8893; margin-top:6px; }}
  .title  {{ font-size:2.1rem; font-weight:800; margin-bottom:0; }}
  .subtle {{ color:#8b94a0; font-size:.95rem; margin-top:2px; }}
  .insight {{ background:linear-gradient(135deg,#2a1d1d,#21252b);
              border-left:4px solid {RED}; border-radius:10px; padding:18px 22px; }}
  .sect {{ font-size:1.15rem; font-weight:700; margin:6px 0 2px; color:{TXT}; }}
</style>
""", unsafe_allow_html=True)


# ----------------------- Carga de datos -----------------------
@st.cache_data   # guarda en caché para no releer los Parquet en cada interacción
def load():
    """Lee los 3 Parquet y los devuelve listos para usar."""
    kpis    = pd.read_parquet(os.path.join(DATA_DIR, "call_center_kpis.parquet"))     # detalle por día
    resumen = pd.read_parquet(os.path.join(DATA_DIR, "call_center_resumen.parquet"))  # 1 fila global
    corr    = pd.read_parquet(os.path.join(DATA_DIR, "call_center_corr.parquet"))     # por rango
    # Ordenamos los rangos de menor a mayor espera (para que los gráficos salgan en orden).
    orden   = ["0-30s", "31-60s", "61-120s", "121-300s", ">300s"]
    corr["Rango_Espera"] = pd.Categorical(corr["Rango_Espera"], categories=orden, ordered=True)
    corr = corr.sort_values("Rango_Espera")
    return kpis, resumen.iloc[0], corr   # resumen.iloc[0] = la única fila del resumen


kpis, r, corr = load()   # kpis=detalle, r=resumen global, corr=por rango


def fig_layout(fig, h=330):
    """Aplica el mismo estilo oscuro a todos los gráficos (evita repetir código)."""
    fig.update_layout(
        template="plotly_dark", paper_bgcolor=CARD, plot_bgcolor=CARD,
        height=h, margin=dict(l=10, r=10, t=14, b=10),
        font=dict(color=TXT, family="Inter, DejaVu Sans"),
        title=None, title_text="",
        legend=dict(bgcolor="rgba(0,0,0,0)"))
    fig.update_xaxes(gridcolor=GRID, zeroline=False)
    fig.update_yaxes(gridcolor=GRID, zeroline=False)
    return fig


# ----------------------- Encabezado -----------------------
st.markdown('<div class="title">📞 Call Center Analytics</div>', unsafe_allow_html=True)
st.markdown(f'<div class="subtle">CME Solutions · Pipeline Big Data Hadoop + Spark + Streamlit · '
            f'{int(r.Total_Registros):,} días analizados · {int(r.Total_Llamadas_Entrantes):,} llamadas</div>',
            unsafe_allow_html=True)
st.write("")

# ----------------------- 5 Tarjetas KPI -----------------------
def kpi(col, lbl, val, sub, color):
    """Dibuja una tarjeta KPI (etiqueta, valor grande y descripción)."""
    col.markdown(
        f'<div class="kpi"><div class="lbl">{lbl}</div>'
        f'<div class="val" style="color:{color}">{val}</div>'
        f'<div class="sub">{sub}</div></div>', unsafe_allow_html=True)

# Cinco columnas, una tarjeta por KPI. Los valores vienen del resumen global (r).
c1, c2, c3, c4, c5 = st.columns(5)
kpi(c1, "Tasa de Abandono",  f"{r.Tasa_Abandono_Global:.2f}%",
    f"{int(r.Total_Llamadas_Abandonadas):,} llamadas perdidas", RED)
kpi(c2, "Tasa de Respuesta", f"{r.Tasa_Respuesta_Global:.2f}%",
    f"{int(r.Total_Llamadas_Respondidas):,} atendidas", GREEN)
kpi(c3, "ASA · Espera prom.", f"{r.ASA_Promedio_Seg:.1f}s",
    "Average Speed of Answer", BLUE)
kpi(c4, "AHT · Manejo prom.", f"{r.AHT_Promedio_Seg:.1f}s",
    "Average Handle Time", AMBER)
ns = r.NS_20s_Promedio
kpi(c5, "Nivel de Servicio 20s", f"{ns:.1f}%" if pd.notna(ns) else "—",
    "% contestadas ≤ 20s", PURPLE)

st.write("")

# ----------------------- Fila 1: Donut + Barras por rango -----------------------
g1, g2 = st.columns([1, 1.4])

with g1:
    # Gráfico de dona: proporción de llamadas respondidas vs abandonadas.
    st.markdown('<div class="sect">Distribución de llamadas</div>', unsafe_allow_html=True)
    donut = go.Figure(go.Pie(
        labels=["Respondidas", "Abandonadas"],
        values=[int(r.Total_Llamadas_Respondidas), int(r.Total_Llamadas_Abandonadas)],
        hole=.62, marker=dict(colors=[GREEN, RED]),
        textinfo="percent", textfont=dict(size=15)))
    # Texto en el centro de la dona: total de llamadas entrantes.
    donut.add_annotation(text=f"<b>{int(r.Total_Llamadas_Entrantes):,}</b><br>entrantes",
                         showarrow=False, font=dict(size=16, color=TXT))
    st.plotly_chart(fig_layout(donut), use_container_width=True)

with g2:
    # Barras horizontales: tasa de abandono según el rango de espera.
    st.markdown('<div class="sect">Tasa de abandono por rango de espera</div>', unsafe_allow_html=True)
    # Color según el nivel: verde (<3%), ámbar (<10%) o rojo (>=10%).
    colors = [GREEN if x < 3 else AMBER if x < 10 else RED for x in corr["Tasa_Abandono_Promedio"]]
    bar = go.Figure(go.Bar(
        x=corr["Tasa_Abandono_Promedio"], y=corr["Rango_Espera"].astype(str),
        orientation="h", marker_color=colors,
        text=[f"{v:.1f}%" for v in corr["Tasa_Abandono_Promedio"]], textposition="outside"))
    bar.update_xaxes(title="Tasa de abandono (%)")
    st.plotly_chart(fig_layout(bar), use_container_width=True)

# ----------------------- Fila 2: Dispersión + Top 10 -----------------------
g3, g4 = st.columns(2)

with g3:
    # Gráfico de dispersión: cada punto es un día (espera vs abandonadas) + línea de tendencia.
    st.markdown(f'<div class="sect">Correlación espera vs abandono '
                f'(r = {r.Corr_Espera_vs_Abandono:.3f})</div>', unsafe_allow_html=True)
    x = kpis["Waiting_Time_sec"].astype(float)
    y = kpis["Abandoned_Calls"].astype(float)
    sc = go.Figure(go.Scatter(x=x, y=y, mode="markers",
                              marker=dict(color=BLUE, size=5, opacity=.45)))
    # np.polyfit calcula la recta que mejor se ajusta a los puntos (la tendencia).
    m, b = np.polyfit(x, y, 1)
    xs = np.array([x.min(), x.max()])
    sc.add_trace(go.Scatter(x=xs, y=m * xs + b, mode="lines",
                            line=dict(color=AMBER, width=3), name="Tendencia"))
    sc.update_xaxes(title="Tiempo de espera (s)")
    sc.update_yaxes(title="Llamadas abandonadas")
    sc.update_layout(showlegend=False)
    st.plotly_chart(fig_layout(sc), use_container_width=True)

with g4:
    # Barras apiladas: los 10 días con más abandono (respondidas + abandonadas).
    st.markdown('<div class="sect">Top 10 días con mayor abandono</div>', unsafe_allow_html=True)
    top = kpis.nlargest(10, "Abandoned_Calls").sort_values("Abandoned_Calls")
    t = go.Figure()
    t.add_trace(go.Bar(y=top["Index"].astype(str), x=top["Answered_Calls"],
                       name="Respondidas", orientation="h", marker_color=GREEN))
    t.add_trace(go.Bar(y=top["Index"].astype(str), x=top["Abandoned_Calls"],
                       name="Abandonadas", orientation="h", marker_color=RED))
    t.update_layout(barmode="stack")   # apila las dos series en una sola barra
    t.update_xaxes(title="Llamadas")
    t.update_yaxes(title="Día (Index)")
    st.plotly_chart(fig_layout(t), use_container_width=True)

# ----------------------- Fila 3: Área en el tiempo -----------------------
# Muestra cómo evoluciona el volumen de llamadas día a día (respondidas + abandonadas apiladas).
st.markdown('<div class="sect">Volumen de llamadas en el tiempo</div>', unsafe_allow_html=True)
ar = go.Figure()
ar.add_trace(go.Scatter(x=kpis["Index"], y=kpis["Answered_Calls"], mode="lines",
                        stackgroup="one", name="Respondidas",
                        line=dict(width=0.5, color=GREEN)))
ar.add_trace(go.Scatter(x=kpis["Index"], y=kpis["Abandoned_Calls"], mode="lines",
                        stackgroup="one", name="Abandonadas",
                        line=dict(width=0.5, color=RED)))
ar.update_xaxes(title="Día (Index)")
ar.update_yaxes(title="Llamadas")
st.plotly_chart(fig_layout(ar, h=300), use_container_width=True)

# ----------------------- Fila 4: Tabla + Hallazgo -----------------------
t1, t2 = st.columns([1.5, 1])

with t1:
    # Tabla con todas las métricas por rango de espera (renombramos las columnas a algo legible).
    st.markdown('<div class="sect">Métricas por rango de espera</div>', unsafe_allow_html=True)
    tabla = corr.copy()
    tabla["Rango_Espera"] = tabla["Rango_Espera"].astype(str)
    tabla = tabla.rename(columns={
        "Rango_Espera": "Rango", "Registros": "Días",
        "Tasa_Abandono_Promedio": "Abandono %", "ASA_Promedio": "ASA (s)",
        "AHT_Promedio": "AHT (s)", "NS_20s_Promedio": "NS 20s %",
        "Total_Llamadas": "Llamadas", "Total_Abandonadas": "Abandonadas"})
    st.dataframe(tabla, use_container_width=True, hide_index=True)

with t2:
    # Cuadro con el hallazgo principal: el servicio colapsa pasados los 5 minutos de espera.
    st.markdown('<div class="sect">Hallazgo crítico</div>', unsafe_allow_html=True)
    peor = corr.iloc[-1]   # el peor rango (>300s) es la última fila
    st.markdown(
        f'<div class="insight">'
        f'<b style="color:{RED};font-size:1.05rem">⚠ El servicio colapsa a los 5 minutos.</b><br><br>'
        f'Superados los <b>300 s</b> de espera, la tasa de abandono salta a '
        f'<b style="color:{RED}">{peor.Tasa_Abandono_Promedio:.1f}%</b> '
        f'(vs ~2.9% bajo 2 min).<br><br>'
        f'La correlación estadística <b>r = {r.Corr_Espera_vs_Abandono:.3f}</b> confirma que el '
        f'tiempo de espera es el <b>predictor dominante</b> del abandono.'
        f'</div>', unsafe_allow_html=True)

# Pie de página con la fuente de los datos.
st.write("")
st.markdown(f'<div class="subtle">Fuente: HDFS /processed/*.parquet · '
            f'ETL PySpark sobre YARN · CME Solutions © 2026</div>', unsafe_allow_html=True)
