import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import random
import time
import copy
from datetime import datetime, timedelta
from collections import deque

# ── Configuracion de Pagina ───────────────────────────────────────────────────
st.set_page_config(
    page_title="LogisticsOps — Demo Dashboard",
    page_icon="🚛",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Constantes ────────────────────────────────────────────────────────────────
CAMIONES_INIT = [
    {"truck_id": "T-1",  "lat": 40.416, "lon": -3.703, "speed": 85,  "delay_minutes": 5,  "cargo_type": "Seco",        "warehouse_id": "W-7", "route_id": "R-3"},
    {"truck_id": "T-2",  "lat": 40.453, "lon": -3.688, "speed": 0,   "delay_minutes": 45, "cargo_type": "Refrigerado", "warehouse_id": "W-8", "route_id": "R-1"},
    {"truck_id": "T-3",  "lat": 40.389, "lon": -3.721, "speed": 112, "delay_minutes": 12, "cargo_type": "Seco",        "warehouse_id": "W-6", "route_id": "R-7"},
    {"truck_id": "T-4",  "lat": 40.471, "lon": -3.756, "speed": 67,  "delay_minutes": 35, "cargo_type": "Refrigerado", "warehouse_id": "W-9", "route_id": "R-2"},
    {"truck_id": "T-5",  "lat": 40.402, "lon": -3.669, "speed": 0,   "delay_minutes": 8,  "cargo_type": "Seco",        "warehouse_id": "W-7", "route_id": "R-5"},
    {"truck_id": "T-6",  "lat": 40.438, "lon": -3.712, "speed": 93,  "delay_minutes": 55, "cargo_type": "Refrigerado", "warehouse_id": "W-6", "route_id": "R-4"},
    {"truck_id": "T-7",  "lat": 40.361, "lon": -3.698, "speed": 44,  "delay_minutes": 2,  "cargo_type": "Seco",        "warehouse_id": "W-8", "route_id": "R-9"},
    {"truck_id": "T-8",  "lat": 40.490, "lon": -3.735, "speed": 78,  "delay_minutes": 18, "cargo_type": "Refrigerado", "warehouse_id": "W-10","route_id": "R-6"},
    {"truck_id": "T-9",  "lat": 40.378, "lon": -3.744, "speed": 101, "delay_minutes": 0,  "cargo_type": "Seco",        "warehouse_id": "W-9", "route_id": "R-8"},
    {"truck_id": "T-10", "lat": 40.425, "lon": -3.669, "speed": 55,  "delay_minutes": 42, "cargo_type": "Refrigerado", "warehouse_id": "W-6", "route_id": "R-10"},
]

PAGERANK_DATA = {
    "warehouse_id": ["W-7", "W-3", "W-9", "W-1", "W-6"],
    "importance_score": [2.847, 2.312, 1.956, 1.634, 1.201],
}

DARK_CSS = """
<style>
    .stApp { background-color: #0d1117; }
    .stMetric { background-color: #161b22; border: 1px solid #21262d; border-radius: 8px; padding: 12px; }
    .stDataFrame { background-color: #161b22; }
    div[data-testid="stSidebarContent"] { background-color: #161b22; }
    h1, h2, h3 { color: #e6edf3; }
    .metric-label { color: #8b949e; }
    /* Borde de acento en secciones */
    .section-header { 
        border-left: 3px solid #00d4aa; 
        padding-left: 10px; 
        color: #00d4aa; 
        margin-top: 1.5em; 
        margin-bottom: 0.8em; 
        font-weight: 600; 
        font-size: 1.2em;
    }
</style>
"""

# ── Simulación (replica lógica del pipeline real) ─────────────────────────────
def generar_evento(tasa_invalidos: float, pct_refrigerado: float) -> dict:
    """
    Simula un GenerateFlowFile de NiFi con las mismas expresiones
    que usa el procesador real en el compose.
    Un % de los eventos (configurable) tienen speed=999 para simular
    el caso inválido que NiFi enruta al DLQ.
    """
    invalido = random.random() < tasa_invalidos
    return {
        "truck_id": f"T-{random.randint(1, 50)}",
        "route_id": f"R-{random.randint(1, 10)}",
        "origin_warehouse": f"W-{random.randint(1, 5)}",
        "dest_warehouse": f"W-{random.randint(6, 10)}",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "latitude": round(40 + random.random(), 3),
        "longitude": round(-3 - random.random(), 3),
        "speed": 999 if invalido else random.randint(0, 120),
        "delay_minutes": random.randint(0, 60),
        "cargo_type": "Refrigerado" if random.random() < pct_refrigerado else "Seco",
    }

def es_valido(evento: dict) -> bool:
    """
    Replica la lógica del RouteOnAttribute de NiFi:
    - truck_id no vacío
    - speed numérico en [0, 120]
    - delay_minutes numérico en [0, 180]
    - latitude y longitude numéricos
    """
    try:
        speed = float(evento["speed"])
        delay = float(evento["delay_minutes"])
        float(evento["latitude"])
        float(evento["longitude"])
        return (
            bool(evento["truck_id"])
            and 0 <= speed <= 120
            and 0 <= delay <= 180
        )
    except (ValueError, TypeError):
        return False

def actualizar_camion(camion: dict) -> dict:
    """
    Simula la actualización del registro en Cassandra truck_last_state.
    Mueve las coordenadas ligeramente (camión en movimiento),
    actualiza speed y delay_minutes, y actualiza updated_at.
    """
    camion["lat"] += random.uniform(-0.005, 0.005)
    camion["lon"] += random.uniform(-0.005, 0.005)
    
    if random.random() < 0.1:
        camion["speed"] = 0
    elif camion["speed"] == 0 and random.random() < 0.3:
        camion["speed"] = random.randint(40, 90)
    else:
        camion["speed"] = max(0, min(120, camion["speed"] + random.randint(-15, 15)))
        
    if random.random() < 0.2:
        camion["delay_minutes"] = max(0, camion["delay_minutes"] + random.randint(-5, 10))
        
    camion["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return camion

# ── Inicialización ────────────────────────────────────────────────────────────
def init_session_state():
    """Inicializa el estado de la sesión con valores de partida realistas."""
    defaults = {
        "simulacion_activa": True,
        "tasa_invalidos": 0.10,
        "pct_refrigerado": 0.50,
        "intervalo_refresco": 3,
        "total_eventos": 0,
        "total_validos": 0,
        "total_dlq": 0,
        "prev_total": 0,
        "prev_validos": 0,
        "prev_dlq": 0,
        "log_eventos": deque(maxlen=15),
        "camiones": copy.deepcopy(CAMIONES_INIT),
        "delay_historico": {
            "W-7": [random.uniform(8, 25) for _ in range(8)],
            "W-6": [random.uniform(15, 40) for _ in range(8)],
            "W-9": [random.uniform(5, 20) for _ in range(8)],
        },
        "timestamps_ventanas": [
            (datetime.now() - timedelta(minutes=15 * i)).strftime("%H:%M")
            for i in range(7, -1, -1)
        ],
        "ultimo_evento": None,
    }

    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v
            
    for camion in st.session_state["camiones"]:
        if "updated_at" not in camion:
            camion["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def resetear_simulacion():
    """Limpia el state para reiniciar."""
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    init_session_state()

# ── Componentes UI ─────────────────────────────────────────────────────────────
def render_sidebar() -> tuple[bool, int, float, float]:
    """Devuelve (activa, intervalo, tasa_invalidos, pct_refrigerado)."""
    st.sidebar.title("Panel de Control")
    activa = st.sidebar.toggle("Simulación activa", value=st.session_state.get("simulacion_activa", True))
    intervalo = st.sidebar.slider("Intervalo de refresco (seg)", 1, 10, value=st.session_state.get("intervalo_refresco", 3))
    
    tasa_inv_pct = st.sidebar.slider("Tasa de eventos inválidos (%)", 0, 40, value=int(st.session_state.get("tasa_invalidos", 0.1)*100))
    tasa_inv = tasa_inv_pct / 100.0
    
    pct_ref_pct = st.sidebar.slider("% carga refrigerada", 0, 100, value=int(st.session_state.get("pct_refrigerado", 0.5)*100))
    pct_ref = pct_ref_pct / 100.0
    
    st.sidebar.divider()
    
    if st.sidebar.button("Resetear simulación", use_container_width=True):
        resetear_simulacion()
        st.rerun()
        
    st.sidebar.markdown("---")
    st.sidebar.caption("Todos los datos son simulados. Los valores reproducen la lógica real del pipeline.")
    
    return activa, intervalo, tasa_inv, pct_ref

def render_metricas_superiores():
    total = st.session_state["total_eventos"]
    validos = st.session_state["total_validos"]
    dlq = st.session_state["total_dlq"]
    
    delta_total = total - st.session_state["prev_total"]
    delta_validos = validos - st.session_state["prev_validos"]
    delta_dlq = dlq - st.session_state["prev_dlq"]
    
    st.session_state["prev_total"] = total
    st.session_state["prev_validos"] = validos
    st.session_state["prev_dlq"] = dlq

    tasa = (validos / total * 100) if total > 0 else 0
    
    camiones = st.session_state["camiones"]
    activos = sum(1 for c in camiones if c["speed"] > 0)
    delay_medio = sum(c["delay_minutes"] for c in camiones) / len(camiones) if camiones else 0
    
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1: st.metric("Eventos totales", total, f"+{delta_total}" if delta_total > 0 else None)
    with c2: st.metric("Eventos válidos", validos, f"+{delta_validos}" if delta_validos > 0 else None)
    with c3: st.metric("Eventos DLQ", dlq, f"+{delta_dlq}" if delta_dlq > 0 else None, delta_color="inverse")
    with c4: st.metric("Tasa de validez", f"{tasa:.1f}%")
    with c5: st.metric("Camiones activos", activos)
    with c6: st.metric("Delay medio global", f"{delay_medio:.1f} min")

def render_log_eventos():
    """Muestra el log circular de los últimos 15 eventos."""
    st.markdown('<div class="section-header">📡 Flujo Kafka — eventos recientes</div>', unsafe_allow_html=True)
    
    eventos = list(st.session_state["log_eventos"])
    if not eventos:
        st.info("Iniciando simulación... esperando eventos.")
        return
        
    df = pd.DataFrame(eventos)
    df_show = df[["timestamp", "truck_id", "speed", "delay_minutes", "cargo_type", "_valido", "_destino"]].copy()
    df_show.rename(columns={
        "delay_minutes": "delay_min", 
        "cargo_type": "cargo", 
        "_valido": "estado", 
        "_destino": "destino"
    }, inplace=True)
    
    df_show["estado"] = df_show["estado"].apply(lambda x: "✅ válido" if x else "❌ DLQ")
    
    def style_estado(val):
        color = '#00d4aa' if '✅' in str(val) else '#ff6b6b'
        return f'color: {color}'
        
    st.dataframe(
        df_show.style.map(style_estado, subset=['estado']),
        hide_index=True,
        use_container_width=True,
    )
    
    with st.expander("Ver detalle del último evento (JSON)"):
        st.json(eventos[0])

def render_estado_camiones():
    """Replica el contenido de la tabla Cassandra logistics.truck_last_state."""
    st.markdown('<div class="section-header">🚛 Estado de camiones — Cassandra `truck_last_state`</div>', unsafe_allow_html=True)
    
    camiones = st.session_state["camiones"]
    df = pd.DataFrame(camiones)
    df = df[["truck_id", "speed", "delay_minutes", "cargo_type", "warehouse_id", "route_id", "lat", "lon", "updated_at"]]
    
    df_show = df.copy()
    df_show["cargo_type"] = df_show["cargo_type"].apply(lambda x: "🧊 Refrigerado" if x == "Refrigerado" else "Seco")
    
    def highlight_row(row):
        cols = [''] * len(row)
        delay_idx = df_show.columns.get_loc('delay_minutes')
        speed_idx = df_show.columns.get_loc('speed')
        
        if row['delay_minutes'] > 30:
            cols[delay_idx] = 'color: #ff6b6b; font-weight: 600'
        if row['speed'] == 0:
            cols[speed_idx] = 'color: #ffa94d; font-weight: 600'
            
        return cols
        
    st.dataframe(
        df_show.style.apply(highlight_row, axis=1),
        hide_index=True,
        use_container_width=True
    )

def render_mapa_camiones():
    """Mapa Plotly mostrando posición actual de los camiones."""
    camiones = st.session_state["camiones"]
    df = pd.DataFrame(camiones)
    
    def determine_color(row):
        if row["delay_minutes"] > 30:
            return "Delay crítico"
        elif row["speed"] == 0:
            return "Parado"
        return "OK"
        
    df["status"] = df.apply(determine_color, axis=1)
    
    color_discrete_map = {
        "OK": "#00d4aa",
        "Parado": "#ffa94d",
        "Delay crítico": "#ff6b6b"
    }

    try:
        fig = px.scatter_mapbox(
            df, lat="lat", lon="lon",
            hover_name="truck_id",
            hover_data=["speed", "delay_minutes", "cargo_type", "route_id"],
            color="status", color_discrete_map=color_discrete_map,
            zoom=9, center={"lat": 40.4168, "lon": -3.7038},
            mapbox_style="carto-darkmatter"
        )
    except Exception:
        fig = px.scatter_geo(
            df, lat="lat", lon="lon",
            hover_name="truck_id",
            hover_data=["speed", "delay_minutes", "cargo_type", "route_id"],
            color="status", color_discrete_map=color_discrete_map,
            scope="europe"
        )
        fig.update_geos(
            center=dict(lon=-3.7038, lat=40.4168),
            lataxis_range=[40.0, 41.0], lonaxis_range=[-4.5, -3.0],
            bgcolor="#0d1117"
        )

    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        paper_bgcolor="#0d1117", plot_bgcolor="#0d1117",
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01, bgcolor="rgba(22, 27, 34, 0.8)", font=dict(color="#e6edf3"))
    )
    st.plotly_chart(fig, use_container_width=True)

def render_batch_analytics():
    """Muestra resultados de analítica batch, curated layer y ventanas."""
    # 1. PageRank
    st.markdown('<div class="section-header">📊 PageRank de almacenes — GraphFrames batch</div>', unsafe_allow_html=True)
    df_pr = pd.DataFrame(PAGERANK_DATA).sort_values(by="importance_score", ascending=True)
    fig_pr = px.bar(
        df_pr, x="importance_score", y="warehouse_id", orientation='h',
        color="importance_score", color_continuous_scale=["#005f5f", "#00d4aa"], text="importance_score"
    )
    fig_pr.update_traces(texttemplate='%{text:.3f}', textposition='outside')
    fig_pr.update_layout(
        margin=dict(l=0, r=0, t=0, b=0), height=180,
        paper_bgcolor="#0d1117", plot_bgcolor="#0d1117", font_color="#8b949e",
        coloraxis_showscale=False, xaxis=dict(showgrid=False, zeroline=False, visible=False), yaxis=dict(title="")
    )
    st.plotly_chart(fig_pr, use_container_width=True)

    # 2. Curated
    st.markdown('<div class="section-header">📋 Curated layer — último día procesado</div>', unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    c3, c4 = st.columns(2)
    
    camiones = st.session_state["camiones"]
    activos = [c for c in camiones if c["speed"] > 0]
    pct_ref_activos = sum(1 for c in activos if c["cargo_type"] == "Refrigerado") / len(activos) * 100 if activos else 0
    delay_medio = sum(c["delay_minutes"] for c in camiones) / len(camiones) if camiones else 0

    with c1: st.metric("Total trips", "1,247", "+12% vs ayer")
    with c2: st.metric("Avg delay (curr)", f"{delay_medio:.1f} min")
    with c3: st.metric("% carga refrig.", f"{pct_ref_activos:.0f}%")
    with c4: st.metric("Temp Madrid", "14.2°C", "⛅", delta_color="off")

    # 3. Ventanas 15 min
    st.markdown('<div class="section-header">⏱ Delay stats por almacén — ventanas 15 min</div>', unsafe_allow_html=True)
    hist = st.session_state["delay_historico"]
    ts = st.session_state["timestamps_ventanas"]
    
    fig_line = go.Figure()
    colores = {"W-7": "#00d4aa", "W-6": "#ff6b6b", "W-9": "#ffa94d"}
    for wh in ["W-7", "W-6", "W-9"]:
        fig_line.add_trace(go.Scatter(
            x=ts, y=hist[wh], mode='lines+markers', name=wh, line=dict(color=colores[wh], width=2)
        ))
        
    fig_line.update_layout(
        margin=dict(l=0, r=0, t=0, b=0), height=220,
        paper_bgcolor="#0d1117", plot_bgcolor="#0d1117", font_color="#8b949e",
        xaxis=dict(showgrid=False), yaxis=dict(showgrid=True, gridcolor="#21262d"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    st.plotly_chart(fig_line, use_container_width=True)

def render_estado_servicios():
    """Estado fijo de infra simulada."""
    st.markdown('<div class="section-header">🟢 Estado de la infraestructura</div>', unsafe_allow_html=True)
    html = """
    <table style="width:100%; text-align:left; border-collapse: collapse; font-size: 0.9em; background-color: #161b22; border-radius: 8px;">
        <tr style="border-bottom: 1px solid #21262d;"><th style="padding: 8px;">Servicio</th><th>Puerto</th><th>Estado</th></tr>
        <tr style="border-bottom: 1px solid #21262d;"><td style="padding: 8px;">Kafka (KRaft)</td><td>39093</td><td>🟢 Online</td></tr>
        <tr style="border-bottom: 1px solid #21262d;"><td style="padding: 8px;">NiFi</td><td>18443</td><td>🟢 Online</td></tr>
        <tr style="border-bottom: 1px solid #21262d;"><td style="padding: 8px;">HDFS Namenode</td><td>19870</td><td>🟢 Online</td></tr>
        <tr style="border-bottom: 1px solid #21262d;"><td style="padding: 8px;">YARN ResourceMgr</td><td>18088</td><td>🟢 Online</td></tr>
        <tr style="border-bottom: 1px solid #21262d;"><td style="padding: 8px;">Spark Streaming</td><td>—</td><td>🟢 Running</td></tr>
        <tr style="border-bottom: 1px solid #21262d;"><td style="padding: 8px;">Cassandra</td><td>19042</td><td>🟢 Online</td></tr>
        <tr style="border-bottom: 1px solid #21262d;"><td style="padding: 8px;">Hive Server</td><td>11000</td><td>🟡 Warning</td></tr>
        <tr><td style="padding: 8px;">Airflow</td><td>18082</td><td>🟢 Online</td></tr>
    </table>
    """
    st.markdown(html, unsafe_allow_html=True)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    st.markdown(DARK_CSS, unsafe_allow_html=True)
    init_session_state()

    activa, intervalo, tasa_inv, pct_ref = render_sidebar()
    st.session_state["simulacion_activa"] = activa
    st.session_state["intervalo_refresco"] = intervalo
    st.session_state["tasa_invalidos"] = tasa_inv
    st.session_state["pct_refrigerado"] = pct_ref

    # Header
    col_titulo, col_reloj = st.columns([3, 1])
    with col_titulo:
        st.title("🚛 LogisticsOps — Demo Dashboard")
        st.caption("Arquitectura: Kafka · NiFi · Spark · HDFS · Cassandra · Hive · Airflow")
    with col_reloj:
        st.metric("Hora actual", datetime.now().strftime("%H:%M:%S"))
        if activa:
            st.markdown("🔴 **LIVE**")

    render_metricas_superiores()
    st.divider()

    col_izq, col_centro, col_der = st.columns([1, 1.2, 1])
    with col_izq:
        render_log_eventos()
    with col_centro:
        render_estado_camiones()
        render_mapa_camiones()
    with col_der:
        render_batch_analytics()
        render_estado_servicios()

    # Ciclo de refresco automático
    if activa:
        n_eventos = random.randint(1, 3)
        for _ in range(n_eventos):
            ev = generar_evento(tasa_inv, pct_ref)
            valido = es_valido(ev)
            ev["_valido"] = valido
            ev["_destino"] = "transport_filtered → HDFS" if valido else "DLQ → /data/transport/dlq"
            st.session_state["log_eventos"].appendleft(ev)
            st.session_state["total_eventos"] += 1
            if valido:
                st.session_state["total_validos"] += 1
            else:
                st.session_state["total_dlq"] += 1
        st.session_state["ultimo_evento"] = ev

        indices = random.sample(range(len(st.session_state["camiones"])), 2)
        for i in indices:
            st.session_state["camiones"][i] = actualizar_camion(st.session_state["camiones"][i])

        if random.random() < 0.4:  # Actualización periódica en vez de en cada tick
            for wh in ["W-7", "W-6", "W-9"]:
                nuevo_delay = random.uniform(5, 45)
                st.session_state["delay_historico"][wh].append(nuevo_delay)
                st.session_state["delay_historico"][wh].pop(0)

        time.sleep(intervalo)
        st.rerun()

if __name__ == "__main__":
    main()
