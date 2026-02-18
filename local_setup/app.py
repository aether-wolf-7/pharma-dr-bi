"""
Pharma DR · BI Dashboard — Apache Streamlit
=============================================
15 dashboards for pharmaceutical sales intelligence in República Dominicana.
Powered by DuckDB (no server required).

Run: streamlit run local_setup/app.py
"""

import sys
from pathlib import Path
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

DB_PATH = Path(__file__).parent / "pharma_dr.duckdb"

# ── Page Config ──────────────────────────────────────────────
st.set_page_config(
    page_title="Pharma DR · BI Platform",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Brand CSS ─────────────────────────────────────────────────
st.markdown("""
<style>
    [data-testid="stSidebar"] { background: #1E3A5F; }
    [data-testid="stSidebar"] * { color: #ECEFF4 !important; }
    .metric-card {
        background: #F8F9FA; border-radius: 8px;
        padding: 16px; border-left: 4px solid #2ECC71;
        margin-bottom: 8px;
    }
    .stMetric label { font-size: 0.82rem !important; color: #6c757d !important; }
    .stMetric [data-testid="metric-container"] { background: #F8F9FA; border-radius: 8px; padding: 12px; }
    h1 { color: #1E3A5F; }
    h2 { color: #1E3A5F; border-bottom: 2px solid #2ECC71; padding-bottom: 4px; }
</style>
""", unsafe_allow_html=True)

# ── DuckDB Connection (cached) ────────────────────────────────
@st.cache_resource
def get_conn():
    if not DB_PATH.exists():
        st.error(f"Database not found: {DB_PATH}\n\nRun: python local_setup/db_init.py && python local_setup/load_data.py")
        st.stop()
    return duckdb.connect(str(DB_PATH), read_only=True)

@st.cache_data(ttl=300)
def q(sql: str) -> pd.DataFrame:
    """Execute a SQL query and return a DataFrame (cached 5 min)."""
    con = get_conn()
    return con.execute(sql).df()

# ── Sidebar Navigation ────────────────────────────────────────
DASHBOARDS = {
    "🏠 Resumen General":          "home",
    "🗺️  Mapa de Ventas":           "mapa",
    "🌍 Ventas por Zona":          "zona",
    "👥 Top Clientes":             "clientes",
    "💊 Top Productos":            "productos",
    "💰 Comisiones":               "comisiones",
    "📈 Evolución Mensual":        "evolucion",
    "📊 Comparativo Año Anterior": "yoy",
    "🏭 Ventas por Laboratorio":   "laboratorio",
    "🔄 Distribuidor vs Interno":  "distribuidor",
    "📉 Margen por Producto":      "margen",
    "🎫 Ticket Promedio":          "ticket",
    "🗂️  Ventas por Categoría":    "categoria",
    "🏆 Ranking Vendedores":       "vendedores",
    "🎯 Cumplimiento de Metas":    "metas",
}

with st.sidebar:
    st.markdown("## 💊 Pharma DR")
    st.markdown("### República Dominicana")
    st.markdown("---")
    selected = st.radio("Dashboards", list(DASHBOARDS.keys()), label_visibility="collapsed")
    dashboard = DASHBOARDS[selected]
    st.markdown("---")

    # ── Global Filters ────────────────────────────────────────
    st.markdown("### 🔍 Filtros Globales")
    years_df = q("SELECT DISTINCT year FROM fact_sales ORDER BY year")
    all_years = years_df["year"].tolist()
    sel_years = st.multiselect("Año", all_years, default=all_years, key="fil_year")

    zones_df = q("SELECT zone_name FROM dim_zone ORDER BY zone_name")
    all_zones = zones_df["zone_name"].tolist()
    sel_zones = st.multiselect("Zona", all_zones, default=all_zones, key="fil_zone")

    cats_df = q("SELECT DISTINCT category FROM dim_product ORDER BY category")
    all_cats = cats_df["category"].tolist()
    sel_cats = st.multiselect("Categoría", all_cats, default=all_cats, key="fil_cat")

    dist_types = ["INTERNO", "EXTERNO"]
    sel_dist = st.multiselect("Tipo Distribuidor", dist_types, default=dist_types, key="fil_dist")

year_filter = tuple(sel_years) if sel_years else tuple(all_years)
zone_filter = tuple(sel_zones) if sel_zones else tuple(all_zones)
cat_filter  = tuple(sel_cats)  if sel_cats  else tuple(all_cats)
dist_filter = tuple(sel_dist)  if sel_dist  else tuple(dist_types)

def where_clause(table_alias="s", product_alias="p", zone_alias="z", dist_alias="d"):
    y = ",".join(str(y) for y in year_filter) or "0"
    zo = "','".join(zone_filter) or "''"
    ca = "','".join(cat_filter)  or "''"
    dt = "','".join(dist_filter) or "''"
    return f"""
        {table_alias}.year IN ({y})
        AND {zone_alias}.zone_name IN ('{zo}')
        AND {product_alias}.category IN ('{ca}')
        AND {dist_alias}.distributor_type IN ('{dt}')
    """

COLORS = px.colors.qualitative.Bold
ZONE_COLORS = {"Capital":"#2ECC71","Norte":"#3498DB","Este":"#E74C3C","Sur":"#F39C12","Oeste":"#9B59B6"}

fmt_rd = lambda v: f"RD$ {v:,.0f}"

# ════════════════════════════════════════════════════════════════
# DASHBOARD: HOME — Resumen General (Dashboard 15 equivalent)
# ════════════════════════════════════════════════════════════════
if dashboard == "home":
    st.title("💊 Pharma DR · Plataforma de Inteligencia Comercial")
    st.markdown("**República Dominicana · Industria Farmacéutica · 2021–2024**")

    # KPI row
    kpi = q(f"""
        SELECT
            ROUND(SUM(s.net_amount),0)           AS total_net,
            ROUND(AVG(s.margin_pct),2)            AS avg_margin,
            COUNT(DISTINCT s.invoice_number)      AS invoices,
            COUNT(DISTINCT s.client_key)          AS clients,
            ROUND(SUM(s.net_amount)/
                  NULLIF(COUNT(DISTINCT s.invoice_number),0),0) AS avg_ticket
        FROM fact_sales s
        JOIN dim_zone z ON z.zone_key=s.zone_key
        JOIN dim_product p ON p.product_key=s.product_key
        JOIN dim_distributor d ON d.distributor_key=s.distributor_key
        WHERE {where_clause()}
    """)
    r = kpi.iloc[0]
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("💵 Ventas Netas",    fmt_rd(r.total_net))
    c2.metric("📊 Margen Promedio", f"{r.avg_margin:.1f}%")
    c3.metric("🧾 Facturas",        f"{int(r.invoices):,}")
    c4.metric("👥 Clientes Activos",f"{int(r.clients):,}")
    c5.metric("🎫 Ticket Promedio", fmt_rd(r.avg_ticket))

    st.markdown("---")
    col_l, col_r = st.columns([2,1])

    with col_l:
        st.subheader("Evolución Mensual de Ventas por Zona")
        trend = q(f"""
            SELECT s.year_month, z.zone_name, ROUND(SUM(s.net_amount),0) net
            FROM fact_sales s
            JOIN dim_zone z ON z.zone_key=s.zone_key
            JOIN dim_product p ON p.product_key=s.product_key
            JOIN dim_distributor d ON d.distributor_key=s.distributor_key
            WHERE {where_clause()}
            GROUP BY s.year_month, z.zone_name ORDER BY s.year_month
        """)
        fig = px.area(trend, x="year_month", y="net", color="zone_name",
                      color_discrete_map=ZONE_COLORS,
                      labels={"year_month":"Mes","net":"Ventas Netas (RD$)","zone_name":"Zona"},
                      template="plotly_white")
        fig.update_layout(height=320, margin=dict(t=10,b=40), legend_title="Zona")
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Participación por Zona")
        zone_pie = q(f"""
            SELECT z.zone_name, ROUND(SUM(s.net_amount),0) net
            FROM fact_sales s
            JOIN dim_zone z ON z.zone_key=s.zone_key
            JOIN dim_product p ON p.product_key=s.product_key
            JOIN dim_distributor d ON d.distributor_key=s.distributor_key
            WHERE {where_clause()}
            GROUP BY z.zone_name ORDER BY net DESC
        """)
        fig2 = px.pie(zone_pie, values="net", names="zone_name",
                      color="zone_name", color_discrete_map=ZONE_COLORS,
                      hole=0.45, template="plotly_white")
        fig2.update_layout(height=320, margin=dict(t=10,b=10), showlegend=True)
        st.plotly_chart(fig2, use_container_width=True)

    col3, col4 = st.columns(2)
    with col3:
        st.subheader("Top 10 Productos")
        tp = q(f"""
            SELECT p.product_name, ROUND(SUM(s.net_amount),0) net
            FROM fact_sales s
            JOIN dim_product p ON p.product_key=s.product_key
            JOIN dim_zone z ON z.zone_key=s.zone_key
            JOIN dim_distributor d ON d.distributor_key=s.distributor_key
            WHERE {where_clause()}
            GROUP BY p.product_name ORDER BY net DESC LIMIT 10
        """)
        fig3 = px.bar(tp, x="net", y="product_name", orientation="h",
                      color="net", color_continuous_scale="Greens",
                      labels={"net":"Ventas (RD$)","product_name":""},
                      template="plotly_white")
        fig3.update_layout(height=340, margin=dict(t=10,b=10), coloraxis_showscale=False, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig3, use_container_width=True)

    with col4:
        st.subheader("Ventas por Categoría")
        cat_df = q(f"""
            SELECT p.category, ROUND(SUM(s.net_amount),0) net
            FROM fact_sales s
            JOIN dim_product p ON p.product_key=s.product_key
            JOIN dim_zone z ON z.zone_key=s.zone_key
            JOIN dim_distributor d ON d.distributor_key=s.distributor_key
            WHERE {where_clause()}
            GROUP BY p.category ORDER BY net DESC
        """)
        fig4 = px.treemap(cat_df, path=["category"], values="net",
                          color="net", color_continuous_scale="Blues",
                          template="plotly_white")
        fig4.update_layout(height=340, margin=dict(t=10,b=10))
        st.plotly_chart(fig4, use_container_width=True)

# ════════════════════════════════════════════════════════════════
# DASHBOARD 1: MAPA DE VENTAS
# ════════════════════════════════════════════════════════════════
elif dashboard == "mapa":
    st.title("🗺️ Mapa de Ventas — República Dominicana")

    city_df = q(f"""
        SELECT c.city_name, c.latitude, c.longitude, z.zone_name,
               ROUND(SUM(s.net_amount),0) AS net_amount,
               COUNT(DISTINCT s.invoice_number) AS invoices,
               COUNT(DISTINCT s.client_key) AS clients
        FROM fact_sales s
        JOIN dim_city c ON c.city_key=s.city_key
        JOIN dim_zone z ON z.zone_key=s.zone_key
        JOIN dim_product p ON p.product_key=s.product_key
        JOIN dim_distributor d ON d.distributor_key=s.distributor_key
        WHERE {where_clause()} AND c.latitude IS NOT NULL
        GROUP BY c.city_name, c.latitude, c.longitude, z.zone_name
        ORDER BY net_amount DESC
    """)

    col1, col2 = st.columns([3,1])
    with col1:
        st.subheader("Ventas por Ciudad (Tamaño = Volumen de Ventas)")
        fig = px.scatter_mapbox(
            city_df, lat="latitude", lon="longitude",
            size="net_amount", color="zone_name",
            hover_name="city_name",
            hover_data={"net_amount":":,.0f","invoices":True,"clients":True,"latitude":False,"longitude":False},
            color_discrete_map=ZONE_COLORS,
            size_max=50, zoom=6.5,
            center={"lat":18.7,"lon":-70.1},
            mapbox_style="open-street-map",
            template="plotly_white",
            labels={"net_amount":"Ventas (RD$)","zone_name":"Zona"},
        )
        fig.update_layout(height=520, margin=dict(t=10,b=10))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.subheader("Top 10 Ciudades")
        top10 = city_df.nlargest(10, "net_amount")[["city_name","net_amount","zone_name"]]
        top10["net_amount"] = top10["net_amount"].apply(lambda x: f"RD$ {x:,.0f}")
        top10.columns = ["Ciudad","Ventas","Zona"]
        st.dataframe(top10, use_container_width=True, hide_index=True)

        st.subheader("Por Zona")
        zbar = city_df.groupby("zone_name")["net_amount"].sum().reset_index().sort_values("net_amount",ascending=True)
        fig2 = px.bar(zbar, x="net_amount", y="zone_name", orientation="h",
                      color="zone_name", color_discrete_map=ZONE_COLORS,
                      template="plotly_white", labels={"net_amount":"RD$","zone_name":""})
        fig2.update_layout(height=200, margin=dict(t=5,b=5), showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

# ════════════════════════════════════════════════════════════════
# DASHBOARD 2: VENTAS POR ZONA
# ════════════════════════════════════════════════════════════════
elif dashboard == "zona":
    st.title("🌍 Ventas por Zona")

    monthly = q(f"""
        SELECT s.year_month, s.year, s.month_num, z.zone_name,
               ROUND(SUM(s.net_amount),0) net,
               COUNT(DISTINCT s.invoice_number) invoices,
               COUNT(DISTINCT s.client_key) clients,
               ROUND(AVG(s.margin_pct),2) margin_pct
        FROM fact_sales s
        JOIN dim_zone z ON z.zone_key=s.zone_key
        JOIN dim_product p ON p.product_key=s.product_key
        JOIN dim_distributor d ON d.distributor_key=s.distributor_key
        WHERE {where_clause()}
        GROUP BY s.year_month, s.year, s.month_num, z.zone_name
        ORDER BY s.year_month
    """)

    # KPI tiles per zone
    zone_kpi = monthly.groupby("zone_name").agg(net=("net","sum"), clients=("clients","sum")).reset_index()
    cols = st.columns(len(zone_kpi))
    for i, (_, row) in enumerate(zone_kpi.iterrows()):
        with cols[i]:
            st.metric(f"🌍 {row.zone_name}", fmt_rd(row.net), f"{row.clients:,.0f} clientes")

    st.markdown("---")
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Ventas Mensuales por Zona (Apilado)")
        fig = px.bar(monthly, x="year_month", y="net", color="zone_name",
                     color_discrete_map=ZONE_COLORS, template="plotly_white",
                     labels={"year_month":"Mes","net":"Ventas (RD$)","zone_name":"Zona"})
        fig.update_layout(height=360, margin=dict(t=10,b=40), barmode="stack")
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader("Distribución de Zonas por Año")
        yearly_zone = monthly.groupby(["year","zone_name"])["net"].sum().reset_index()
        fig2 = px.bar(yearly_zone, x="year", y="net", color="zone_name",
                      color_discrete_map=ZONE_COLORS, barmode="group",
                      template="plotly_white",
                      labels={"year":"Año","net":"Ventas (RD$)","zone_name":"Zona"})
        fig2.update_layout(height=360, margin=dict(t=10,b=40))
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("KPIs por Zona y Mes")
    pivot = monthly.pivot_table(index="zone_name", columns="year_month", values="net", aggfunc="sum")
    pivot = pivot.fillna(0)
    st.dataframe(pivot.style.format("{:,.0f}"), use_container_width=True)

# ════════════════════════════════════════════════════════════════
# DASHBOARD 3: TOP CLIENTES
# ════════════════════════════════════════════════════════════════
elif dashboard == "clientes":
    st.title("👥 Top Clientes")

    clients_df = q(f"""
        SELECT cl.client_name, cl.client_type, z.zone_name, c.city_name,
               ROUND(SUM(s.net_amount),0) net,
               COUNT(DISTINCT s.invoice_number) invoices,
               ROUND(SUM(s.net_amount)/NULLIF(COUNT(DISTINCT s.invoice_number),0),0) avg_ticket,
               ROUND(AVG(s.margin_pct),2) margin_pct
        FROM fact_sales s
        JOIN dim_client cl ON cl.client_key=s.client_key
        JOIN dim_zone z ON z.zone_key=s.zone_key
        JOIN dim_city c ON c.city_key=s.city_key
        JOIN dim_product p ON p.product_key=s.product_key
        JOIN dim_distributor d ON d.distributor_key=s.distributor_key
        WHERE {where_clause()}
        GROUP BY cl.client_name, cl.client_type, z.zone_name, c.city_name
        ORDER BY net DESC
    """)

    clients_df["rank"] = range(1, len(clients_df)+1)

    c1,c2,c3 = st.columns(3)
    c1.metric("🏆 Clientes Totales", f"{len(clients_df):,}")
    c2.metric("💵 Top Cliente",     clients_df.iloc[0]["client_name"])
    c3.metric("📦 Ventas Top Cliente", fmt_rd(clients_df.iloc[0]["net"]))

    col_l, col_r = st.columns([2,1])
    with col_l:
        st.subheader("Top 20 Clientes por Ventas Netas")
        top20 = clients_df.head(20)
        fig = px.bar(top20, x="net", y="client_name", orientation="h",
                     color="zone_name", color_discrete_map=ZONE_COLORS,
                     hover_data={"client_type":True,"invoices":True,"avg_ticket":":,.0f"},
                     template="plotly_white",
                     labels={"net":"Ventas (RD$)","client_name":"","zone_name":"Zona"})
        fig.update_layout(height=480, margin=dict(t=10,b=10), yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Tipo de Cliente")
        type_df = clients_df.groupby("client_type")["net"].sum().reset_index()
        fig2 = px.pie(type_df, values="net", names="client_type", hole=0.4,
                      template="plotly_white", color_discrete_sequence=COLORS)
        fig2.update_layout(height=260, margin=dict(t=10,b=10))
        st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Scatter: Ventas vs Ticket")
        fig3 = px.scatter(clients_df.head(50), x="avg_ticket", y="net",
                          size="invoices", color="client_type",
                          hover_name="client_name",
                          template="plotly_white",
                          labels={"avg_ticket":"Ticket Prom (RD$)","net":"Ventas (RD$)"},
                          color_discrete_sequence=COLORS)
        fig3.update_layout(height=260, margin=dict(t=10,b=10), showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)

    st.subheader("Tabla — Top 50 Clientes")
    display = clients_df.head(50)[["rank","client_name","client_type","zone_name","city_name","net","avg_ticket","invoices","margin_pct"]].copy()
    display.columns = ["#","Cliente","Tipo","Zona","Ciudad","Ventas Netas","Ticket Prom","Facturas","Margen %"]
    display["Ventas Netas"] = display["Ventas Netas"].apply(lambda x: f"RD$ {x:,.0f}")
    display["Ticket Prom"]  = display["Ticket Prom"].apply(lambda x: f"RD$ {x:,.0f}")
    st.dataframe(display, use_container_width=True, hide_index=True)

# ════════════════════════════════════════════════════════════════
# DASHBOARD 4: TOP PRODUCTOS
# ════════════════════════════════════════════════════════════════
elif dashboard == "productos":
    st.title("💊 Top Productos Farmacéuticos")

    prods = q(f"""
        SELECT p.product_name, p.category, p.rx_otc_flag, l.lab_name,
               ROUND(SUM(s.net_amount),0) net,
               SUM(s.quantity) qty,
               ROUND(AVG(s.margin_pct),2) margin_pct,
               ROUND(AVG(s.unit_price),2) avg_price
        FROM fact_sales s
        JOIN dim_product p ON p.product_key=s.product_key
        JOIN dim_laboratory l ON l.lab_key=s.laboratory_key
        JOIN dim_zone z ON z.zone_key=s.zone_key
        JOIN dim_distributor d ON d.distributor_key=s.distributor_key
        WHERE {where_clause()}
        GROUP BY p.product_name, p.category, p.rx_otc_flag, l.lab_name
        ORDER BY net DESC
    """)

    c1,c2,c3 = st.columns(3)
    c1.metric("💊 Top Producto",  prods.iloc[0]["product_name"])
    c2.metric("💵 Ventas",        fmt_rd(prods.iloc[0]["net"]))
    c3.metric("📊 Margen",        f"{prods.iloc[0]['margin_pct']:.1f}%")

    col_l, col_r = st.columns([1,1])
    with col_l:
        st.subheader("Treemap — Categoría → Producto")
        fig = px.treemap(prods, path=[px.Constant("Todos"), "category", "product_name"],
                         values="net", color="margin_pct",
                         color_continuous_scale="RdYlGn",
                         color_continuous_midpoint=30,
                         template="plotly_white",
                         labels={"net":"Ventas","margin_pct":"Margen %"})
        fig.update_layout(height=440, margin=dict(t=10,b=10))
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Top 15 Productos por Ventas")
        fig2 = px.bar(prods.head(15), x="net", y="product_name", orientation="h",
                      color="category", template="plotly_white",
                      color_discrete_sequence=COLORS,
                      labels={"net":"Ventas (RD$)","product_name":"","category":"Categoría"})
        fig2.update_layout(height=440, margin=dict(t=10,b=10), yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("RX vs OTC")
    rx_otc = prods.groupby("rx_otc_flag")["net"].sum().reset_index()
    c1, c2 = st.columns(2)
    with c1:
        fig3 = px.pie(rx_otc, values="net", names="rx_otc_flag", hole=0.45,
                      color_discrete_sequence=["#2ECC71","#E74C3C"],
                      template="plotly_white")
        fig3.update_layout(height=240, margin=dict(t=10,b=10))
        st.plotly_chart(fig3, use_container_width=True)
    with c2:
        tbl20 = prods.head(20)[["product_name","category","lab_name","net","qty","margin_pct","avg_price"]].copy()
        tbl20 = tbl20.rename(columns={"product_name":"Producto","category":"Categoría","lab_name":"Lab",
                                      "net":"Ventas","qty":"Unidades","margin_pct":"Margen %","avg_price":"P. Prom"})
        tbl20["Ventas"]   = tbl20["Ventas"].apply(lambda x: f"RD$ {x:,.0f}")
        tbl20["Margen %"] = tbl20["Margen %"].apply(lambda x: f"{x:.1f}")
        tbl20["P. Prom"]  = tbl20["P. Prom"].apply(lambda x: f"{x:.2f}")
        st.dataframe(tbl20, use_container_width=True, hide_index=True)

# ════════════════════════════════════════════════════════════════
# DASHBOARD 5: COMISIONES
# ════════════════════════════════════════════════════════════════
elif dashboard == "comisiones":
    st.title("💰 Comisiones de Vendedores")

    _years_csv = ",".join(str(y) for y in year_filter) or "0"
    comm = q(f"""
        SELECT fc.year_month, sp.full_name, z.zone_name,
               fc.sales_amount, fc.target_amount, fc.achievement_pct,
               fc.commission_rate*100 AS comm_rate_pct,
               fc.commission_amount, fc.bonus_amount, fc.total_payout
        FROM fact_commission fc
        JOIN dim_salesperson sp ON sp.salesperson_key=fc.salesperson_key
        JOIN dim_zone z ON z.zone_key=fc.zone_key
        WHERE CAST(SPLIT_PART(fc.year_month, '-', 1) AS INTEGER) IN ({_years_csv})
        AND z.zone_name IN ('{"','".join(zone_filter)}')
        ORDER BY fc.year_month DESC, fc.sales_amount DESC
    """)

    if comm.empty:
        st.warning("No hay datos de comisiones para los filtros seleccionados.")
    else:
        total_payout = comm["total_payout"].sum()
        top_sp = comm.groupby("full_name")["sales_amount"].sum().idxmax()
        c1,c2,c3 = st.columns(3)
        c1.metric("💰 Total Pagado", fmt_rd(total_payout))
        c2.metric("🏆 Top Vendedor", top_sp)
        c3.metric("📊 Logro Prom.",  f"{comm['achievement_pct'].mean():.1f}%")

        col_l, col_r = st.columns([3,2])
        with col_l:
            st.subheader("Logro de Metas por Vendedor (Último Mes)")
            last_month = comm["year_month"].max()
            lm_df = comm[comm["year_month"]==last_month].sort_values("achievement_pct", ascending=True)
            colors = ["#E74C3C" if v < 80 else "#F39C12" if v < 100 else "#2ECC71" for v in lm_df["achievement_pct"]]
            fig = go.Figure(go.Bar(
                x=lm_df["achievement_pct"], y=lm_df["full_name"],
                orientation="h", marker_color=colors,
                text=[f"{v:.1f}%" for v in lm_df["achievement_pct"]], textposition="auto",
            ))
            fig.add_vline(x=100, line_dash="dash", line_color="#2ECC71", annotation_text="Meta 100%")
            fig.update_layout(height=420, margin=dict(t=10,b=10), xaxis_title="Logro %",
                              plot_bgcolor="white", paper_bgcolor="white", yaxis_title="")
            st.plotly_chart(fig, use_container_width=True)

        with col_r:
            st.subheader(f"Comisiones — {last_month}")
            lm_tbl = lm_df[["full_name","sales_amount","achievement_pct","total_payout"]].copy()
            lm_tbl.columns = ["Vendedor","Ventas","Logro %","Comisión"]
            lm_tbl["Ventas"]   = lm_tbl["Ventas"].apply(lambda x: f"RD$ {x:,.0f}")
            lm_tbl["Comisión"] = lm_tbl["Comisión"].apply(lambda x: f"RD$ {x:,.0f}")
            lm_tbl["Logro %"]  = lm_tbl["Logro %"].apply(lambda x: f"{x:.1f}%")
            st.dataframe(lm_tbl, use_container_width=True, hide_index=True)

        st.subheader("Evolución de Comisiones por Vendedor")
        sel_sp = st.selectbox("Vendedor:", sorted(comm["full_name"].unique()))
        sp_trend = comm[comm["full_name"]==sel_sp].sort_values("year_month")
        fig2 = make_subplots(specs=[[{"secondary_y": True}]])
        fig2.add_trace(go.Bar(x=sp_trend["year_month"], y=sp_trend["sales_amount"], name="Ventas", marker_color="#3498DB"), secondary_y=False)
        fig2.add_trace(go.Scatter(x=sp_trend["year_month"], y=sp_trend["achievement_pct"], name="Logro %", mode="lines+markers", line=dict(color="#2ECC71", width=2)), secondary_y=True)
        fig2.add_hline(y=800000, line_dash="dot", line_color="#E74C3C", secondary_y=False, annotation_text="Meta Ventas")
        fig2.update_yaxes(title_text="Ventas (RD$)", secondary_y=False)
        fig2.update_yaxes(title_text="Logro (%)", secondary_y=True)
        fig2.update_layout(height=300, margin=dict(t=10,b=40), plot_bgcolor="white", paper_bgcolor="white")
        st.plotly_chart(fig2, use_container_width=True)

# ════════════════════════════════════════════════════════════════
# DASHBOARD 6: EVOLUCIÓN MENSUAL
# ════════════════════════════════════════════════════════════════
elif dashboard == "evolucion":
    st.title("📈 Evolución Mensual de Ventas")

    monthly = q(f"""
        SELECT s.year_month, s.year, s.month_num, z.zone_name,
               ROUND(SUM(s.net_amount),0) net,
               COUNT(DISTINCT s.invoice_number) invoices,
               ROUND(SUM(s.net_amount)/NULLIF(COUNT(DISTINCT s.invoice_number),0),0) avg_ticket,
               ROUND(AVG(s.margin_pct),2) margin_pct
        FROM fact_sales s
        JOIN dim_zone z ON z.zone_key=s.zone_key
        JOIN dim_product p ON p.product_key=s.product_key
        JOIN dim_distributor d ON d.distributor_key=s.distributor_key
        WHERE {where_clause()}
        GROUP BY s.year_month, s.year, s.month_num, z.zone_name
        ORDER BY s.year_month
    """)

    total = monthly.groupby("year_month").agg(net=("net","sum"), invoices=("invoices","sum"), avg_ticket=("avg_ticket","mean")).reset_index()

    st.subheader("Ventas Netas Totales + Facturas")
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(x=total["year_month"], y=total["net"], mode="lines+markers",
                             name="Ventas Netas", line=dict(color="#1E3A5F", width=2.5),
                             fill="tozeroy", fillcolor="rgba(30,58,95,0.1)"), secondary_y=False)
    fig.add_trace(go.Bar(x=total["year_month"], y=total["invoices"], name="Facturas",
                         marker_color="#2ECC71", opacity=0.5), secondary_y=True)
    fig.update_yaxes(title_text="Ventas (RD$)", secondary_y=False)
    fig.update_yaxes(title_text="# Facturas", secondary_y=True)
    fig.update_layout(height=360, margin=dict(t=10,b=40), plot_bgcolor="white", paper_bgcolor="white")
    st.plotly_chart(fig, use_container_width=True)

    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader("Ventas por Zona — Tendencia")
        fig2 = px.line(monthly, x="year_month", y="net", color="zone_name",
                       color_discrete_map=ZONE_COLORS, markers=True,
                       template="plotly_white",
                       labels={"year_month":"Mes","net":"Ventas (RD$)","zone_name":"Zona"})
        fig2.update_layout(height=300, margin=dict(t=10,b=40))
        st.plotly_chart(fig2, use_container_width=True)

    with col_r:
        st.subheader("Ticket Promedio por Mes")
        fig3 = px.line(total, x="year_month", y="avg_ticket", markers=True,
                       template="plotly_white", line_shape="spline",
                       color_discrete_sequence=["#E74C3C"],
                       labels={"year_month":"Mes","avg_ticket":"Ticket Prom (RD$)"})
        fig3.update_layout(height=300, margin=dict(t=10,b=40))
        st.plotly_chart(fig3, use_container_width=True)

# ════════════════════════════════════════════════════════════════
# DASHBOARD 7: COMPARATIVO AÑO ANTERIOR (YoY)
# ════════════════════════════════════════════════════════════════
elif dashboard == "yoy":
    st.title("📊 Comparativo Año Anterior (YoY)")

    yoy = q(f"""
        WITH base AS (
            SELECT s.year, s.month_num, s.month_name,
                   ROUND(SUM(s.net_amount),0) net
            FROM fact_sales s
            JOIN dim_zone z ON z.zone_key=s.zone_key
            JOIN dim_product p ON p.product_key=s.product_key
            JOIN dim_distributor d ON d.distributor_key=s.distributor_key
            WHERE {where_clause()}
            GROUP BY s.year, s.month_num, s.month_name
        )
        SELECT cur.year, cur.month_num, cur.month_name,
               cur.net AS current_sales,
               prev.net AS prior_year_sales,
               ROUND(cur.net - COALESCE(prev.net,0), 0) AS variance,
               ROUND(CASE WHEN COALESCE(prev.net,0)>0
                          THEN (cur.net - prev.net)/prev.net*100
                          ELSE NULL END, 2) AS growth_pct
        FROM base cur
        LEFT JOIN base prev ON prev.year=cur.year-1 AND prev.month_num=cur.month_num
        ORDER BY cur.year, cur.month_num
    """)

    if yoy.empty:
        st.warning("Selecciona al menos 2 años para ver comparativo.")
    else:
        latest_year = int(yoy["year"].max())
        ly_data = yoy[yoy["year"]==latest_year]
        yoy_total = ly_data["current_sales"].sum()
        prev_total = ly_data["prior_year_sales"].sum()
        growth = (yoy_total - prev_total) / prev_total * 100 if prev_total else 0

        c1,c2,c3 = st.columns(3)
        c1.metric(f"💵 Ventas {latest_year}", fmt_rd(yoy_total))
        c2.metric(f"💵 Ventas {latest_year-1}", fmt_rd(prev_total) if prev_total else "N/A")
        c3.metric("📈 Crecimiento YoY", f"{growth:+.1f}%", delta_color="normal")

        st.subheader(f"Ventas Mensuales: {latest_year} vs {latest_year-1}")
        fig = go.Figure()
        fig.add_trace(go.Bar(x=ly_data["month_name"], y=ly_data["prior_year_sales"],
                             name=str(latest_year-1), marker_color="#95A5A6"))
        fig.add_trace(go.Bar(x=ly_data["month_name"], y=ly_data["current_sales"],
                             name=str(latest_year), marker_color="#2ECC71"))
        fig.update_layout(barmode="group", template="plotly_white", height=360,
                          margin=dict(t=10,b=40), xaxis_title="Mes", yaxis_title="Ventas (RD$)")
        st.plotly_chart(fig, use_container_width=True)

        col_l, col_r = st.columns(2)
        with col_l:
            st.subheader("Crecimiento YoY (%) por Mes")
            fig2 = px.bar(ly_data, x="month_name", y="growth_pct",
                          color="growth_pct", color_continuous_scale="RdYlGn",
                          color_continuous_midpoint=0, template="plotly_white",
                          labels={"month_name":"Mes","growth_pct":"Crecimiento %"})
            fig2.add_hline(y=0, line_dash="dash", line_color="gray")
            fig2.update_layout(height=300, margin=dict(t=10,b=40), coloraxis_showscale=False)
            st.plotly_chart(fig2, use_container_width=True)

        with col_r:
            st.subheader("Tabla Comparativa")
            tbl = ly_data[["month_name","current_sales","prior_year_sales","variance","growth_pct"]].copy()
            tbl.columns = ["Mes","Año Actual","Año Anterior","Varianza","Crecimiento %"]
            for c in ["Año Actual","Año Anterior","Varianza"]:
                tbl[c] = tbl[c].apply(lambda x: f"RD$ {x:,.0f}" if pd.notna(x) else "N/A")
            tbl["Crecimiento %"] = tbl["Crecimiento %"].apply(lambda x: f"{x:+.1f}%" if pd.notna(x) else "N/A")
            st.dataframe(tbl, use_container_width=True, hide_index=True)

# ════════════════════════════════════════════════════════════════
# DASHBOARD 8: VENTAS POR LABORATORIO
# ════════════════════════════════════════════════════════════════
elif dashboard == "laboratorio":
    st.title("🏭 Ventas por Laboratorio")

    labs = q(f"""
        SELECT l.lab_name, l.lab_country, l.lab_type,
               s.year_month, s.year,
               ROUND(SUM(s.net_amount),0) net,
               SUM(s.quantity) qty,
               ROUND(AVG(s.margin_pct),2) margin_pct
        FROM fact_sales s
        JOIN dim_laboratory l ON l.lab_key=s.laboratory_key
        JOIN dim_zone z ON z.zone_key=s.zone_key
        JOIN dim_product p ON p.product_key=s.product_key
        JOIN dim_distributor d ON d.distributor_key=s.distributor_key
        WHERE {where_clause()}
        GROUP BY l.lab_name, l.lab_country, l.lab_type, s.year_month, s.year
        ORDER BY net DESC
    """)

    lab_total = labs.groupby(["lab_name","lab_country","lab_type"]).agg(net=("net","sum"), margin_pct=("margin_pct","mean")).reset_index().sort_values("net",ascending=False)

    col_l, col_r = st.columns([1,1])
    with col_l:
        st.subheader("Participación por Laboratorio")
        fig = px.pie(lab_total.head(12), values="net", names="lab_name",
                     hole=0.4, template="plotly_white", color_discrete_sequence=COLORS)
        fig.update_layout(height=380, margin=dict(t=10,b=10))
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Ventas vs Margen por Lab")
        fig2 = px.scatter(lab_total, x="margin_pct", y="net", size="net",
                          color="lab_type", text="lab_name",
                          template="plotly_white", color_discrete_sequence=COLORS,
                          labels={"margin_pct":"Margen %","net":"Ventas (RD$)","lab_type":"Tipo"})
        fig2.update_traces(textposition="top center")
        fig2.update_layout(height=380, margin=dict(t=10,b=10))
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Evolución Mensual — Top 6 Laboratorios")
    top6_labs = lab_total.head(6)["lab_name"].tolist()
    trend = labs[labs["lab_name"].isin(top6_labs)].groupby(["year_month","lab_name"])["net"].sum().reset_index()
    fig3 = px.line(trend, x="year_month", y="net", color="lab_name", markers=True,
                   template="plotly_white", color_discrete_sequence=COLORS,
                   labels={"year_month":"Mes","net":"Ventas (RD$)","lab_name":"Laboratorio"})
    fig3.update_layout(height=300, margin=dict(t=10,b=40))
    st.plotly_chart(fig3, use_container_width=True)

# ════════════════════════════════════════════════════════════════
# DASHBOARD 9: DISTRIBUIDOR VS INTERNO
# ════════════════════════════════════════════════════════════════
elif dashboard == "distribuidor":
    st.title("🔄 Distribuidor Externo vs Ventas Internas")

    dist_df = q(f"""
        SELECT d.distributor_name, d.distributor_type, s.year_month, s.year,
               ROUND(SUM(s.net_amount),0) net,
               COUNT(DISTINCT s.invoice_number) invoices,
               COUNT(DISTINCT s.client_key) clients,
               ROUND(AVG(s.margin_pct),2) margin_pct
        FROM fact_sales s
        JOIN dim_distributor d ON d.distributor_key=s.distributor_key
        JOIN dim_zone z ON z.zone_key=s.zone_key
        JOIN dim_product p ON p.product_key=s.product_key
        WHERE {where_clause()}
        GROUP BY d.distributor_name, d.distributor_type, s.year_month, s.year
        ORDER BY net DESC
    """)

    type_total = dist_df.groupby(["distributor_type","year_month"])["net"].sum().reset_index()

    c1,c2 = st.columns(2)
    with c1:
        st.subheader("Interno vs Externo — Tendencia Mensual")
        fig = px.area(type_total, x="year_month", y="net", color="distributor_type",
                      color_discrete_map={"INTERNO":"#2ECC71","EXTERNO":"#3498DB"},
                      template="plotly_white",
                      labels={"year_month":"Mes","net":"Ventas (RD$)","distributor_type":"Tipo"})
        fig.update_layout(height=320, margin=dict(t=10,b=40))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader("Participación por Distribuidor")
        dist_pie = dist_df.groupby("distributor_name")["net"].sum().reset_index()
        fig2 = px.pie(dist_pie, values="net", names="distributor_name", hole=0.4,
                      template="plotly_white", color_discrete_sequence=COLORS)
        fig2.update_layout(height=320, margin=dict(t=10,b=10))
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("KPIs por Distribuidor")
    kpi_tbl = dist_df.groupby(["distributor_name","distributor_type"]).agg(
        net=("net","sum"), invoices=("invoices","sum"),
        clients=("clients","sum"), margin_pct=("margin_pct","mean")
    ).reset_index().sort_values("net",ascending=False)
    kpi_tbl["net"] = kpi_tbl["net"].apply(lambda x: f"RD$ {x:,.0f}")
    kpi_tbl.columns = ["Distribuidor","Tipo","Ventas Netas","Facturas","Clientes","Margen %"]
    st.dataframe(kpi_tbl, use_container_width=True, hide_index=True)

# ════════════════════════════════════════════════════════════════
# DASHBOARD 10: MARGEN POR PRODUCTO
# ════════════════════════════════════════════════════════════════
elif dashboard == "margen":
    st.title("📉 Margen por Producto")

    margin_df = q(f"""
        SELECT p.product_name, p.category, l.lab_name,
               ROUND(SUM(s.net_amount),0) net,
               ROUND(SUM(s.cost_amount),0) cost_total,
               ROUND(SUM(s.margin_amount),0) margin_total,
               ROUND(AVG(s.margin_pct),2) margin_pct,
               ROUND(AVG(s.unit_price),2) avg_price
        FROM fact_sales s
        JOIN dim_product p ON p.product_key=s.product_key
        JOIN dim_laboratory l ON l.lab_key=s.laboratory_key
        JOIN dim_zone z ON z.zone_key=s.zone_key
        JOIN dim_distributor d ON d.distributor_key=s.distributor_key
        WHERE {where_clause()}
        GROUP BY p.product_name, p.category, l.lab_name
        ORDER BY margin_pct DESC
    """)

    c1,c2,c3 = st.columns(3)
    c1.metric("📈 Mayor Margen", f"{margin_df.iloc[0]['product_name'][:30]}", f"{margin_df.iloc[0]['margin_pct']:.1f}%")
    c2.metric("📉 Menor Margen", f"{margin_df.iloc[-1]['product_name'][:30]}", f"{margin_df.iloc[-1]['margin_pct']:.1f}%")
    c3.metric("📊 Margen Prom.",  f"{margin_df['margin_pct'].mean():.1f}%")

    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader("Scatter: Precio vs Margen %")
        fig = px.scatter(margin_df, x="avg_price", y="margin_pct", size="net",
                         color="category", hover_name="product_name",
                         template="plotly_white", color_discrete_sequence=COLORS,
                         labels={"avg_price":"Precio Prom (RD$)","margin_pct":"Margen %","category":"Categoría"})
        fig.update_layout(height=400, margin=dict(t=10,b=10))
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Top 15 — Mejor Margen %")
        top15 = margin_df.head(15)
        fig2 = px.bar(top15, x="margin_pct", y="product_name", orientation="h",
                      color="margin_pct", color_continuous_scale="RdYlGn",
                      template="plotly_white",
                      labels={"margin_pct":"Margen %","product_name":""})
        fig2.update_layout(height=400, margin=dict(t=10,b=10), yaxis=dict(autorange="reversed"), coloraxis_showscale=False)
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Heatmap — Margen por Categoría y Laboratorio")
    heat = margin_df.pivot_table(index="category", columns="lab_name", values="margin_pct", aggfunc="mean")
    heat = heat.fillna(0)
    fig3 = px.imshow(heat, color_continuous_scale="RdYlGn", aspect="auto",
                     labels={"x":"Laboratorio","y":"Categoría","color":"Margen %"},
                     template="plotly_white")
    fig3.update_layout(height=380, margin=dict(t=10,b=10))
    st.plotly_chart(fig3, use_container_width=True)

# ════════════════════════════════════════════════════════════════
# DASHBOARD 11: TICKET PROMEDIO
# ════════════════════════════════════════════════════════════════
elif dashboard == "ticket":
    st.title("🎫 Ticket Promedio por Cliente")

    ticket_df = q(f"""
        SELECT cl.client_type, z.zone_name, s.year_month,
               COUNT(DISTINCT s.invoice_number) invoices,
               COUNT(DISTINCT s.client_key) clients,
               ROUND(SUM(s.net_amount),0) net,
               ROUND(SUM(s.net_amount)/NULLIF(COUNT(DISTINCT s.invoice_number),0),0) avg_ticket
        FROM fact_sales s
        JOIN dim_client cl ON cl.client_key=s.client_key
        JOIN dim_zone z ON z.zone_key=s.zone_key
        JOIN dim_product p ON p.product_key=s.product_key
        JOIN dim_distributor d ON d.distributor_key=s.distributor_key
        WHERE {where_clause()}
        GROUP BY cl.client_type, z.zone_name, s.year_month
        ORDER BY s.year_month
    """)

    overall_ticket = ticket_df["net"].sum() / ticket_df["invoices"].sum() if ticket_df["invoices"].sum() else 0
    st.metric("🎫 Ticket Promedio General", fmt_rd(overall_ticket))

    col_l, col_r = st.columns(2)
    with col_l:
        st.subheader("Ticket Promedio por Tipo de Cliente")
        _tt = ticket_df.groupby("client_type")[["net","invoices"]].sum().reset_index()
        _tt["avg_ticket"] = _tt["net"] / _tt["invoices"].replace(0, float("nan"))
        type_ticket = _tt[["client_type","avg_ticket"]].fillna(0)
        fig = px.bar(type_ticket.sort_values("avg_ticket", ascending=False),
                     x="client_type", y="avg_ticket", color="client_type",
                     color_discrete_sequence=COLORS, template="plotly_white",
                     labels={"client_type":"Tipo","avg_ticket":"Ticket Prom (RD$)"})
        fig.update_layout(height=340, margin=dict(t=10,b=40), showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Ticket Promedio por Zona")
        _zt = ticket_df.groupby("zone_name")[["net","invoices"]].sum().reset_index()
        _zt["avg_ticket"] = _zt["net"] / _zt["invoices"].replace(0, float("nan"))
        zone_ticket = _zt[["zone_name","avg_ticket"]].fillna(0)
        fig2 = px.bar(zone_ticket.sort_values("avg_ticket", ascending=False),
                      x="zone_name", y="avg_ticket", color="zone_name",
                      color_discrete_map=ZONE_COLORS, template="plotly_white",
                      labels={"zone_name":"Zona","avg_ticket":"Ticket Prom (RD$)"})
        fig2.update_layout(height=340, margin=dict(t=10,b=40), showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Evolución del Ticket Promedio por Mes")
    _mt = ticket_df.groupby("year_month")[["net","invoices"]].sum().reset_index()
    _mt["avg_ticket"] = _mt["net"] / _mt["invoices"].replace(0, float("nan"))
    monthly_ticket = _mt[["year_month","avg_ticket"]].fillna(0)
    fig3 = px.line(monthly_ticket, x="year_month", y="avg_ticket", markers=True,
                   line_shape="spline", template="plotly_white",
                   color_discrete_sequence=["#1E3A5F"],
                   labels={"year_month":"Mes","avg_ticket":"Ticket Prom (RD$)"})
    fig3.update_layout(height=280, margin=dict(t=10,b=40))
    st.plotly_chart(fig3, use_container_width=True)

# ════════════════════════════════════════════════════════════════
# DASHBOARD 12: VENTAS POR CATEGORÍA
# ════════════════════════════════════════════════════════════════
elif dashboard == "categoria":
    st.title("🗂️ Ventas por Categoría Terapéutica")

    cat_df = q(f"""
        SELECT p.category, p.subcategory, p.rx_otc_flag, z.zone_name, s.year_month,
               ROUND(SUM(s.net_amount),0) net,
               SUM(s.quantity) qty,
               ROUND(AVG(s.margin_pct),2) margin_pct
        FROM fact_sales s
        JOIN dim_product p ON p.product_key=s.product_key
        JOIN dim_zone z ON z.zone_key=s.zone_key
        JOIN dim_distributor d ON d.distributor_key=s.distributor_key
        WHERE {where_clause()}
        GROUP BY p.category, p.subcategory, p.rx_otc_flag, z.zone_name, s.year_month
        ORDER BY net DESC
    """)

    col_l, col_r = st.columns([3,2])
    with col_l:
        st.subheader("Sunburst — Categoría > Subcategoría")
        tree = cat_df.groupby(["category","subcategory"])["net"].sum().reset_index()
        fig = px.sunburst(tree, path=["category","subcategory"], values="net",
                          color="net", color_continuous_scale="Blues",
                          template="plotly_white")
        fig.update_layout(height=440, margin=dict(t=10,b=10))
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("RX vs OTC Split")
        rx_df = cat_df.groupby("rx_otc_flag")["net"].sum().reset_index()
        fig2 = px.pie(rx_df, values="net", names="rx_otc_flag", hole=0.5,
                      color_discrete_map={"RX":"#E74C3C","OTC":"#2ECC71"},
                      template="plotly_white")
        fig2.update_layout(height=240, margin=dict(t=10,b=10))
        st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Categorías por Ventas")
        cat_total = cat_df.groupby("category")["net"].sum().sort_values(ascending=True).reset_index()
        fig3 = px.bar(cat_total, x="net", y="category", orientation="h",
                      color="net", color_continuous_scale="Greens",
                      template="plotly_white",
                      labels={"net":"Ventas (RD$)","category":""})
        fig3.update_layout(height=280, margin=dict(t=10,b=10), coloraxis_showscale=False)
        st.plotly_chart(fig3, use_container_width=True)

    st.subheader("Evolución por Categoría")
    top5_cats = cat_df.groupby("category")["net"].sum().nlargest(5).index.tolist()
    cat_trend = cat_df[cat_df["category"].isin(top5_cats)].groupby(["year_month","category"])["net"].sum().reset_index()
    fig4 = px.line(cat_trend, x="year_month", y="net", color="category",
                   markers=True, template="plotly_white", color_discrete_sequence=COLORS,
                   labels={"year_month":"Mes","net":"Ventas (RD$)","category":"Categoría"})
    fig4.update_layout(height=280, margin=dict(t=10,b=40))
    st.plotly_chart(fig4, use_container_width=True)

# ════════════════════════════════════════════════════════════════
# DASHBOARD 13: RANKING VENDEDORES
# ════════════════════════════════════════════════════════════════
elif dashboard == "vendedores":
    st.title("🏆 Ranking de Vendedores")

    sp_df = q(f"""
        SELECT sp.full_name, z.zone_name, s.year_month, s.year,
               ROUND(SUM(s.net_amount),0) net,
               COUNT(DISTINCT s.invoice_number) invoices,
               COUNT(DISTINCT s.client_key) clients,
               ROUND(AVG(s.margin_pct),2) margin_pct
        FROM fact_sales s
        JOIN dim_salesperson sp ON sp.salesperson_key=s.salesperson_key
        JOIN dim_zone z ON z.zone_key=s.zone_key
        JOIN dim_product p ON p.product_key=s.product_key
        JOIN dim_distributor d ON d.distributor_key=s.distributor_key
        WHERE {where_clause()}
        GROUP BY sp.full_name, z.zone_name, s.year_month, s.year
        ORDER BY net DESC
    """)

    sp_total = sp_df.groupby(["full_name","zone_name"]).agg(
        net=("net","sum"), invoices=("invoices","sum"), clients=("clients","sum")
    ).reset_index().sort_values("net",ascending=False).reset_index(drop=True)
    sp_total["rank"] = sp_total.index + 1

    col_l, col_r = st.columns([2,1])
    with col_l:
        st.subheader("Ranking por Ventas Netas")
        fig = px.bar(sp_total, x="net", y="full_name", orientation="h",
                     color="zone_name", color_discrete_map=ZONE_COLORS,
                     template="plotly_white",
                     labels={"net":"Ventas (RD$)","full_name":"Vendedor","zone_name":"Zona"})
        fig.update_layout(height=440, margin=dict(t=10,b=10), yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig, use_container_width=True)

    with col_r:
        st.subheader("Leaderboard")
        tbl = sp_total[["rank","full_name","zone_name","net","invoices"]].copy()
        tbl.columns = ["#","Vendedor","Zona","Ventas","Facturas"]
        tbl["Ventas"] = tbl["Ventas"].apply(lambda x: f"RD$ {x:,.0f}")
        st.dataframe(tbl, use_container_width=True, hide_index=True, height=440)

    st.subheader("Ventas Mensuales por Vendedor — Top 6")
    top6 = sp_total.head(6)["full_name"].tolist()
    trend = sp_df[sp_df["full_name"].isin(top6)].groupby(["year_month","full_name"])["net"].sum().reset_index()
    fig2 = px.line(trend, x="year_month", y="net", color="full_name", markers=True,
                   template="plotly_white", color_discrete_sequence=COLORS,
                   labels={"year_month":"Mes","net":"Ventas (RD$)","full_name":"Vendedor"})
    fig2.update_layout(height=300, margin=dict(t=10,b=40))
    st.plotly_chart(fig2, use_container_width=True)

# ════════════════════════════════════════════════════════════════
# DASHBOARD 14: CUMPLIMIENTO DE METAS
# ════════════════════════════════════════════════════════════════
elif dashboard == "metas":
    st.title("🎯 Cumplimiento de Metas")

    _years_csv2 = ",".join(str(y) for y in year_filter) or "0"
    comm = q(f"""
        SELECT fc.year_month, sp.full_name, z.zone_name,
               fc.sales_amount, fc.target_amount, fc.achievement_pct,
               fc.total_payout
        FROM fact_commission fc
        JOIN dim_salesperson sp ON sp.salesperson_key=fc.salesperson_key
        JOIN dim_zone z ON z.zone_key=fc.zone_key
        WHERE CAST(SPLIT_PART(fc.year_month, '-', 1) AS INTEGER) IN ({_years_csv2})
        AND z.zone_name IN ('{"','".join(zone_filter)}')
        ORDER BY fc.year_month DESC
    """)

    if comm.empty:
        st.warning("No hay datos disponibles.")
    else:
        last_month = comm["year_month"].max()
        lm = comm[comm["year_month"]==last_month]

        overall_ach = (lm["sales_amount"].sum() / lm["target_amount"].sum() * 100) if lm["target_amount"].sum() > 0 else 0
        above_target = (lm["achievement_pct"] >= 100).sum()
        below_target = (lm["achievement_pct"] < 100).sum()

        c1,c2,c3 = st.columns(3)
        c1.metric(f"🎯 Logro General ({last_month})", f"{overall_ach:.1f}%",
                  delta_color="normal", delta=f"{overall_ach-100:+.1f}% vs meta")
        c2.metric("✅ Sobre Meta", f"{above_target} vendedores")
        c3.metric("⚠️ Bajo Meta",  f"{below_target} vendedores")

        st.subheader(f"Logro por Vendedor — {last_month}")
        colors = ["#2ECC71" if v >= 100 else "#F39C12" if v >= 80 else "#E74C3C" for v in lm["achievement_pct"]]
        fig = go.Figure()
        for _, row in lm.sort_values("achievement_pct").iterrows():
            color = "#2ECC71" if row.achievement_pct >= 100 else "#F39C12" if row.achievement_pct >= 80 else "#E74C3C"
            fig.add_trace(go.Bar(
                x=[row.achievement_pct], y=[row.full_name], orientation="h",
                marker_color=color, showlegend=False,
                text=f"{row.achievement_pct:.1f}%", textposition="outside",
            ))
        fig.add_vline(x=100, line_dash="dash", line_color="navy", annotation_text="Meta 100%")
        fig.add_vline(x=80, line_dash="dot", line_color="orange", annotation_text="Mínimo 80%")
        fig.update_layout(height=420, margin=dict(t=10,b=10), xaxis_title="Logro %",
                          plot_bgcolor="white", paper_bgcolor="white", yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Evolución del Logro Promedio — Tendencia")
        _ma = comm.groupby("year_month")[["sales_amount","target_amount"]].sum().reset_index()
        _ma["avg_ach"] = (_ma["sales_amount"] / _ma["target_amount"].replace(0, float("nan")) * 100).fillna(0)
        monthly_ach = _ma[["year_month","avg_ach"]].sort_values("year_month")
        fig2 = px.line(monthly_ach, x="year_month", y="avg_ach", markers=True,
                       line_shape="spline", template="plotly_white",
                       color_discrete_sequence=["#1E3A5F"],
                       labels={"year_month":"Mes","avg_ach":"Logro Promedio %"})
        fig2.add_hline(y=100, line_dash="dash", line_color="#2ECC71", annotation_text="Meta 100%")
        fig2.add_hrect(y0=100, y1=200, fillcolor="#2ECC71", opacity=0.05)
        fig2.add_hrect(y0=0, y1=100, fillcolor="#E74C3C", opacity=0.03)
        fig2.update_layout(height=280, margin=dict(t=10,b=40))
        st.plotly_chart(fig2, use_container_width=True)

# ── Footer ────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<div style='text-align:center; color:#6c757d; font-size:0.8rem;'>"
    "💊 Pharma DR · Plataforma de Inteligencia Comercial · "
    "República Dominicana · Powered by DuckDB + Streamlit"
    "</div>",
    unsafe_allow_html=True,
)

