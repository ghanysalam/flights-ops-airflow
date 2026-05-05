import streamlit as st
import pandas as pd
import altair as alt

# ── Page configuration ──────────────────────────────────────────────────────
st.set_page_config(
    page_title="Flight Operations Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS – premium dark glassmorphism aesthetic ────────────────────────
st.markdown(
    """
    <style>
    /* ---------- Google Font ---------- */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* ---------- Main background ---------- */
    .stApp {
        background: linear-gradient(135deg, #0f0c29 0%, #1a1a2e 40%, #16213e 100%);
    }

    /* ---------- Sidebar ---------- */
    section[data-testid="stSidebar"] {
        background: rgba(15, 12, 41, 0.85);
        backdrop-filter: blur(12px);
        border-right: 1px solid rgba(255,255,255,0.06);
    }

    /* ---------- KPI cards ---------- */
    div[data-testid="stMetric"] {
        background: linear-gradient(145deg, rgba(255,255,255,0.06) 0%, rgba(255,255,255,0.02) 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 16px;
        padding: 20px 24px;
        backdrop-filter: blur(10px);
        box-shadow: 0 8px 32px rgba(0,0,0,0.25);
        transition: transform 0.25s ease, box-shadow 0.25s ease;
    }
    div[data-testid="stMetric"]:hover {
        transform: translateY(-4px);
        box-shadow: 0 12px 40px rgba(80, 120, 255, 0.15);
    }
    div[data-testid="stMetric"] label {
        color: #8b95a8 !important;
        font-weight: 500 !important;
        font-size: 0.85rem !important;
        letter-spacing: 0.04em;
        text-transform: uppercase;
    }
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: #e8ecf4 !important;
        font-weight: 700 !important;
        font-size: 2rem !important;
    }

    /* ---------- Section headers ---------- */
    h1, h2, h3 {
        color: #e8ecf4 !important;
    }

    /* ---------- Charts glass container ---------- */
    div[data-testid="stVegaLiteChart"] {
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 16px;
        padding: 12px;
        backdrop-filter: blur(8px);
    }

    /* ---------- DataFrame ---------- */
    div[data-testid="stDataFrame"] {
        border-radius: 12px;
        overflow: hidden;
    }

    /* ---------- Scrollbar ---------- */
    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.15); border-radius: 3px; }

    /* ---------- Divider ---------- */
    hr { border-color: rgba(255,255,255,0.08) !important; }

    /* ---------- Accent glow header ---------- */
    .header-glow {
        background: linear-gradient(90deg, #6c63ff, #3b82f6, #06b6d4);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 800;
        font-size: 2.2rem;
        letter-spacing: -0.02em;
    }
    .sub-header {
        color: #64748b;
        font-size: 1rem;
        margin-top: -8px;
    }

    /* ---------- Input Labels (White) ---------- */
    .stSlider label,
    .stMultiSelect label,
    .stCheckbox label,
    div[data-testid="stCheckbox"] *,
    .stTextInput label,
    div[data-testid="stSidebar"] label {
        color: #ffffff !important;
    }

    /* ---------- Multiselect inner text (Black) ---------- */
    .stMultiSelect div[data-baseweb="select"] * {
        color: #000000 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


# ── Data Loading ─────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Connecting to Snowflake...")
def load_flight_data():
    conn = st.connection("snowflake")
    session = conn.session()
    
    # Gunakan fully qualified name: "DATABASE.SCHEMA.TABLE"
    # Sesuaikan 'KPI' dengan nama schema tempat tabel FLIGHTS_GOLD Anda berada
    df = session.table("FLIGHTS.KPI.FLIGHT_KPIS").to_pandas()
    
    # Normalisasi kolom ke lowercase
    df.columns = [c.lower().strip() for c in df.columns]
    return df

try:
    df = load_flight_data()
except Exception as e:
    st.error(f"Error loading data from Snowflake: {e}")
    st.stop()

# ── Sidebar filters ──────────────────────────────────────────────────────────
st.sidebar.markdown("## Filters")

# — Flight volume slider
min_flights = int(df["total_flights"].min())
max_flights = int(df["total_flights"].max())
flight_range = st.sidebar.slider(
    "Total Flights Range",
    min_value=min_flights,
    max_value=max_flights,
    value=(min_flights, max_flights),
)

# — Country multi-select (default = all)
all_countries = sorted(df["origin_country"].unique().tolist())
selected_countries = st.sidebar.multiselect(
    "Select Countries",
    options=all_countries,
    default=[],
    placeholder="All countries",
)

# — On-ground toggle
show_on_ground_only = st.sidebar.checkbox("Only countries with aircraft on ground")

# Apply filters
mask = (
    (df["total_flights"] >= flight_range[0])
    & (df["total_flights"] <= flight_range[1])
)
if selected_countries:
    mask &= df["origin_country"].isin(selected_countries)
if show_on_ground_only:
    mask &= df["on_ground"] > 0

filtered = df[mask].copy()

# ── Header ───────────────────────────────────────────────────────────────────
st.markdown('<p class="header-glow">Flight Operations Dashboard</p>', unsafe_allow_html=True)
st.markdown(
    '<p class="sub-header">Gold-layer analytics · Aggregated by origin country · '
    f'{len(filtered)} countries shown</p>',
    unsafe_allow_html=True,
)
st.markdown("---")

# ── KPI row ──────────────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
total_flights_sum = int(filtered["total_flights"].sum())
avg_velocity_global = round(filtered["avg_velocity"].mean(), 1) if len(filtered) else 0
total_on_ground = int(filtered["on_ground"].sum())
country_count = len(filtered)

k1.metric("Countries", f"{country_count:,}")
k2.metric("Total Flights", f"{total_flights_sum:,}")
k3.metric("Avg Velocity (kn)", f"{avg_velocity_global:,}")
k4.metric("Aircraft on Ground", f"{total_on_ground:,}")

st.markdown("")

# ── Altair colour palette ────────────────────────────────────────────────────
GRADIENT_RANGE = ["#6c63ff", "#3b82f6", "#06b6d4", "#10b981", "#f59e0b"]

# ── Chart row 1: Top-N bar chart + velocity scatter ─────────────────────────
st.markdown("### Flight Volume & Velocity Overview")
col_bar, col_scatter = st.columns([1.2, 1])

top_n = st.sidebar.slider("Top N countries (bar chart)", 5, 30, 15)

top_df = filtered.nlargest(top_n, "total_flights")

with col_bar:
    bar = (
        alt.Chart(top_df, title=alt.Title(f"Top {top_n} Countries by Total Flights"))
        .mark_bar(
            cornerRadiusTopLeft=6,
            cornerRadiusTopRight=6,
        )
        .encode(
            x=alt.X("total_flights:Q", title="Total Flights"),
            y=alt.Y("origin_country:N", sort="-x", title=None),
            color=alt.Color(
                "total_flights:Q",
                scale=alt.Scale(range=GRADIENT_RANGE),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("origin_country:N", title="Country"),
                alt.Tooltip("total_flights:Q", title="Flights", format=","),
                alt.Tooltip("avg_velocity:Q", title="Avg Velocity", format=".1f"),
            ],
        )
        .properties(height=450)
        .configure_axis(
            labelColor="#94a3b8",
            titleColor="#94a3b8",
            gridColor="rgba(255,255,255,0.04)",
        )
        .configure_title(color="#1a1a1a", fontSize=15, anchor="start")
        .configure_view(strokeWidth=0)
    )
    st.altair_chart(bar, use_container_width=True)

with col_scatter:
    scatter = (
        alt.Chart(filtered, title=alt.Title("Avg Velocity vs Total Flights"))
        .mark_circle(opacity=0.75)
        .encode(
            x=alt.X("total_flights:Q", title="Total Flights", scale=alt.Scale(type="log")),
            y=alt.Y("avg_velocity:Q", title="Avg Velocity (kn)"),
            size=alt.Size("on_ground:Q", title="On Ground", scale=alt.Scale(range=[40, 600])),
            color=alt.Color(
                "avg_velocity:Q",
                scale=alt.Scale(scheme="turbo"),
                legend=alt.Legend(title="Velocity"),
            ),
            tooltip=[
                alt.Tooltip("origin_country:N", title="Country"),
                alt.Tooltip("total_flights:Q", title="Flights", format=","),
                alt.Tooltip("avg_velocity:Q", title="Avg Velocity", format=".1f"),
                alt.Tooltip("on_ground:Q", title="On Ground"),
            ],
        )
        .properties(height=450)
        .configure_axis(
            labelColor="#94a3b8",
            titleColor="#94a3b8",
            gridColor="rgba(255,255,255,0.04)",
        )
        .configure_title(color="#1a1a1a", fontSize=15, anchor="start")
        .configure_view(strokeWidth=0)
    )
    st.altair_chart(scatter, use_container_width=True)

# ── Chart row 2: On-ground distribution + velocity histogram ────────────────
st.markdown("### On-Ground & Velocity Distribution")
col_pie, col_hist = st.columns(2)

with col_pie:
    on_ground_top = filtered.nlargest(10, "on_ground")[["origin_country", "on_ground"]]
    others = pd.DataFrame(
        {
            "origin_country": ["Others"],
            "on_ground": [
                filtered["on_ground"].sum() - on_ground_top["on_ground"].sum()
            ],
        }
    )
    pie_df = pd.concat([on_ground_top, others], ignore_index=True)
    pie_df = pie_df[pie_df["on_ground"] > 0]

    donut = (
        alt.Chart(pie_df, title=alt.Title("On-Ground Distribution (Top 10 + Others)"))
        .mark_arc(innerRadius=60, outerRadius=140, padAngle=0.02, cornerRadius=6)
        .encode(
            theta=alt.Theta("on_ground:Q"),
            color=alt.Color(
                "origin_country:N",
                scale=alt.Scale(scheme="tableau20"),
                legend=alt.Legend(title="Country", labelColor="#94a3b8", titleColor="#94a3b8"),
            ),
            tooltip=[
                alt.Tooltip("origin_country:N", title="Country"),
                alt.Tooltip("on_ground:Q", title="On Ground"),
            ],
        )
        .properties(height=400)
        .configure_title(color="#1a1a1a", fontSize=15, anchor="start")
        .configure_view(strokeWidth=0)
    )
    st.altair_chart(donut, width="stretch")

with col_hist:
    hist = (
        alt.Chart(filtered, title=alt.Title("Velocity Distribution Across Countries"))
        .mark_bar(
            cornerRadiusTopLeft=4,
            cornerRadiusTopRight=4,
            opacity=0.85,
        )
        .encode(
            x=alt.X("avg_velocity:Q", bin=alt.Bin(maxbins=20), title="Avg Velocity (kn)"),
            y=alt.Y("count():Q", title="Number of Countries"),
            color=alt.value("#3b82f6"),
            tooltip=[
                alt.Tooltip("avg_velocity:Q", bin=alt.Bin(maxbins=20), title="Velocity Bin"),
                alt.Tooltip("count():Q", title="Countries"),
            ],
        )
        .properties(height=400)
        .configure_axis(
            labelColor="#94a3b8",
            titleColor="#94a3b8",
            gridColor="rgba(255,255,255,0.04)",
        )
        .configure_title(color="#1a1a1a", fontSize=15, anchor="start")
        .configure_view(strokeWidth=0)
    )
    st.altair_chart(hist, width="stretch")

# ── Chart row 3: Ground-ratio analysis ──────────────────────────────────────
st.markdown("### Ground Ratio Analysis")

ratio_df = filtered.copy()
ratio_df["ground_ratio"] = (
    ratio_df["on_ground"] / ratio_df["total_flights"] * 100
).round(2)
ratio_top = ratio_df[ratio_df["total_flights"] >= 5].nlargest(20, "ground_ratio")

ground_bar = (
    alt.Chart(ratio_top, title=alt.Title("Top 20 Countries by Ground Ratio (%) — min 5 flights"))
    .mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6)
    .encode(
        x=alt.X("ground_ratio:Q", title="Ground Ratio (%)"),
        y=alt.Y("origin_country:N", sort="-x", title=None),
        color=alt.Color(
            "ground_ratio:Q",
            scale=alt.Scale(range=["#10b981", "#f59e0b", "#ef4444"]),
            legend=None,
        ),
        tooltip=[
            alt.Tooltip("origin_country:N", title="Country"),
            alt.Tooltip("ground_ratio:Q", title="Ground %", format=".2f"),
            alt.Tooltip("total_flights:Q", title="Flights", format=","),
            alt.Tooltip("on_ground:Q", title="On Ground"),
        ],
    )
    .properties(height=480)
    .configure_axis(
        labelColor="#94a3b8",
        titleColor="#94a3b8",
        gridColor="rgba(255,255,255,0.04)",
    )
    .configure_title(color="#1a1a1a", fontSize=15, anchor="start")
    .configure_view(strokeWidth=0)
)
st.altair_chart(ground_bar, width="stretch")

# ── Data table ───────────────────────────────────────────────────────────────
st.markdown("### Detailed Data Table")

search = st.text_input("Search country…", "")
display_df = filtered.copy()
display_df["ground_ratio_%"] = (
    display_df["on_ground"] / display_df["total_flights"] * 100
).round(2)

if search:
    display_df = display_df[
        display_df["origin_country"].str.contains(search, case=False, na=False)
    ]

display_df = display_df.sort_values("total_flights", ascending=False).reset_index(drop=True)
display_df.columns = [c.replace("_", " ").title() for c in display_df.columns]

st.dataframe(
    display_df,
    width="stretch",
    height=420,
)

# ── Footer ───────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    "<div style='text-align:center; color:#475569; font-size:0.8rem;'>"
    "Flight Operations Dashboard · Gold Layer · Data sourced from OpenSky Network"
    "</div>",
    unsafe_allow_html=True,
)
