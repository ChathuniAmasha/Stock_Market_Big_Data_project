import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import storage
from io import BytesIO

# ---------- Helpers ----------
def load_csv(bucket, file_path, parse_dates=None):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(file_path)
    data = blob.download_as_bytes()
    return pd.read_csv(BytesIO(data), parse_dates=parse_dates)

def get_prev_day_value(df, colname):
    if colname not in df.columns:
        return None
    tmp = df[["timestamp", colname]].dropna()
    if tmp.empty:
        return None
    tmp = tmp.copy()
    tmp["date"] = tmp["timestamp"].dt.date
    latest_date = tmp["date"].max()
    prev_date = latest_date - pd.Timedelta(days=1)
    prev = tmp[tmp["date"] == prev_date]
    if not prev.empty:
        return float(prev[colname].iloc[-1])
    return float(tmp[colname].iloc[-1])

# ---------- App setup ----------
BUCKET = "stock-project-cleaned-data"
integrated = load_csv(BUCKET, "integrated_data/integrated_all.csv", parse_dates=["timestamp"])
integrated = integrated.sort_values("timestamp")
companies = integrated["symbol"].dropna().unique()

st.set_page_config(page_title="Stock Market Dashboard", layout="wide")

st.markdown(
    """
    <style>
    /* Reduce top and bottom padding */
    .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }
    /* Reduce spacing between elements */
    div.stButton > button {
        margin: 0px;
    }
    </style>
    """,
    unsafe_allow_html=True
)

page = st.sidebar.radio("☰", ["📊 Market Overview", "🏢 Company Overview"])

logo_urls = {
    "MSFT": "https://logo.clearbit.com/microsoft.com",
    "AAPL": "https://logo.clearbit.com/apple.com",
    "AMZN": "https://logo.clearbit.com/amazon.com",
    "TSLA": "https://logo.clearbit.com/tesla.com"
}

# ============================================================
# PAGE 1: MARKET OVERVIEW
# ============================================================
if page == "📊 Market Overview":
    st.title("📊 Stock Market Overview")

    # ---------- Compute hourly % change for mini-cards ----------
    hourly_changes = {}
    last_prices = {}
    for company in companies:
        df_c = integrated[integrated["symbol"] == company]
        if len(df_c) >= 2:
            last_price = float(df_c["c"].iloc[-1])
            prev_price = float(df_c["c"].iloc[-2])
            pct = ((last_price - prev_price) / prev_price) * 100 if prev_price != 0 else 0.0
        else:
            last_price = float(df_c["c"].iloc[-1]) if len(df_c) == 1 else 0.0
            pct = 0.0
        last_prices[company] = last_price
        hourly_changes[company] = pct

    # ---------- Prev-day macro values ----------
    gdp_val = get_prev_day_value(integrated, "GDP")
    unrate_val = get_prev_day_value(integrated, "UNRATE")

    # ============================================================
    # ROW 1: Price cards (each company) + GDP card
    # ============================================================
    cols_top = st.columns(len(companies) + 1)
    for i, company in enumerate(companies):
        with cols_top[i]:
            # Custom styled label (pink)
            st.markdown(
                f"""
                <div style="display:flex; align-items:center; justify-content:center; gap:10px;">
                    <img src="{logo_urls.get(company)}" width="30" style="border-radius:5px;">
                    <span style="font-size:16px; font-weight:bold; color:deeppink;">{company}</span>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.metric(
                label="",  # hide default label
                value=f"${last_prices[company]:.2f}",
                delta=f"{hourly_changes[company]:.2f}% (1h)"
            )
    with cols_top[-1]:
        if gdp_val is not None:
            st.markdown(
                f"""
                <div style="text-align:center;">
                    <div style="font-size:20px; font-weight:bold; color:#1f77b4;">
                        💰 GDP ($ Billion)
                    </div>
                    <div style="height:15px;"></div>
                    <div style="font-size:35px; font-weight:600; color:#1f77b4;">
                        {gdp_val:,.2f}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.info("GDP not found in dataset.")


    # ============================================================
    # ROW 2: Gauge + Line Chart + Volume Bar (increased height)
    # ============================================================
    st.markdown("---")
    col_gauge, col_line, col_vol = st.columns([1, 2, 1])

    # Gauge
    with col_gauge:
        st.markdown("### 👷‍♂️ Unemployment Rate")
        if unrate_val is not None:
            st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
            fig_unrate = go.Figure(go.Indicator(
                mode="gauge+number",
                value=unrate_val,
                number={"suffix": "%"},
                gauge={
                    "axis": {"range": [0, 10], "tickvals": []},
                    "bar": {"color": "red"},
                    "steps": [
                        {"range": [0, unrate_val], "color": "salmon"},
                        {"range": [unrate_val, 10], "color": "lightgray"},
                    ],
                }
            ))
            fig_unrate.update_layout(height=320, margin=dict(t=10, b=10))  # increased height
            st.plotly_chart(fig_unrate, use_container_width=True)

    # Line chart
    with col_line:
        st.markdown("### 💹 Stock Price Trends")
        fig_all = px.line(integrated, x="timestamp", y="c", color="symbol", title="")
        ymin, ymax = integrated["c"].min(), integrated["c"].max()
        pad = (ymax - ymin) * 0.07 if np.isfinite(ymax - ymin) else 1.0
        fig_all.update_yaxes(range=[ymin - pad, ymax + pad])
        fig_all.update_layout(height=320, margin=dict(t=20, b=20))  # increased height
        st.plotly_chart(fig_all, use_container_width=True)

    # Trading Volume
    with col_vol:
        st.markdown("### 📊 Trading Volume")
        if "Volume" in integrated.columns:
            idx = integrated.groupby("symbol")["timestamp"].idxmax()
            latest_volumes = integrated.loc[idx, ["symbol", "Volume"]].sort_values("Volume", ascending=False)
            fig_vol = px.bar(latest_volumes, y="symbol", x="Volume", orientation="h", color="symbol", text_auto=True)
            fig_vol.update_layout(showlegend=False, title="", height=320, margin=dict(t=20, b=20))  # increased height
            st.plotly_chart(fig_vol, use_container_width=True)

# ============================================================
# PAGE 2: COMPANY OVERVIEW
# ============================================================
if page == "🏢 Company Overview":
    st.title("🏢 Company Overview")

    # --- Company selection ---
    company = st.selectbox("Select company", companies)
    df_c = integrated[integrated["symbol"] == company].sort_values("timestamp")

    # --- Latest values for cards ---
    if not df_c.empty:
        latest_row = df_c.iloc[-1]
        prev_close = float(latest_row["pc"]) if "pc" in df_c.columns else None
        current_price = float(latest_row["c"]) if "c" in df_c.columns else None
        pct_change = float(latest_row["dp"]) if "dp" in df_c.columns else None
    else:
        prev_close, current_price, pct_change = None, None, None

    # --- Company summaries ---
    company_summaries = {
        "MSFT": """Microsoft Corporation develops and supports software, services, devices, and solutions worldwide. 
        Its Productivity and Business Processes segment offers Microsoft 365, Teams, Dynamics, LinkedIn and more. 
        Its Intelligent Cloud segment provides Azure, GitHub, SQL Server, and enterprise services. 
        Its Personal Computing segment includes Windows, Surface, Xbox, advertising, and more. 
        Founded in 1975, headquartered in Redmond, Washington.""",

        "AAPL": """Apple Inc. designs, manufactures, and markets smartphones, personal computers, tablets, wearables, 
        and accessories worldwide. Products include iPhone, Mac, iPad, AirPods, Apple Watch, and services like 
        Apple Music, Apple TV+, Apple Arcade, Apple Pay, and the App Store. 
        Founded in 1976, headquartered in Cupertino, California.""",

        "AMZN": """Amazon.com, Inc. engages in e-commerce, cloud computing (AWS), digital streaming, and AI. 
        It sells consumer products and subscriptions, manufactures devices like Kindle and Echo, and offers Amazon Prime. 
        AWS provides compute, storage, ML, and other cloud services. 
        Founded in 1994, headquartered in Seattle, Washington.""",

        "TSLA": """Tesla, Inc. designs, manufactures, and sells electric vehicles and clean energy solutions. 
        The Automotive segment offers EVs, regulatory credits, and related services. 
        The Energy segment provides solar panels, batteries, and energy storage. 
        Founded in 2003, headquartered in Austin, Texas."""
    }

    summary_text = company_summaries.get(company, "Company profile not available.")

    # ============================================================
    # ROW 1: Profile + Price & Stats
    # ============================================================
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])

    # --- Company Profile ---
    with col1:
        st.markdown(
            f"""
            <div style="
                background-color:#1e1e2f;
                padding:20px;
                border-radius:15px;
                color:white;
                min-height:200px;
            ">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <h3 style="margin:0;">{company} Corporation</h3>
                    <img src="{logo_urls.get(company)}" width="60" style="border-radius:5px;">
                </div>
                <p style="margin-top:10px; font-size:14px; color:#cccccc; text-align:justify;">
                    {summary_text}
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # --- Col 2: Previous Close + Highest Price ---
    with col2:
        if prev_close is not None:
            st.metric(label="Previous Close", value=f"${prev_close:.2f}")
        else:
            st.info("Prev Close not found")

        if "h" in df_c.columns:
            st.metric(label="Highest Price", value=f"${df_c['h'].iloc[-1]:.2f}")
        else:
            st.info("High not found")

    # --- Col 3: Current Price + Lowest Price ---
    with col3:
        if current_price is not None:
            st.markdown(
                f"""
                <div style="text-align:left; font-size:14px; color:white;">
                    Current Price
                </div>
                <div style="text-align:left; font-weight:bold; font-size:35px; color:blue; margin-bottom:14px;">
                    ${current_price:.2f}
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.info("Current Price not found")

        if "l" in df_c.columns:
            st.metric(label="Lowest Price", value=f"${df_c['l'].iloc[-1]:.2f}")
        else:
            st.info("Low not found")

    # --- Col 4: % Change + Volume ---
    with col4:
        if pct_change is not None:
            color = "green" if pct_change >= 0 else "red"
            sign = "+" if pct_change >= 0 else ""
            st.markdown(
                f"""
                <div style="text-align:left; font-size:14px; color:white;">
                    % Change
                </div>
                <div style="text-align:left; font-weight:bold; font-size:35px; color:{color}; margin-bottom:14px;">
                    {sign}{pct_change:.2f}%
                </div>
                """,
                unsafe_allow_html=True,
            )
        else:
            st.info("% Change not found")

        if "Volume" in df_c.columns:
            # Convert to millions
            volume_millions = df_c['Volume'].iloc[-1] / 1_000_000
            st.metric(label="Volume", value=f"{volume_millions:.1f}M")
        else:
            st.info("Volume not found")


    # ============================================================
    # ROW 2: Trend Area Chart + Returns Distribution
    # ============================================================
    col5, col6 = st.columns([2, 1])

    with col5:
        st.markdown(f"### 📈 {company} Stock Price Trend")
        fig_area = px.area(df_c, x="timestamp", y="c", title="")
        y_min, y_max = df_c["c"].min(), df_c["c"].max()
        margin = (y_max - y_min) * 0.1 if np.isfinite(y_max - y_min) else 1.0
        fig_area.update_yaxes(range=[y_min - margin, y_max + margin])
        fig_area.update_layout(height=400, margin=dict(t=30, b=30))
        st.plotly_chart(fig_area, use_container_width=True)

    with col6:
        st.markdown("### 📊 Returns KDE")
        df_c = df_c.copy()
        df_c["return"] = df_c["c"].pct_change()

        # KDE plot
        fig_kde = px.histogram(
            df_c, x="return", nbins=60, histnorm="probability density",
            opacity=0.5, marginal="box"
        )
        fig_kde.update_traces(marker_color="steelblue")
        fig_kde.update_layout(height=300, margin=dict(t=30, b=30))
        st.plotly_chart(fig_kde, use_container_width=True)

        # Stability check
        vol = df_c["return"].std()
        stability = "✅ Stable" if vol < 0.02 else "⚠️ Unstable"

        st.markdown(
            f"""
            <div style="text-align:center; font-size:18px; font-weight:bold; 
                        color:{'green' if vol < 0.02 else 'red'};">
                {stability}<br>
                <span style="font-size:13px; color:#aaa;">(Volatility = {vol:.3f})</span>
            </div>
            """,
            unsafe_allow_html=True
        )

    # ============================================================
    # ROW 3: Correlation + Causality
    # ============================================================
    # rename mapping
    col_names_map = {
        "c": "Current Price",
        "h": "High",
        "l": "Low",
        "o": "Open",
        "d": "Change",
        "dp": "% Change",
        "pc": "Previous Close",
        "CPIAUCSL": "CPI"
    }

    col7, spacer, col8 = st.columns([1, 0.05, 1])

    with col7:
        st.markdown("### 🔗 Correlation Heatmap")
        try:
            corr = load_csv(BUCKET, f"results/{company}_correlation.csv")

            if "timestamp" in corr.columns:
                corr = corr.drop(columns=["timestamp"])

            corr = corr.dropna(axis=0, how="all").dropna(axis=1, how="all")

            if corr.shape[0] == corr.shape[1]:
                corr.index = corr.columns

            corr.rename(columns=col_names_map, index=col_names_map, inplace=True)

            fig_corr = px.imshow(
                corr,
                text_auto=".2f",
                x=corr.columns,
                y=corr.index,
                color_continuous_scale="RdBu_r",
                title=f"{company} Correlation Matrix"
            )
            fig_corr.update_layout(
                height=600,
                margin=dict(t=40, b=40)
            )
            st.plotly_chart(fig_corr, use_container_width=True)
        except Exception as e:
            st.info(f"Correlation file not found or invalid: {e}")

    with col8:
        # ---- Causality Analysis (smaller + shifted up) ----
        st.markdown("### 🧭 Causality Analysis - Top Predictive Features")
        try:
            causality = load_csv(BUCKET, f"results/{company}_causality.csv")
            causality["predictor"] = causality["predictor"].replace(col_names_map)
            causality_summary = causality.groupby("predictor")["p_value"].min().reset_index()
            causality_summary["significance"] = -np.log10(causality_summary["p_value"].replace(0, 1e-10))
            fig_causality = px.bar(
                causality_summary.sort_values("significance", ascending=False).head(10),
                x="predictor", y="significance"
            )
            fig_causality.update_layout(
                height=300,  # smaller height
                margin=dict(t=30, b=30)  # move graph up a bit
            )
            st.plotly_chart(fig_causality, use_container_width=True)
        except Exception as e:
            st.info(f"Causality file not found or invalid: {e}")

        # ---- Forecast Analysis ----
        st.markdown(f"### 🔮 {company} Price Forecast (Next 7 Days)")
        try:
            forecast = load_csv(BUCKET, f"results/{company}_forecast.csv")

            if "timestamp" in forecast.columns:
                forecast = forecast.drop(columns=["timestamp"])

            forecast = forecast.reset_index(drop=True)

            if "forecast_c" in forecast.columns:
                forecast.rename(columns={"forecast_c": "Forecasted Price"}, inplace=True)

            forecast["DayLabel"] = ""
            for i in range(len(forecast)):
                if (i + 1) % 24 == 0:
                    day_num = (i + 1) // 24
                    forecast.at[i, "DayLabel"] = f"Day {day_num}"

            y_min, y_max = forecast["Forecasted Price"].min(), forecast["Forecasted Price"].max()
            margin = (y_max - y_min) * 0.1 if np.isfinite(y_max - y_min) else 1.0

            fig_forecast = px.line(
                forecast, x=forecast.index + 1, y="Forecasted Price"
            )

            # Make forecast line bold
            fig_forecast.update_traces(line=dict(width=3))

            fig_forecast.update_yaxes(range=[y_min - margin, y_max + margin])

            day_ticks = forecast[forecast["DayLabel"] != ""]
            fig_forecast.update_xaxes(
                tickmode="array",
                tickvals=day_ticks.index + 1,
                ticktext=day_ticks["DayLabel"],
                title_text="Day"
            )

            fig_forecast.update_layout(
                height=300,  # same height as causality for packing
                margin=dict(t=30, b=40)
            )

            st.plotly_chart(fig_forecast, use_container_width=True)

        except Exception as e:
            st.info(f"Forecast file not found or invalid: {e}")



