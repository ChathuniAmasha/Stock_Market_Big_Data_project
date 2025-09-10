# Real‑Time Stock Market Dashboard (Streamlit + Google Cloud)

A cloud‑hosted, interactive dashboard for exploring **real‑time stock market trends**, **ML‑based forecasts**, and **correlation/causation analysis** across major tickers. The system unifies traditional market data with alternative signals (e.g., Google Trends and macroeconomic indicators) and updates automatically on a fixed schedule.

## 🔗 Live Dashboard
**Open the app:** https://YOUR-CLOUD-RUN-URL  <!-- Replace with your Cloud Run URL, e.g., https://stock-dashboard-abcde-uc.a.run.app -->

> If the link is restricted, ask the owner for access or an updated public URL.

## ✨ What You Can Do
- View **historical price trends** and time‑windowed charts
- Inspect **correlation heatmaps** across features (prices, trends, macro indicators)
- Explore **Granger causality results** to see potential leading signals
- See **short‑term ML forecasts** (e.g., 1‑week ahead) for supported companies

## 🧰 Tech & Platform
- **Frontend/App:** Streamlit
- **Data & ML:** Python (pandas, NumPy, statsmodels), time‑series modeling (e.g., SARIMAX)
- **Cloud:** Google Cloud (Cloud Run, Firestore, Cloud Storage, Cloud Scheduler)
- **Data Sources:** Market APIs (e.g., Finnhub/Yahoo Finance), Google Trends, macroeconomic series (e.g., FRED)

## 🗺️ High‑Level Architecture
1. **Ingest & Automate** — Scheduled jobs fetch data and write raw records.
2. **Store & Prepare** — Raw data in Firestore; cleaned/integrated datasets in Cloud Storage.
3. **Analyze & Forecast** — Company‑wise correlation, Granger causality, and short‑term forecasting.
4. **Visualize** — Streamlit dashboard serves interactive charts and results via Cloud Run.

## 📝 Notes
- This repository is meant as a **code and results showcase**. It intentionally **omits setup instructions** for running in the owner’s Google Cloud environment.
- If you’re interested in deploying your own copy, please fork the repo and adapt services to **your** GCP project (billing, IAM, buckets, scheduler, and secrets).

## 📜 License
See the [LICENSE](LICENSE) file for usage terms (if provided).
