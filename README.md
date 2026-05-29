# Transformer Overload Prediction for the Portuguese Distribution Grid

Master's thesis — NOVA IMS, 2026  
Developed in partnership with CGI Portugal and E-REDES (Portuguese electricity distribution operator).

---

## Overview

This repository contains the modelling code for a thesis comparing three machine learning approaches to predict distribution transformer overloads at **4-hour and 24-hour** prediction horizons, using SCADA telemetry from the Portuguese electricity grid (July 2023 – July 2024).

The three models evaluated are:

- **Logistic Regression** (Spark ML, with FeatureHasher for categorical encoding)
- **XGBoost** (`SparkXGBClassifier`, with early stopping)
- **LSTM** (PyTorch, with learned embeddings for transformer ID and municipality)

Primary evaluation metrics are **AUPRC** and **F1**, given severe class imbalance (~244:1 at the 4h horizon, ~91:1 at 24h).

---

## Repository Structure

```
├── 00_utils.ipynb           # Shared utility functions and centralised evaluation
├── 01_bronze_ingestion/     # Raw data ingestion (SCADA telemetry, event logs, transformer metadata, meteorological data)
├── 02_silver_cleaning/      # Data type correction and table preparation
└── 03_gold_features/        # Feature engineering, label definition, and train/test split
```
---

## Environment

| Component | Version |
|-----------|---------|
| Databricks Runtime | 18.1 ML (Photon) |
| Apache Spark | 4.1.0 |
| Python | 3.12.3 |
| PyTorch | 2.9.0+cpu |
| XGBoost | 3.1.1 |
| scikit-learn | 1.6.1 |

Worker type: `Standard_D16ds_v5` (64 GB RAM, 16 cores), autoscaling 2–8 nodes.  
Training is **CPU-only** (GPU quota unavailable in Azure West Europe at time of development).

---

## Data

The underlying data is **not included** in this repository. It consists of proprietary SCADA telemetry, transformer metadata and SCADA event logs belonging to E-REDES and is not publicly available.


---

## License

This work was produced under a CGI / E-REDES academic partnership. The code is made available for reference purposes. Please contact the author before reusing or adapting it for other projects.
