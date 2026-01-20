# 🚀 Big Data Platform – Python

Plateforme Big Data développée en **Python**, combinant **traitement distribué**, **orchestration de pipelines**, **API backend** et **visualisation interactive**.

Ce projet vise à ingérer, transformer, stocker et exposer des données à grande échelle via une architecture moderne et modulaire.

---

## 🧱 Stack technique

### 🔧 Backend & Data
- Python 3.10+
- PySpark 3.5.0
- Pandas / PyArrow
- Prefect
- MinIO
- MongoDB

### 🌐 API & Visualisation
- FastAPI
- Uvicorn
- Streamlit
- Plotly

### 🧪 Utilitaires
- Faker
- python-dotenv
- Requests

---

## 📁 Structure du projet

```
.
├── api/
├── pipelines/
├── spark/
├── dashboards/
├── data/
├── config/
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## ⚙️ Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## ▶️ Lancer les services

### API
```bash
uvicorn api.main:app --reload
```

### Streamlit
```bash
streamlit run dashboards/app.py
```

### Prefect
```bash
prefect server start
```

---

## 📜 Licence
Projet à usage pédagogique / expérimental.