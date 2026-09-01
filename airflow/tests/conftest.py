import os
import sys

# Adiciona a raiz do repositório (aurix-data-pipelines) ao sys.path para
# importar DAGs e módulos das pipelines (reconciliation, ingestion, etc.)
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
AIRFLOW_DIR = os.path.join(REPO_ROOT, "airflow")
sys.path.insert(0, REPO_ROOT)
sys.path.insert(0, AIRFLOW_DIR)
sys.path.insert(0, os.path.join(AIRFLOW_DIR, "dags"))

os.environ.setdefault("AIRFLOW_HOME", AIRFLOW_DIR)
