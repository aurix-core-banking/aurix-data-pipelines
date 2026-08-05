import os
import sys

# Adiciona a raiz do repositório ao sys.path para importar DAGs e módulos
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dags"))

os.environ.setdefault("AIRFLOW_HOME", os.path.join(os.getcwd(), "airflow"))
