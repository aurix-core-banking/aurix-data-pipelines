"""Testes das DAGs do Airflow (dagbag + estrutura).

Observação: a pasta local `airflow/` deste repositório cria um namespace
package. Os testes de dagbag só são executados quando o apache-airflow real
está instalado no ambiente (verifica um submódulo distinto, `airflow.models`).
"""

import importlib
import os
import sys

import pytest

DAGS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dags")

DAG_ARQUIVOS = [
    "ingest_postgres_to_bronze.py",
    "bronze_to_silver_dbt.py",
    "silver_to_gold_dbt.py",
    "sync_clickhouse.py",
    "compliance_lgpd.py",
    "reconciliacao_contabil.py",
    "market_data_ingestion.py",
    "timescaledb_ingestion.py",
]


def _tem_airflow_real() -> bool:
    """Detecta o apache-airflow de verdade (com airflow.models)."""
    try:
        import airflow.models  # noqa: F401
        import airflow.operators.python  # noqa: F401
        return True
    except Exception:  # noqa: BLE001
        return False


REQUER_AIRFLOW = pytest.mark.skipif(
    not _tem_airflow_real(),
    reason="apache-airflow não instalado no ambiente",
)


def _dagbag():
    from airflow.models import DagBag

    sys.path.insert(0, DAGS_DIR)
    return DagBag(dag_folder=DAGS_DIR, include_examples=False)


@REQUER_AIRFLOW
def test_dagbag_importa_todas_as_dags():
    dagbag = _dagbag()
    erros = {k: v for k, v in dagbag.import_errors.items() if not k.endswith("alertas_aurix.py")}
    assert not erros, f"Erros de importação no dagbag: {erros}"
    for arquivo in DAG_ARQUIVOS:
        assert os.path.exists(os.path.join(DAGS_DIR, arquivo)), f"DAG ausente: {arquivo}"


@REQUER_AIRFLOW
@pytest.mark.parametrize("arquivo", DAG_ARQUIVOS)
def test_dag_possui_dag_id_esperado(arquivo):
    dagbag = _dagbag()
    dag_id_esperado = arquivo.replace(".py", "")
    ids = {d.dag_id for d in dagbag.dags.values()}
    assert dag_id_esperado in ids, f"DAG {dag_id_esperado} não encontrada em {arquivo}"


@REQUER_AIRFLOW
@pytest.mark.parametrize("arquivo", DAG_ARQUIVOS)
def test_dag_define_alertas(arquivo):
    dagbag = _dagbag()
    dag = dagbag.dags.get(arquivo.replace(".py", ""))
    assert dag is not None, f"DAG não encontrada: {arquivo}"
    callbacks = dag.default_args.get("on_failure_callback")
    assert callbacks is not None, f"DAG {arquivo} sem on_failure_callback de alerta"


@REQUER_AIRFLOW
@pytest.mark.parametrize(
    "arquivo,chaves_esperadas",
    [
        ("sync_clickhouse.py", ["sincronizar_clickhouse"]),
        ("compliance_lgpd.py", ["gerar_relatorio_compliance", "purgar_dados_expirados"]),
        (
            "reconciliacao_contabil.py",
            ["reconciliar_saldos", "reconciliar_transacoes", "reconciliar_pix"],
        ),
        ("silver_to_gold_dbt.py", ["dbt_run_gold", "dbt_test_gold"]),
        ("bronze_to_silver_dbt.py", ["dbt_run_silver", "dbt_test_silver"]),
    ],
)
def test_dag_tem_tarefas_esperadas(arquivo, chaves_esperadas):
    dagbag = _dagbag()
    dag = dagbag.dags.get(arquivo.replace(".py", ""))
    assert dag is not None
    task_ids = set(dag.task_dict.keys())
    for chave in chaves_esperadas:
        assert chave in task_ids, f"Tarefa {chave} ausente em {arquivo}"


@pytest.mark.parametrize("arquivo", DAG_ARQUIVOS)
def test_dag_arquivo_sintaxe_valida(arquivo):
    caminho = os.path.join(DAGS_DIR, arquivo)
    with open(caminho, encoding="utf-8") as f:
        compile(f.read(), caminho, "exec")


@pytest.mark.parametrize(
    "modulo,funs",
    [
        ("ingestion.pg_to_bronze", ["main"]),
        ("compliance.data_compliance", ["LGPDCompliance", "DataAuditor"]),
        ("sync.postgres_to_clickhouse", ["PostgresToClickHouseSync"]),
    ],
)
def test_modulos_pipeline_importam(modulo, funs):
    for dep in ("pandas", "psycopg2", "clickhouse_connect"):
        try:
            __import__(dep)
        except ImportError:
            pytest.skip(f"dependência {dep} não instalada")
    mod = importlib.import_module(modulo)
    for fun in funs:
        assert hasattr(mod, fun), f"{fun} ausente em {modulo}"
