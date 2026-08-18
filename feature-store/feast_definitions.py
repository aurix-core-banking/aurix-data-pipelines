"""
Feast Feature Store — Features ML para Aurix Platform.

Features pré-computadas para:
- Credit scoring (risco de crédito)
- Fraud detection (detecção de fraude)
- Customer segmentation (segmentação de clientes)
- Churn prediction (previsão de evasão)

Uso:
  python feature-store/feast_definitions.py apply
  python feature-store/feast_definitions.py serve
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

# Feast imports (com fallback para sem dependência)
try:
    from feast import FeatureStore, Entity, Feature, ValueType, RepoConfig
    from feast.data_source import KafkaSource, BatchSource
    from feast.data_format import AvroFormat
    from feast.feature_view import FeatureView
    from feast.infra.offline_stores.contrib.postgres.postgres_offline_store import (
        PostgresOfflineStoreConfig,
    )
    from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
    from feast.repo_config import RegistryConfig
    HAS_FEAST = True
except ImportError:
    HAS_FEAST = False
    print("Feast não instalado — usando definições estáticas")


# ═══════════════════════════════════════════════════════════
# Entity Definitions
# ═══════════════════════════════════════════════════════════

if HAS_FEAST:
    # Entity: Cliente
    cliente = Entity(
        name="cliente_id",
        value_type=ValueType.INT64,
        description="ID único do cliente Aurix",
    )

    # Entity: Conta
    conta = Entity(
        name="conta_id",
        value_type=ValueType.INT64,
        description="ID único da conta bancária",
    )

    # Entity: Transação
    transacao = Entity(
        name="transacao_id",
        value_type=ValueType.STRING,
        description="ID da transação (UUID)",
    )

    # ═══════════════════════════════════════════════════════
    # Feature Views — Credit Scoring
    # ═══════════════════════════════════════════════════════
    credit_features = FeatureView(
        name="credit_features",
        entities=["conta_id"],
        ttl=timedelta(days=1),
        features=[
            Feature(name="saldo_atual", dtype=ValueType.FLOAT),
            Feature(name="saldo_medio_30d", dtype=ValueType.FLOAT),
            Feature(name="saldo_min_30d", dtype=ValueType.FLOAT),
            Feature(name="saldo_max_30d", dtype=ValueType.FLOAT),
            Feature(name="total_transacoes_30d", dtype=ValueType.INT32),
            Feature(name="volume_transacoes_30d", dtype=ValueType.FLOAT),
            Feature(name="ticket_medio_30d", dtype=ValueType.FLOAT),
            Feature(name="dias_desde_ultima_transacao", dtype=ValueType.INT32),
            Feature(name="qtd_contas", dtype=ValueType.INT32),
            Feature(name="limite_total", dtype=ValueType.FLOAT),
            Feature(name="utilizacao_limite_pct", dtype=ValueType.FLOAT),
            Feature(name="score_risco medio_7d", dtype=ValueType.FLOAT),
        ],
        online=True,
        source=BatchSource(
            path="s3://aurix-gold/credit_features/",
            event_timestamp_column="event_timestamp",
        ),
    )

    # ═══════════════════════════════════════════════════════
    # Feature Views — Fraud Detection
    # ═══════════════════════════════════════════════════════
    fraud_features = FeatureView(
        name="fraud_features",
        entities=["transacao_id"],
        ttl=timedelta(hours=1),
        features=[
            Feature(name="valor_transacao", dtype=ValueType.FLOAT),
            Feature(name="hora_do_dia", dtype=ValueType.INT32),
            Feature(name="dia_da_semana", dtype=ValueType.INT32),
            Feature(name="is_fim_de_semana", dtype=ValueType.BOOL),
            Feature(name="is_horario_comercial", dtype=ValueType.BOOL),
            Feature(name="transacoes_ultima_hora", dtype=ValueType.INT32),
            Feature(name="valor_acumulado_1h", dtype=ValueType.FLOAT),
            Feature(name="distancia_ultima_transacao_km", dtype=ValueType.FLOAT),
            Feature(name="score_fraude_anterior", dtype=ValueType.FLOAT),
            Feature(name="qtd_dispositivos_30d", dtype=ValueType.INT32),
            Feature(name="is_novo_dispositivo", dtype=ValueType.BOOL),
            Feature(name="risco_canal", dtype=ValueType.FLOAT),
        ],
        online=True,
        source=BatchSource(
            path="s3://aurix-gold/fraud_features/",
            event_timestamp_column="event_timestamp",
        ),
    )

    # ═══════════════════════════════════════════════════════
    # Feature Views — Customer Segmentation
    # ═══════════════════════════════════════════════════════
    customer_features = FeatureView(
        name="customer_features",
        entities=["cliente_id"],
        ttl=timedelta(days=7),
        features=[
            Feature(name="idade", dtype=ValueType.INT32),
            Feature(name="tempo_como_cliente_dias", dtype=ValueType.INT32),
            Feature(name="qtd_contas", dtype=ValueType.INT32),
            Feature(name="saldo_total", dtype=ValueType.FLOAT),
            Feature(name="volume_mensal_transacoes", dtype=ValueType.FLOAT),
            Feature(name="frequencia_transacoes_semanal", dtype=ValueType.FLOAT),
            Feature(name="tipos_produto_contratados", dtype=ValueType.INT32),
            Feature(name="tem_emprestimo", dtype=ValueType.BOOL),
            Feature(name="tem_cartao", dtype=ValueType.BOOL),
            Feature(name="tem_investimento", dtype=ValueType.BOOL),
            Feature(name="risco_score", dtype=ValueType.FLOAT),
            Feature name="classe_risco", dtype=ValueType.STRING),
        ],
        online=True,
        source=BatchSource(
            path="s3://aurix-gold/customer_features/",
            event_timestamp_column="event_timestamp",
        ),
    )

    # ═══════════════════════════════════════════════════════
    # Feature Views — Churn Prediction
    # ═══════════════════════════════════════════════════════
    churn_features = FeatureView(
        name="churn_features",
        entities=["cliente_id"],
        ttl=timedelta(days=1),
        features=[
            Feature(name="dias_desde_ultimo_login", dtype=ValueType.INT32),
            Feature(name="dias_desde_ultima_transacao", dtype=ValueType.INT32),
            Feature(name="transacoes_ultimo_mes", dtype=ValueType.INT32),
            Feature(name="transacoes_mes_anterior", dtype=ValueType.INT32),
            Feature(name="variacao_transacoes_pct", dtype=ValueType.FLOAT),
            Feature(name="saldo_medio_90d", dtype=ValueType.FLOAT),
            Feature(name="saldo_atual", dtype=ValueType.FLOAT),
            Feature(name="variacao_saldo_pct", dtype=ValueType.FLOAT),
            Feature(name="qtd_reclamacoes_90d", dtype=ValueType.INT32),
            Feature(name="tempo_medio_resposta_min", dtype=ValueType.FLOAT),
            Feature(name="canais_utilizados", dtype=ValueType.INT32),
            Feature(name="score_satisfacao", dtype=ValueType.FLOAT),
        ],
        online=True,
        source=BatchSource(
            path="s3://aurix-gold/churn_features/",
            event_timestamp_column="event_timestamp",
        ),
    )

    # ═══════════════════════════════════════════════════════
    # Configuração do Feature Store
    # ═══════════════════════════════════════════════════════
    feature_store_config = RepoConfig(
        project="aurix",
        registry="s3://aurix-warehouse/feast/registry.db",
        provider="local",
        offline_store=PostgresOfflineStoreConfig(
            host=os.getenv("PG_HOST", "localhost"),
            port=int(os.getenv("PG_PORT", "5432")),
            database=os.getenv("PG_DB", "aurix"),
            user=os.getenv("PG_USER", "aurix"),
            password=os.getenv("PG_PASSWORD", "aurix"),
        ),
        online_store=SqliteOnlineStoreConfig(
            path="data/online_store.db",
        ),
        entity_key_serialization_version=2,
    )


# ═══════════════════════════════════════════════════════════
# Feature Generation SQL (para Airflow materializar)
# ═══════════════════════════════════════════════════════════

CREDIT_FEATURES_SQL = """
-- Credit Features: contas + transações últimos 30 dias
SELECT
    c.id as conta_id,
    NOW() as event_timestamp,
    c.saldo as saldo_atual,
    COALESCE(AVG(t.valor) OVER (
        PARTITION BY c.id ORDER BY t.data_transacao
        RANGE BETWEEN INTERVAL '30 days' PRECEDING AND CURRENT ROW
    ), 0) as saldo_medio_30d,
    COALESCE(MIN(t.valor) OVER w30, 0) as saldo_min_30d,
    COALESCE(MAX(t.valor) OVER w30, 0) as saldo_max_30d,
    COUNT(t.id) OVER w30 as total_transacoes_30d,
    COALESCE(SUM(t.valor) OVER w30, 0) as volume_transacoes_30d,
    COALESCE(AVG(t.valor) OVER w30, 0) as ticket_medio_30d,
    EXTRACT(DAY FROM NOW() - MAX(t.data_transacao)) as dias_desde_ultima_transacao,
    (SELECT COUNT(*) FROM contas c2 WHERE c2.cliente_id = c.cliente_id) as qtd_contas,
    c.limite as limite_total,
    CASE WHEN c.limite > 0 THEN (c.saldo / c.limite * 100) ELSE 0 END as utilizacao_limite_pct,
    (SELECT AVG(score_risco) FROM transacoes t2
     WHERE t2.conta_id = c.id AND t2.data_transacao > NOW() - INTERVAL '7 days'
    ) as score_risco_medio_7d
FROM contas c
LEFT JOIN transacoes t ON t.conta_id = c.id
WHERE c.status = 'ATIVA'
WINDOW w30 AS (PARTITION BY c.id ORDER BY t.data_transacao
               RANGE BETWEEN INTERVAL '30 days' PRECEDING AND CURRENT ROW)
"""

FRAUD_FEATURES_SQL = """
-- Fraud Features: transações + contexto
SELECT
    t.id as transacao_id,
    t.data_transacao as event_timestamp,
    t.valor as valor_transacao,
    EXTRACT(HOUR FROM t.data_transacao) as hora_do_dia,
    EXTRACT(DOW FROM t.data_transacao) as dia_da_semana,
    CASE WHEN EXTRACT(DOW FROM t.data_transacao) IN (0, 6) THEN true ELSE false END as is_fim_de_semana,
    CASE WHEN EXTRACT(HOUR FROM t.data_transacao) BETWEEN 8 AND 18 THEN true ELSE false END as is_horario_comercial,
    (SELECT COUNT(*) FROM transacoes t2
     WHERE t2.conta_id = t.conta_id
       AND t2.data_transacao > t.data_transacao - INTERVAL '1 hour'
       AND t2.id != t.id) as transacoes_ultima_hora,
    (SELECT COALESCE(SUM(valor), 0) FROM transacoes t2
     WHERE t2.conta_id = t.conta_id
       AND t2.data_transacao > t.data_transacao - INTERVAL '1 hour'
       AND t2.id != t.id) as valor_acumulado_1h,
    t.score_risco as score_fraude_anterior,
    t.aprovada as is_aprovada
FROM transacoes t
WHERE t.data_transacao > NOW() - INTERVAL '24 hours'
"""

CUSTOMER_FEATURES_SQL = """
-- Customer Features: clientes + contas + transações
SELECT
    cl.id as cliente_id,
    NOW() as event_timestamp,
    EXTRACT(YEAR FROM AGE(cl.data_nascimento)) as idade,
    EXTRACT(DAY FROM NOW() - cl.created_at) as tempo_como_cliente_dias,
    COUNT(DISTINCT c.id) as qtd_contas,
    COALESCE(SUM(c.saldo), 0) as saldo_total,
    (SELECT COUNT(*) FROM transacoes t
     WHERE t.conta_id IN (SELECT id FROM contas WHERE cliente_id = cl.id)
       AND t.data_transacao > NOW() - INTERVAL '30 days'
    ) as volume_mensal_transacoes,
    (SELECT COUNT(*) FROM transacoes t
     WHERE t.conta_id IN (SELECT id FROM contas WHERE cliente_id = cl.id)
       AND t.data_transacao > NOW() - INTERVAL '7 days'
    ) / 7.0 as frequencia_transacoes_semanal,
    cl.score_risco as risco_score,
    CASE
        WHEN cl.score_risco >= 800 THEN 'A'
        WHEN cl.score_risco >= 600 THEN 'B'
        WHEN cl.score_risco >= 400 THEN 'C'
        ELSE 'D'
    END as classe_risco
FROM clientes cl
LEFT JOIN contas c ON c.cliente_id = cl.id
WHERE cl.status = 'ATIVO'
GROUP BY cl.id, cl.data_nascimento, cl.created_at, cl.score_risco
"""

CHURN_FEATURES_SQL = """
-- Churn Features: atividade recente vs histórico
SELECT
    cl.id as cliente_id,
    NOW() as event_timestamp,
    EXTRACT(DAY FROM NOW() - (
        SELECT MAX(t.data_transacao) FROM transacoes t
        WHERE t.conta_id IN (SELECT id FROM contas WHERE cliente_id = cl.id)
    )) as dias_desde_ultima_transacao,
    (SELECT COUNT(*) FROM transacoes t
     WHERE t.conta_id IN (SELECT id FROM contas WHERE cliente_id = cl.id)
       AND t.data_transacao > NOW() - INTERVAL '30 days'
    ) as transacoes_ultimo_mes,
    (SELECT COUNT(*) FROM transacoes t
     WHERE t.conta_id IN (SELECT id FROM contas WHERE cliente_id = cl.id)
       AND t.data_transacao BETWEEN NOW() - INTERVAL '60 days' AND NOW() - INTERVAL '30 days'
    ) as transacoes_mes_anterior,
    cl.score_risco as score_satisfacao
FROM clientes cl
WHERE cl.status = 'ATIVO'
"""


# ═══════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════

def print_definitions():
    """Imprime definições de features sem Feast."""
    print("=" * 60)
    print("Aurix Feature Store — Definições")
    print("=" * 60)

    definitions = {
        "Credit Features (credit_features)": [
            "saldo_atual", "saldo_medio_30d", "saldo_min_30d", "saldo_max_30d",
            "total_transacoes_30d", "volume_transacoes_30d", "ticket_medio_30d",
            "dias_desde_ultima_transacao", "qtd_contas", "limite_total",
            "utilizacao_limite_pct", "score_risco_medio_7d",
        ],
        "Fraud Features (fraud_features)": [
            "valor_transacao", "hora_do_dia", "dia_da_semana",
            "is_fim_de_semana", "is_horario_comercial",
            "transacoes_ultima_hora", "valor_acumulado_1h",
            "distancia_ultima_transacao_km", "score_fraude_anterior",
            "qtd_dispositivos_30d", "is_novo_dispositivo", "risco_canal",
        ],
        "Customer Features (customer_features)": [
            "idade", "tempo_como_cliente_dias", "qtd_contas", "saldo_total",
            "volume_mensal_transacoes", "frequencia_transacoes_semanal",
            "tipos_produto_contratados", "tem_emprestimo", "tem_cartao",
            "tem_investimento", "risco_score", "classe_risco",
        ],
        "Churn Features (churn_features)": [
            "dias_desde_ultimo_login", "dias_desde_ultima_transacao",
            "transacoes_ultimo_mes", "transacoes_mes_anterior",
            "variacao_transacoes_pct", "saldo_medio_90d", "saldo_atual",
            "variacao_saldo_pct", "qtd_reclamacoes_90d",
            "tempo_medio_resposta_min", "canais_utilizados", "score_satisfacao",
        ],
    }

    for view_name, features in definitions.items():
        print(f"\n  {view_name}:")
        for feat in features:
            print(f"    → {feat}")

    print(f"\n  Total: {sum(len(f) for f in definitions.values())} features em 4 feature views")

    print("\n  SQL de materialização:")
    print("    → credit_features:     s3://aurix-gold/credit_features/")
    print("    → fraud_features:      s3://aurix-gold/fraud_features/")
    print("    → customer_features:   s3://aurix-gold/customer_features/")
    print("    → churn_features:      s3://aurix-gold/churn_features/")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("action", nargs="?", default="show", choices=["apply", "serve", "show"])
    args = parser.parse_args()

    if args.action == "show" or not HAS_FEAST:
        print_definitions()
    elif args.action == "apply" and HAS_FEAST:
        store = FeatureStore(repo_path=".")
        store.apply([
            cliente, conta, transacao,
            credit_features, fraud_features, customer_features, churn_features,
        ])
        print("Feature definitions applied!")
    elif args.action == "serve" and HAS_FEAST:
        print("Starting Feature Server on port 6566...")
        os.system("feast serve -h 0.0.0.0 -p 6566")
