"""Módulo de alertas para as DAGs do Airflow (Slack e e-mail)."""

import logging
import os
from datetime import datetime
from typing import Any, Dict

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None

log = logging.getLogger(__name__)

SLACK_WEBHOOK_ENV = "SLACK_WEBHOOK_URL"
EMAIL_TO_ENV = "ALERTAS_EMAIL_TO"


def _webhook_url() -> str:
    return os.environ.get(SLACK_WEBHOOK_ENV, "").strip()


def _destinatarios_email() -> str:
    return os.environ.get(EMAIL_TO_ENV, "").strip()


def notificar_slack(texto: str) -> bool:
    """Envia mensagem ao Slack via webhook. Retorna False se não configurado."""
    url = _webhook_url()
    if not url or requests is None:
        log.info("Slack não configurado (%s). Mensagem: %s", SLACK_WEBHOOK_ENV, texto)
        return False
    try:
        resp = requests.post(url, json={"text": texto}, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:  # noqa: BLE001
        log.warning("Falha ao notificar Slack: %s", e)
        return False


def notificar_email(assunto: str, corpo: str) -> bool:
    """Envia e-mail via configuração SMTP do Airflow, se configurado."""
    para = _destinatarios_email()
    if not para:
        return False
    try:
        from airflow.utils.email import send_email

        send_email(to=para.split(","), subject=assunto, html_content=f"<pre>{corpo}</pre>")
        return True
    except Exception as e:  # noqa: BLE001
        log.warning("Falha ao notificar e-mail: %s", e)
        return False


def notificar_falha(context: Dict[str, Any]) -> None:
    """Callback de falha de tarefa: alerta Slack + e-mail."""
    ti = context.get("task_instance")
    dag_id = getattr(context.get("dag"), "dag_id", getattr(ti, "dag_id", "desconhecida"))
    task_id = getattr(ti, "task_id", "desconhecida")
    log_url = getattr(ti, "log_url", "")
    exception = context.get("exception")

    texto = (
        f":red_circle: Falha na DAG `{dag_id}`\n"
        f"- Tarefa: `{task_id}`\n"
        f"- Execução: {context.get('execution_date', '')}\n"
        f"- Erro: `{exception}`\n"
        f"- Log: {log_url}"
    )
    notificar_slack(texto)
    notificar_email(f"[Aurix] Falha na DAG {dag_id}", texto)


def notificar_sucesso(context: Dict[str, Any]) -> None:
    """Callback de sucesso de tarefa: alerta Slack."""
    dag_id = getattr(context.get("dag"), "dag_id", "desconhecida")
    task_id = getattr(context.get("task_instance"), "task_id", "desconhecida")
    texto = (
        f":white_check_mark: Tarefa `{task_id}` da DAG `{dag_id}` concluída "
        f"em {datetime.now().isoformat()}"
    )
    notificar_slack(texto)
