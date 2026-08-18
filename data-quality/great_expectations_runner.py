"""
Great Expectations Runner — Executa data quality checks em todas as tabelas.
Gera relatório JSON + alertas para Slack/Prometheus.

Uso:
  python data-quality/great_expectations_runner.py
  python data-quality/great_expectations_runner.py --tables contas,transacoes
"""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import psycopg2
import yaml


class DataQualityEngine:
    """Engine de validação de data quality sem depender do Great Expectations (lightweight)."""

    def __init__(self, pg_config: dict = None):
        self.pg_config = pg_config or {
            "host": os.getenv("PG_HOST", "localhost"),
            "port": int(os.getenv("PG_PORT", "5432")),
            "dbname": os.getenv("PG_DB", "aurix"),
            "user": os.getenv("PG_USER", "aurix"),
            "password": os.getenv("PG_PASSWORD", "aurix"),
        }
        self.results = []
        self.suites = self._load_suites()

    def _load_suites(self) -> dict:
        """Carrega expectation suites do YAML."""
        suite_path = Path(__file__).parent / "great_expectations.yml"
        if not suite_path.exists():
            return {}
        with open(suite_path) as f:
            config = yaml.safe_load(f)
        return config.get("expectation_suites", {})

    def _connect(self):
        return psycopg2.connect(**self.pg_config)

    def _run_sql(self, conn, sql: str) -> list:
        cur = conn.cursor()
        cur.execute(sql)
        result = cur.fetchall()
        cur.close()
        return result

    # ═══ Expectation Implementations ═══

    def _check_not_null(self, conn, table: str, column: str) -> dict:
        sql = f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL"
        count = self._run_sql(conn, sql)[0][0]
        total = self._run_sql(conn, f"SELECT COUNT(*) FROM {table}")[0][0]
        passed = count == 0
        return {
            "expectation": "expect_column_values_to_not_be_null",
            "column": column,
            "table": table,
            "passed": passed,
            "null_count": count,
            "total_rows": total,
            "details": f"{count} valores nulos encontrados" if not passed else "OK",
        }

    def _check_unique(self, conn, table: str, column: str) -> dict:
        sql = f"SELECT {column}, COUNT(*) as cnt FROM {table} GROUP BY {column} HAVING COUNT(*) > 1"
        dupes = self._run_sql(conn, sql)
        passed = len(dupes) == 0
        return {
            "expectation": "expect_column_values_to_be_unique",
            "column": column,
            "table": table,
            "passed": passed,
            "duplicate_count": len(dupes),
            "sample_duplicates": [str(d[0]) for d in dupes[:5]],
            "details": f"{len(dupes)} valores duplicados" if not passed else "OK",
        }

    def _check_in_set(self, conn, table: str, column: str, value_set: list) -> dict:
        values_str = ", ".join(f"'{v}'" for v in value_set)
        sql = f"SELECT DISTINCT {column} FROM {table} WHERE {column} NOT IN ({values_str})"
        invalid = self._run_sql(conn, sql)
        passed = len(invalid) == 0
        return {
            "expectation": "expect_column_values_to_be_in_set",
            "column": column,
            "table": table,
            "passed": passed,
            "invalid_values": [str(v[0]) for v in invalid[:10]],
            "valid_set": value_set,
            "details": f"Valores inválidos: {[str(v[0]) for v in invalid]}" if not passed else "OK",
        }

    def _check_regex(self, conn, table: str, column: str, regex: str) -> dict:
        sql = f"SELECT COUNT(*) FROM {table} WHERE {column}::text !~ '{regex}'"
        count = self._run_sql(conn, sql)[0][0]
        passed = count == 0
        return {
            "expectation": "expect_column_values_to_match_regex",
            "column": column,
            "table": table,
            "passed": passed,
            "non_matching_count": count,
            "regex": regex,
            "details": f"{count} valores não correspondem ao regex" if not passed else "OK",
        }

    def _check_between(self, conn, table: str, column: str,
                       min_value: float, max_value: float) -> dict:
        sql = f"SELECT COUNT(*) FROM {table} WHERE {column} < {min_value} OR {column} > {max_value}"
        count = self._run_sql(conn, sql)[0][0]
        passed = count == 0
        return {
            "expectation": "expect_column_values_to_be_between",
            "column": column,
            "table": table,
            "passed": passed,
            "out_of_range_count": count,
            "min_value": min_value,
            "max_value": max_value,
            "details": f"{count} valores fora do range [{min_value}, {max_value}]" if not passed else "OK",
        }

    def _check_row_count(self, conn, table: str, min_value: int, max_value: int) -> dict:
        sql = f"SELECT COUNT(*) FROM {table}"
        count = self._run_sql(conn, sql)[0][0]
        passed = min_value <= count <= max_value
        return {
            "expectation": "expect_table_row_count_to_be_between",
            "table": table,
            "passed": passed,
            "row_count": count,
            "min_value": min_value,
            "max_value": max_value,
            "details": f"Row count: {count}" if passed else f"Row count {count} fora do range",
        }

    # ═══ Runner ═══

    def run_suite(self, table: str, suite: dict) -> list:
        """Executa todas as expectations de uma suite."""
        results = []
        conn = self._connect()

        try:
            for exp in suite.get("expectations", []):
                exp_type = exp["expectation_type"]
                kwargs = exp.get("kwargs", {})

                try:
                    if exp_type == "expect_column_values_to_not_be_null":
                        results.append(self._check_not_null(conn, table, kwargs["column"]))
                    elif exp_type == "expect_column_values_to_be_unique":
                        results.append(self._check_unique(conn, table, kwargs["column"]))
                    elif exp_type == "expect_column_values_to_be_in_set":
                        results.append(self._check_in_set(conn, table, kwargs["column"], kwargs["value_set"]))
                    elif exp_type == "expect_column_values_to_match_regex":
                        results.append(self._check_regex(conn, table, kwargs["column"], kwargs["regex"]))
                    elif exp_type == "expect_column_values_to_be_between":
                        results.append(self._check_between(conn, table, kwargs["column"],
                                                           kwargs["min_value"], kwargs["max_value"]))
                    elif exp_type == "expect_table_row_count_to_be_between":
                        results.append(self._check_row_count(conn, table,
                                                             kwargs["min_value"], kwargs["max_value"]))
                except Exception as e:
                    results.append({
                        "expectation": exp_type,
                        "table": table,
                        "passed": False,
                        "error": str(e),
                    })
        finally:
            conn.close()

        return results

    def run_all(self, tables: list = None) -> dict:
        """Executa todas as suites e gera relatório."""
        print("=" * 60)
        print("Aurix Data Quality Engine")
        print("=" * 60)

        start = time.time()
        all_results = {}

        suites_to_run = tables if tables else list(self.suites.keys())

        for table in suites_to_run:
            if table not in self.suites:
                print(f"  ⚠ Suite '{table}' não encontrada, pulando...")
                continue

            suite = self.suites[table]
            results = self.run_suite(table, suite)
            all_results[table] = results

            passed = sum(1 for r in results if r["passed"])
            total = len(results)
            status = "✅" if passed == total else "❌"
            print(f"\n  {status} {table}: {passed}/{total} passed")

            for r in results:
                if not r["passed"]:
                    print(f"    ❌ {r['expectation']}: {r.get('details', 'FAILED')}")

        # Gerar relatório
        total_checks = sum(len(v) for v in all_results.values())
        total_passed = sum(
            sum(1 for r in v if r["passed"]) for v in all_results.values()
        )
        elapsed = time.time() - start

        report = {
            "timestamp": datetime.now().isoformat(),
            "total_checks": total_checks,
            "total_passed": total_passed,
            "total_failed": total_checks - total_passed,
            "pass_rate": (total_passed / total_checks * 100) if total_checks > 0 else 0,
            "elapsed_seconds": round(elapsed, 2),
            "suites": all_results,
        }

        # Salvar relatório
        report_dir = Path(__file__).parent / "reports"
        report_dir.mkdir(exist_ok=True)
        report_path = report_dir / f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)

        # Salvar latest
        latest_path = report_dir / "latest_report.json"
        with open(latest_path, "w") as f:
            json.dump(report, f, indent=2, default=str)

        print(f"\n{'=' * 60}")
        print(f"Total: {total_passed}/{total_checks} passed "
              f"({report['pass_rate']:.1f}%) em {elapsed:.1f}s")
        print(f"Relatório: {report_path}")

        return report


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--tables", help="Comma-separated tables to check")
    args = parser.parse_args()

    tables = args.tables.split(",") if args.tables else None
    engine = DataQualityEngine()
    report = engine.run_all(tables)
    sys.exit(0 if report["total_failed"] == 0 else 1)
