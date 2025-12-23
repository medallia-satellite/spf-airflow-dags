import functools
import json
import pprint
from collections import defaultdict
from typing import TypedDict, Optional, Any, List
import datetime
from dateutil.relativedelta import relativedelta

import re

from airflow.decorators import dag, task_group, task
from airflow.models import Param
from airflow.operators.python import get_current_context
from airflow.providers.http.hooks.http import HttpHook


BASE_PATTERN = r"(\w+)_topic-builder(-\w+)+(\.\w{2,4}){0,2}(\.\w+)(\.\w{2,4}){1,2}-\1"
BASE_REGEX = re.compile(BASE_PATTERN)
INDEX_PATTERN = (
    rf"^seaas-{BASE_PATTERN}"
    + r"-(?P<month>[0-9]{4}-[0-9]{2}-[0-9]{2})-(?P<tenant_id>[0-9]+)-(?P<suffix>[0-9]+)$"
)
INDEX_REGEX = re.compile(INDEX_PATTERN)

ALIAS_REGEX_MAPPING = {
    "read": re.compile(rf"{BASE_PATTERN}"),
    "write": re.compile(rf"{BASE_PATTERN}" + r"-[0-9]{4}-[0-9]{2}-[0-9]{2}"),
    "rollover": re.compile(rf"{BASE_PATTERN}-rollover"),
}
POLICY_MAPPING = {
    "M6": 6,
    "M6_rollover": 6,
    "M18": 18,
    "M18_rollover": 18,
    "M36": 36,
    "M36_rollover": 36,
}


def expected_index_template(tenant, retention_months):
    return {
        "index_patterns": [f"seaas-{tenant}-*"],
        "template": {
            "settings": {
                "index": {
                    "lifecycle": {
                        "name": f"M{retention_months}_rollover",
                        "rollover_alias": f"{tenant}-rollover",
                    },
                    "analysis": {
                        "filter": {
                            "compound_capture": {
                                "type": "pattern_capture",
                                "preserve_original": "false",
                                "patterns": ["(!?[^@!@]+)@!@"],
                            }
                        },
                        "analyzer": {
                            "topic-builder-analyzer": {
                                "filter": ["compound_capture"],
                                "type": "custom",
                                "tokenizer": "whitespace",
                            }
                        },
                    },
                    "number_of_shards": "1",
                    "number_of_replicas": "1",
                }
            },
            "mappings": {
                "properties": {
                    "comments": {
                        "type": "nested",
                        "properties": {
                            "language": {"type": "keyword"},
                            "linguisticConnections": {
                                "type": "text",
                                "analyzer": "topic-builder-analyzer",
                                "position_increment_gap": 1000,
                            },
                            "linguisticConnectionsIndexes": {"type": "short"},
                            "name": {"type": "keyword"},
                            "persona": {"type": "keyword"},
                            "sentenceContent": {
                                "type": "text",
                                "analyzer": "topic-builder-analyzer",
                            },
                            "sentenceIndex": {"type": "short"},
                            "wordEndIndexes": {"type": "integer"},
                            "wordStartIndexes": {"type": "integer"},
                        },
                    },
                    "responseDate": {"type": "date"},
                    "surveyId": {"type": "long"},
                }
            },
        },
    }


def extract_index_details(index):
    tenant = BASE_REGEX.search(index).group(0)
    m = INDEX_REGEX.fullmatch(index).groupdict()
    tenant_id = m["tenant_id"]
    suffix = m["suffix"]
    month = m["month"]
    should_rollover = datetime.date.fromisoformat(
        month
    ) == datetime.date.today().replace(day=1) + relativedelta(months=1)
    read_alias = ALIAS_REGEX_MAPPING["read"].search(index).group(0)
    write_alias = ALIAS_REGEX_MAPPING["write"].search(index).group(0)
    rollover_alias = f"{read_alias}-rollover"
    return {
        "tenant": tenant,
        "tenant_id": tenant_id,
        "suffix": suffix,
        "month": month,
        "read_alias": read_alias,
        "write_alias": write_alias,
        "rollover_alias": rollover_alias,
        "should_rollover": should_rollover,
    }


def chain_on_success(func):
    @functools.wraps(func)
    def wrapper(context):
        if not context["success"]:
            return context
        return func(context)

    return wrapper


def xcom_pull(task_id: str, key: str) -> Any:
    context = get_current_context()
    ti = context["ti"]
    print(f"xcom_pull {task_id} {key}")
    return ti.xcom_pull(task_ids=task_id, key=key)


def xcom_push(key: str, value: Any) -> None:
    context = get_current_context()
    ti = context["ti"]
    print(f"xcom_push {key} {value}")
    ti.xcom_push(key, value)


def generate_past_month_starts(n):
    current_month_start = datetime.datetime.today().replace(
        day=1, hour=0, minute=0, second=0, tzinfo=datetime.timezone.utc
    ) + relativedelta(months=1)
    return [current_month_start - relativedelta(months=i) for i in range(n)]


def http_hook_put(conn_id: str, endpoint: str, data: str):
    hook_put = HttpHook(method="PUT", http_conn_id=conn_id)
    response = hook_put.run(
        endpoint=f"/{endpoint}?pretty",
        headers={"Content-Type": "application/json"},
        data=data,
    )
    hook_put.check_response(response)
    return response.json()


def http_hook_post(conn_id: str, endpoint: str, data: str):
    hook_post = HttpHook(method="POST", http_conn_id=conn_id)
    response = hook_post.run(
        endpoint=f"/{endpoint}?pretty",
        headers={"Content-Type": "application/json"},
        data=data,
    )
    hook_post.check_response(response)
    return response.json()


def http_hook_get(conn_id: str, endpoint: str):
    hook_get = HttpHook(method="GET", http_conn_id=conn_id)
    response = hook_get.run(
        endpoint=endpoint,
        headers={"Accept": "application/json"},
    )
    hook_get.check_response(response)
    return response.json()


class Context(TypedDict, total=False):
    tenant: str
    tenant_id: int
    success: bool
    error: Optional[str]
    stage: Optional[str]
    value: Optional[Any]
    retention: Optional[int]


def success(context: Context, stage: str, value: Any = None) -> Context:
    return Context(
        tenant=context["tenant"],
        tenant_id=context["tenant_id"],
        success=True,
        stage=stage,
        value=value if value else context.get("value"),
        retention=context.get("retention"),
    )


def failure(context: Context, stage: str, error: Any = None) -> Context:
    return Context(
        tenant=context["tenant"],
        tenant_id=context["tenant_id"],
        success=False,
        stage=stage,
        error=error if error else context.get("error"),
        retention=context.get("retention"),
    )


@dag(
    dag_display_name="WIP2",
    tags=["spf", "test", "poc"],
    description="This DAG verifies monthly indices.",
    max_active_runs=1,
    catchup=False,
    params={"db_conn": Param("es-testing", type="string")},
)
def wiwip_dag():
    dry_run = True

    def _filter_and_push(results, filter_fn) -> None:
        for k, v in results.items():
            if filter_fn(k):
                xcom_push(k, v)

    @task
    def report(upstream: List[Context], stage: str) -> None:
        errors = [x for x in upstream if not x["success"]]
        print(
            f"""
        success: {len(upstream) - len(errors)}/{len(upstream)}
        errors: {len(errors)}/{len(upstream)}
        errors in stage {stage}: {len([e for e in errors if e["stage"] == stage])}/{len(errors)}
        """
        )
        for i, c in enumerate(upstream):
            if not c["success"] and c["stage"] == stage:
                print(f"{i}: {c['tenant']}")
                pprint.pprint(c)

    @task
    def fetch_indices_per_tenant(conn_id: str, stage: str = "") -> List[Context]:
        fetched = http_hook_get(conn_id, "/_cat/indices?h=index&format=json")
        print(fetched)
        results = defaultdict(list)
        for index in [r["index"] for r in fetched if INDEX_REGEX.match(r["index"])]:
            results[
                (
                    BASE_REGEX.search(index).group(0),
                    INDEX_REGEX.fullmatch(index).groupdict()["tenant_id"],
                )
            ].append(index)

        grouped = []
        for k, v in results.items():
            xcom_push(k[0], v)
            grouped.append(
                Context(tenant=k[0], tenant_id=k[1], stage=stage, success=True)
            )
        return grouped

    @task_group
    def ilm_settings(conn_id: str, upstream: List[Context]) -> List[Context]:
        tg_stage = "ilm_settings"

        @task
        def fetch(h: str) -> bool:
            results = http_hook_get(
                h, "/_settings/index.lifecycle.name,index.lifecycle.rollover_alias"
            )
            _filter_and_push(results, lambda x: INDEX_REGEX.match(x))
            return True

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = xcom_pull("fetch_indices_per_tenant", context["tenant"])
            il_list = []
            for index in indices:
                if s := xcom_pull("ilm_settings.fetch", index):
                    il_list.append(s["settings"]["index"]["lifecycle"])
                else:
                    print(f"No settings for {index}")

            if len(indices) != len(il_list):
                return failure(
                    context=context,
                    stage=tg_stage,
                    error="Some indices are missing ILM settings",
                )

            retention = list(set(POLICY_MAPPING.get(il.get("name")) for il in il_list))
            if len(retention) != 1 or retention[0] not in POLICY_MAPPING.values():
                return failure(
                    context=context,
                    stage=tg_stage,
                    error=f'Invalid policies: {set(il.get("name") for il in il_list)}',
                )

            rollover = set(il.get("rollover_alias") for il in il_list)
            if not all(r == f"{context['tenant']}-rollover" for r in rollover):
                return failure(
                    context=context,
                    stage=tg_stage,
                    error=f"Invalid rollover alias {rollover}",
                )

            return Context(
                tenant=context["tenant"],
                tenant_id=context["tenant_id"],
                stage=tg_stage,
                success=True,
                retention=retention[0],
            )

        verified = verify.expand(context=upstream)
        fetch(conn_id) >> verified
        report(upstream=verified, stage=tg_stage)
        return verified

    @task_group
    def index_templates(conn_id: str, upstream: List[Context]) -> List[Context]:
        tg_stage = "index_templates"

        @task
        def fetch(h: str) -> bool:
            results = http_hook_get(h, "/_index_template/*-rollover")
            _filter_and_push(
                {r["name"]: r["index_template"] for r in results["index_templates"]},
                lambda x: ALIAS_REGEX_MAPPING["rollover"].match(x),
            )
            return True

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            index_template = xcom_pull(
                "index_templates.fetch", f'{context["tenant"]}-rollover'
            )
            _ = index_template.pop("composed_of")

            if index_template != expected_index_template(
                tenant=context["tenant"], retention_months=context["retention"]
            ):
                return failure(
                    context=context,
                    stage=tg_stage,
                    error=f"Invalid index template: {index_template}",
                )

            return success(context=context, stage=tg_stage)

        verified = verify.expand(context=upstream)
        fetch(conn_id) >> verified
        report(upstream=verified, stage=tg_stage)
        return verified

    @task_group
    def monthly_indices(conn_id: str, upstream: List[Context]) -> List[Context]:
        tg_stage = "monthly_indices"

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = xcom_pull("fetch_indices_per_tenant", context["tenant"])
            missing = []
            for month_start in generate_past_month_starts(context["retention"]):
                if not any(
                    f'{context["tenant"]}-{month_start:%Y-%m-%d}' in index
                    for index in indices
                ):
                    missing.append(month_start)
            if missing:
                return failure(context=context, stage=tg_stage, error=missing)
            return success(context=context, stage=tg_stage)

        @task
        def fix(c: str, context: Context) -> Context:
            if context["success"] or context["stage"] != tg_stage:
                return context

            for month_start in context["error"]:
                index = f'seaas-{context["tenant"]}-{month_start:%Y-%m-%d}-{context["tenant_id"]}-0'
                details = extract_index_details(index)
                origination_date = int(month_start.timestamp() * 1e3)
                payload = {
                    "settings": {"index.lifecycle.origination_date": origination_date},
                    "aliases": {
                        details["read_alias"]: {"is_write_index": False},
                        details["write_alias"]: {"is_write_index": True},
                        details["rollover_alias"]: {"is_write_index": False},
                    },
                }
                if dry_run:
                    print(f"Dry run: {index} - {payload}")
                    continue
                response = http_hook_put(c, index, json.dumps(payload))
                print(f"{index}: {response}")

            return success(context=context, stage=tg_stage)

        verified = verify.expand(context=upstream)
        report(upstream=verified, stage=tg_stage)
        return fix.partial(c=conn_id).expand(context=verified)

    @task_group
    def aliases(conn_id: str, upstream: List[Context]) -> List[Context]:
        tg_stage = "aliases"

        @task
        def fetch(h: str, data: List[Context]) -> List[Context]:
            results = http_hook_get(h, "/_aliases")
            _filter_and_push(results, lambda x: INDEX_REGEX.match(x))
            return list(data)

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = xcom_pull("fetch_indices_per_tenant", context["tenant"])

            active_indices = []
            for month_start in generate_past_month_starts(context["retention"]):
                active_indices += [
                    index
                    for index in indices
                    if f'{context["tenant"]}-{month_start:%Y-%m-%d}' in index
                ]

            missing = []
            for index in active_indices:
                index_aliases = (
                    xcom_pull("aliases.fetch", index).get("aliases", dict()).keys()
                )
                if not all(
                    [
                        any(r.fullmatch(alias) for alias in index_aliases)
                        for r in ALIAS_REGEX_MAPPING.values()
                    ]
                ):
                    missing.append(index)

            if missing:
                return failure(context=context, stage=tg_stage, error=missing)
            return success(context=context, stage=tg_stage)

        @task
        def fix(c: str, context: Context) -> Context:
            if context["success"] or context["stage"] != tg_stage:
                return context
            actions = []
            for index in context["error"]:
                details = extract_index_details(index)
                actions += [
                    {
                        "add": {
                            "index": index,
                            "alias": details["read_alias"],
                            "is_write_index": False,
                        }
                    },
                    {
                        "add": {
                            "index": index,
                            "alias": details["write_alias"],
                            "is_write_index": True,
                        }
                    },
                    {
                        "add": {
                            "index": index,
                            "alias": details["rollover_alias"],
                            "is_write_index": details["should_rollover"],
                        }
                    },
                ]
            print(f"{context['tenant']}: {actions}")

            if dry_run:
                print(f"{context['tenant']}: {actions}")
                return success(context=context, stage=tg_stage)

            response = http_hook_post(c, "/_aliases", json.dumps({"actions": actions}))
            print(response)

            return success(context=context, stage=tg_stage)

        verified = verify.expand(context=fetch(conn_id, upstream))
        report(upstream=verified, stage=tg_stage)
        return fix.partial(c=conn_id).expand(context=verified)

    @task_group
    def rollover_alias(conn_id: str, upstream: List[Context]) -> List[Context]:
        tg_stage = "rollover_alias"

        @task
        def fetch_indices_in_alias(h: str, context: Context) -> Context:
            if not context["success"]:
                return context
            alias = f"{context['tenant']}-rollover"
            results = http_hook_get(h, f"/_cat/aliases/{alias}")
            indices = [(i["index"], i["is_write_index"] == "true") for i in results]
            xcom_push(alias, indices)
            return success(context=context, stage=tg_stage)

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            alias = f"{context['tenant']}-rollover"
            indices = xcom_pull("rollover_alias.fetch_indices_in_alias", alias)[0]
            print(f"{context['tenant']}: {list(indices)}")
            for index, is_write_alias in indices:
                if is_write_alias:
                    details = extract_index_details(index)
                    if details["should_rollover"]:
                        return success(context=context, stage=tg_stage)
                    else:
                        return failure(context=context, stage=tg_stage, error=index)
            return failure(context=context, stage=tg_stage, error=None)

        @task
        def fix(c: str, context: Context) -> Context:
            if context["success"] or context["stage"] != tg_stage:
                return context
            alias = f"{context['tenant']}-rollover"
            indices = xcom_pull("rollover_alias.fetch_indices_in_alias", alias)[0]
            actions = []
            if index := context["error"]:
                actions.append(
                    {"add": {"index": index, "alias": alias, "is_write_index": False}}
                )
            for index, is_write_alias in indices:
                details = extract_index_details(index)
                if details["should_rollover"]:
                    actions.append(
                        {
                            "add": {
                                "index": index,
                                "alias": alias,
                                "is_write_index": True,
                            }
                        }
                    )
                    break

            if dry_run:
                print(f"{context['tenant']}: {actions}")
                return success(context=context, stage=tg_stage)

            http_hook_post(c, f"/_aliases/{alias}", json.dumps({"actions": actions}))
            return success(context=context, stage=tg_stage)

        verified = verify.expand(
            context=fetch_indices_in_alias.partial(h=conn_id).expand(context=upstream)
        )
        report(upstream=verified, stage=tg_stage)
        return fix.partial(c=conn_id).expand(context=verified)

    @task
    def print_errors(upstream: List[Context]) -> None:
        for c in upstream:
            if not c["success"]:
                print(f'{c["tenant"]} - {c["stage"]}:')
                pprint.pprint(c, indent=2)

    connection_id = "{{ params.db_conn }}"
    t1 = fetch_indices_per_tenant(conn_id=connection_id)
    t2 = ilm_settings(conn_id=connection_id, upstream=t1)
    t3 = index_templates(conn_id=connection_id, upstream=t2)
    t4 = monthly_indices(conn_id=connection_id, upstream=t3)
    t5 = aliases(conn_id=connection_id, upstream=t4)
    t6 = rollover_alias(conn_id=connection_id, upstream=t5)
    print_errors(upstream=t6)


wiwip_dag()
