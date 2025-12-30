import functools
import json
import pprint
from collections import defaultdict
from typing import TypedDict, Optional, Any, List, Tuple
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
    month = m["month"]
    return {
        "tenant": tenant,
        "tenant_id": m["tenant_id"],
        "suffix": int(m["suffix"]),
        "month": month,
        "read_alias": tenant,
        "rollover_alias": f"{tenant}-rollover",
        "write_alias": ALIAS_REGEX_MAPPING["write"].search(index).group(0),
        "should_rollover": (
            datetime.date.fromisoformat(month)
            == datetime.date.today().replace(day=1) + relativedelta(months=1)
        ),
    }


def index_has_expired(index, expire):
    details = extract_index_details(index)
    oldest = datetime.datetime.today().replace(
        day=1, hour=0, minute=0, second=0, tzinfo=datetime.timezone.utc
    ) - relativedelta(months=expire)
    index_datetime = datetime.datetime.fromisoformat(details["month"]).replace(
        tzinfo=datetime.timezone.utc
    )
    return oldest > index_datetime


def chain_on_success(func):
    @functools.wraps(func)
    def wrapper(context):
        if not context["success"]:
            return context
        return func(context)
    return wrapper

def chain_on_error_in_stage(stage):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(context):
            if context["success"] or context["stage"] != stage:
                return context
            return func(context)
        return wrapper
    return decorator


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
    ) - relativedelta(months=n-1)
    return [current_month_start + relativedelta(months=i) for i in range(n+1)]


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
    error: Optional[Any]
    stage: Optional[str]
    value: Optional[Any]
    retention: Optional[int]
    latest_suffix: Optional[int]
    conn_id: str
    dry_run: bool


def success(
    context: Context,
    stage: str,
    value: Any = None,
) -> Context:
    context.update(
        {
            "success": True,
            "stage": stage,
            "value": value,
            "error": None,
        }
    )
    return context


def failure(
    context: Context,
    stage: str,
    error: Any = None,
) -> Context:
    context.update(
        {
            "success": False,
            "stage": stage,
            "value": None,
            "error": error,
        }
    )
    return context


def fetch_indices_in_alias(alias: str, conn_id: str) -> List[Tuple[str, str, bool]]:
    results = http_hook_get(conn_id, f"/_cat/aliases/{alias}")
    return [(i["index"], i["alias"], i["is_write_index"] == "true") for i in results]


def fetch_indices(prefix: str, conn_id: str) -> List[str]:
    results = http_hook_get(conn_id, f"/_cat/indices/{prefix}*?h=index&format=json")
    return [r["index"] for r in results]


def update_aliases(context, actions):
    if context["dry_run"]:
        print(f"{context['tenant']}: {actions}")
        return

    response = http_hook_post(
        context["conn_id"],
        "/_aliases/",
        json.dumps({"actions": actions}),
    )
    print(response)


def create_index(context, index, payload):
    if context["dry_run"]:
        print(f"Dry run: {index} - {payload}")
        return
    response = http_hook_put(context["conn_id"], index, json.dumps(payload))
    print(f"{index}: {response}")


@dag(
    dag_display_name="Fix & Verify",
    tags=["spf", "elasticsearch"],
    description="This DAG replaces fix and verify job.",
    max_active_runs=1,
    catchup=False,
    params={
        "db_conn": Param("es-testing", type="string"),
        "dry_run": Param(True, type="boolean"),
    },
    render_template_as_native_obj=True,
)
def fix_and_verify_dag():
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
    def fetch_indices_per_tenant(context: Context) -> List[Context]:
        fetched = http_hook_get(context["conn_id"], "/_cat/indices?h=index&format=json")
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
            latest_suffix = max([extract_index_details(i)["suffix"] for i in v])
            grouped.append(
                Context(
                    success=True,
                    tenant=k[0],
                    tenant_id=k[1],
                    latest_suffix=latest_suffix,
                    conn_id=context["conn_id"],
                    dry_run=context["dry_run"],
                )
            )
        return grouped

    @task_group
    def ilm_settings(upstream: List[Context]) -> List[Context]:
        tg_stage = "ilm_settings"

        @task
        def fetch(data: List[Context]) -> List[Context]:
            results = http_hook_get(
                data[0]["conn_id"],
                "/_settings/index.lifecycle.name,index.lifecycle.rollover_alias",
            )
            _filter_and_push(results, lambda x: INDEX_REGEX.match(x))
            return data

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
            context.update({"retention": retention[0]})
            return success(context=context, stage=tg_stage)

        verified = verify.expand(context=fetch(upstream))
        report(upstream=verified, stage=tg_stage)
        return verified

    @task_group
    def index_templates(upstream: List[Context]) -> List[Context]:
        tg_stage = "index_templates"

        @task
        def fetch(data: List[Context]) -> List[Context]:
            results = http_hook_get(data[0]["conn_id"], "/_index_template/*-rollover")
            _filter_and_push(
                {r["name"]: r["index_template"] for r in results["index_templates"]},
                lambda x: ALIAS_REGEX_MAPPING["rollover"].match(x),
            )
            return data

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

        verified = verify.expand(context=fetch(upstream))
        report(upstream=verified, stage=tg_stage)
        return verified

    @task_group
    def monthly_indices(upstream: List[Context]) -> List[Context]:
        tg_stage = "monthly_indices"

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = xcom_pull("fetch_indices_per_tenant", context["tenant"])
            missing = []
            for month_start in generate_past_month_starts(context["retention"]):
                if not any(
                    index.startswith(
                        f'seaas-{context["tenant"]}-{month_start:%Y-%m-%d}'
                    )
                    for index in indices
                ):
                    missing.append(month_start)

            if missing:
                return failure(context=context, stage=tg_stage, error=missing)
            return success(context=context, stage=tg_stage)

        @task
        @chain_on_error_in_stage(stage=tg_stage)
        def fix(context: Context) -> Context:

            suffix = context["latest_suffix"]

            for month_start in context["error"]:
                suffix += 1
                origination_date = int(month_start.timestamp() * 1e3)
                index = f'seaas-{context["tenant"]}-{month_start:%Y-%m-%d}-{context["tenant_id"]}-{suffix:06}'
                details = extract_index_details(index)
                payload = {
                    "settings": {"index.lifecycle.origination_date": origination_date},
                    "aliases": {
                        details["read_alias"]: {"is_write_index": False},
                        details["write_alias"]: {"is_write_index": True},
                        details["rollover_alias"]: {"is_write_index": False},
                    },
                }
                create_index(context, index, payload)

            context.update({"latest_suffix": suffix})
            return success(context=context, stage=tg_stage)

        verified = verify.expand(context=upstream)
        report(upstream=verified, stage=tg_stage)
        return fix.expand(context=verified)

    @task_group
    def read_alias(upstream: List[Context]) -> List[Context]:
        tg_stage = "read_alias"

        @task
        @chain_on_success
        def fetch(context: Context) -> Context:
            tenant = context["tenant"]
            return success(
                context=context,
                stage=tg_stage,
                value={
                    "read_indices": [
                        i
                        for i, _, _ in fetch_indices_in_alias(
                            alias=tenant, conn_id=context["conn_id"]
                        )
                    ],
                    "indices": fetch_indices(prefix=tenant, conn_id=context["conn_id"]),
                },
            )

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = set(context["value"]["indices"])
            read_indices = set(context["value"]["read_indices"])
            if len(indices) != len(read_indices):
                return failure(
                    context=context, stage=tg_stage, error=list(indices - read_indices)
                )
            return success(context=context, stage=tg_stage)

        @task
        @chain_on_error_in_stage(stage=tg_stage)
        def fix(context: Context) -> Context:
            alias = context["tenant"]
            actions = [
                {"add": {"index": index, "alias": alias, "is_write_index": False}}
                for index in context["error"]
            ]

            update_aliases(context=context, actions=actions)
            return success(context=context, stage=tg_stage)

        verified = verify.expand(context=fetch.expand(context=upstream))
        report(upstream=verified, stage=tg_stage)
        return fix.expand(context=verified)

    @task_group
    def write_alias(upstream: List[Context]) -> List[Context]:
        tg_stage = "write_alias"

        @task
        @chain_on_success
        def fetch(context: Context) -> Context:
            results = http_hook_get(
                conn_id=context["conn_id"], endpoint=f'/{context["tenant"]}/_alias'
            )
            active_aliases = defaultdict(list)
            retention = context["retention"]

            for index, r in results.items():
                details = extract_index_details(index)
                alias = details["write_alias"]
                if index_has_expired(index=index, expire=retention):
                    continue
                is_write = r["aliases"].get(alias, {}).get("is_write_index")
                active_aliases[alias].append((index, is_write))

            return success(context=context, stage=tg_stage, value=active_aliases)

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            fetched = context["value"]
            missing = {}
            print(fetched)
            for alias, indices in context["value"].items():
                if any(i[1] is None for i in indices) or not any(
                    i[1] is True for i in indices
                ):
                    missing[alias] = indices
            if missing:
                return failure(context=context, stage=tg_stage, error=missing)
            return success(context=context, stage=tg_stage)

        @task
        @chain_on_error_in_stage(stage=tg_stage)
        def fix(context: Context) -> Context:
            actions = []
            for alias, indices in context["error"].items():
                if any(i[1] is True for i in indices):
                    write_index = [i[0] for i in indices if i[1] is True][0]
                else:
                    write_index = max(
                        [i[0] for i in indices],
                        key=lambda i: extract_index_details(i)["suffix"],
                    )
                actions += [
                    {
                        "add": {
                            "index": i,
                            "alias": alias,
                            "is_write_index": write_index == i,
                        }
                    }
                    for i, _ in indices
                ]

            update_aliases(context=context, actions=actions)
            return success(context=context, stage=tg_stage)

        verified = verify.expand(context=fetch.expand(context=upstream))
        report(upstream=verified, stage=tg_stage)
        return fix.expand(context=verified)

    @task_group
    def rollover_alias(upstream: List[Context]) -> List[Context]:
        tg_stage = "rollover_alias"

        @task
        def fetch(context: Context) -> Context:
            if not context["success"]:
                return context
            tenant = context["tenant"]
            return success(
                context=context,
                stage=tg_stage,
                value={
                    "read_alias": [
                        i
                        for i, _, _ in fetch_indices_in_alias(
                            alias=tenant, conn_id=context["conn_id"]
                        )
                    ],
                    "rollover_alias": {
                        i: b
                        for i, _, b in fetch_indices_in_alias(
                            alias=f"{tenant}-rollover", conn_id=context["conn_id"]
                        )
                    },
                },
            )

        @task
        @chain_on_success
        def verify(context: Context) -> Context:
            indices = context["value"]
            indices_in_read_alias = indices["read_alias"]
            indices_in_rollover_alias = indices["rollover_alias"]

            needs_fixing = []
            for index in indices_in_read_alias:
                if index not in indices_in_rollover_alias:
                    needs_fixing.append(index)
                    continue
                details = extract_index_details(index)

                if indices_in_rollover_alias[index] and details["should_rollover"]:
                    return success(context=context, stage=tg_stage)

                if indices_in_rollover_alias[index] or details["should_rollover"]:
                    needs_fixing.append(index)

            return failure(context=context, stage=tg_stage, error=needs_fixing)

        @task
        @chain_on_error_in_stage(stage=tg_stage)
        def fix(context: Context) -> Context:

            alias = f"{context['tenant']}-rollover"
            actions = [
                {
                    "add": {
                        "index": index,
                        "alias": alias,
                        "is_write_index": extract_index_details(index)[
                            "should_rollover"
                        ],
                    }
                }
                for index in context["error"]
            ]

            update_aliases(context=context, actions=actions)
            return success(context=context, stage=tg_stage)

        verified = verify.expand(context=fetch.expand(context=upstream))
        report(upstream=verified, stage=tg_stage)
        return fix.expand(context=verified)

    @task
    def print_errors(upstream: List[Context]) -> None:
        for c in upstream:
            if not c["success"]:
                print(f'{c["tenant"]} - {c["stage"]}:')
                pprint.pprint(c, indent=2)

    initial_context = Context(
        conn_id="{{ params.db_conn }}", dry_run="{{ params.dry_run }}"
    )
    t1 = fetch_indices_per_tenant(context=initial_context)
    t2 = ilm_settings(upstream=t1)
    t3 = index_templates(upstream=t2)
    t4 = monthly_indices(upstream=t3)
    t5 = read_alias(upstream=t4)
    t6 = write_alias(upstream=t5)
    t7 = rollover_alias(upstream=t6)
    print_errors(upstream=t7)


fix_and_verify_dag()
