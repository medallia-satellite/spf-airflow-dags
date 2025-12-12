from collections import defaultdict

from airflow.decorators import dag, task_group
from airflow.providers.http.hooks.http import HttpHook

from repo.elasticsearch_client import *
from repo.fix_and_verify import *
from repo.utils import *


@dag(
    dag_display_name="WIP",
    tags=["spf", "test", "poc"],
    description="This DAG verifies monthly indices.",
    max_active_runs=1,
    catchup=False,
)
def wip_dag():
    hook_get = HttpHook(method='GET', http_conn_id='es-wordtags')

    @task_group
    def fetch_and_group_indices():
        @task
        def group_by_tenant(indices: list):
            result = defaultdict(list)
            for index in indices:
                result[BASE_REGEX.search(index).group(0)].append(index)
            return [success(key=k, value=v) for k, v in result.items()]

        fetched = fetch_indices(hook=hook_get)
        grouped = group_by_tenant(fetched)
        return push(grouped)

    @task_group
    def fetch_and_group_aliases():
        @task
        def group_by_index(aliases: list):
            result = defaultdict(list)
            for alias_entry in aliases:
                if match := INDEX_REGEX.match(alias_entry["index"]):
                    result[match.group(0)].append(alias_entry["alias"])
            return [success(key=k, value=v) for k, v in result.items()]

        fetched = fetch_aliases(hook=hook_get)
        grouped = group_by_index(fetched)
        return push(grouped)

    @task_group
    def ilm_settings(upstream: List[Result]):
        @task(task_id="extract")
        def extract_ilm_setting(data):
            instance = data["key"]
            il_list = [s["settings"]["index"]["lifecycle"] for s in data["value"]]

            if any("name" not in il for il in il_list):
                return failure(key=instance, error=f"Invalid policies: {il_list}")

            policies = set(il["name"] for il in il_list)
            if not all(p in POLICY_MAPPING for p in policies):
                return failure(key=instance, error=f"Invalid policies: {policies}")

            if len(set(POLICY_MAPPING.get(p) for p in policies)) != 1:
                return failure(key=instance, error=f"Retention period is not unique: {policies}")

            if any("rollover_alias" not in il for il in il_list):
                return failure(key=instance,
                               error=f"No rollover alias {[il for il in il_list if 'rollover_alias' not in il]}")

            rollover_aliases = set(il["rollover_alias"] for il in il_list)
            if not all(ALIAS_REGEX_MAPPING["rollover"].match(a) for a in rollover_aliases):
                return failure(key=instance, error=f"Invalid rollover alias {rollover_aliases}")

            if len(rollover_aliases) != 1:
                return failure(key=instance, error=f"Rollover alias is not unique {rollover_aliases}")

            result = {
                "retention": next(iter(set(POLICY_MAPPING.get(p) for p in policies))),
                "rollover_alias": next(iter(rollover_aliases)),
            }

            return success(key=instance, value=result)

        f = fetch_alias_settings.partial(hook=hook_get).expand(data=upstream)
        e = extract_ilm_setting.expand(data=f)
        return push(e)

    @task_group
    def index_templates(upstream: List[Result]):
        @task(task_id="extract")
        def extract_fetch_index_template(data):
            tenant = data["key"]
            num_months = retrieve("ilm_settings", tenant)["retention"]
            # get
            index_template = data["value"]["index_template"]
            ref = {'index_patterns': [f'seaas-{tenant}-*'],
 'template': {'mappings': {'properties': {'comments': {'properties': {'language': {'type': 'keyword'},
                                                                      'linguisticConnections': {'analyzer': 'topic-builder-analyzer',
                                                                                                'position_increment_gap': 1000,
                                                                                                'type': 'text'},
                                                                      'linguisticConnectionsIndexes': {'type': 'short'},
                                                                      'name': {'type': 'keyword'},
                                                                      'persona': {'type': 'keyword'},
                                                                      'sentenceContent': {'analyzer': 'topic-builder-analyzer',
                                                                                          'type': 'text'},
                                                                      'sentenceIndex': {'type': 'short'},
                                                                      'wordEndIndexes': {'type': 'integer'},
                                                                      'wordStartIndexes': {'type': 'integer'}},
                                                       'type': 'nested'},
                                          'responseDate': {'type': 'date'},
                                          'surveyId': {'type': 'long'}}},
              'settings': {'index': {'analysis': {'analyzer': {'topic-builder-analyzer': {'filter': ['compound_capture'],
                                                                                'tokenizer': 'whitespace',
                                                                                'type': 'custom'}},
                                        'filter': {'compound_capture': {'patterns': ['(!?[^@!@]+)@!@'],
                                                                        'preserve_original': 'false',
                                                                        'type': 'pattern_capture'}}},
                            'lifecycle': {
                                'name': f'M{num_months}_rollover',
                                'rollover_alias': f'{tenant}-rollover',
                            },
                           'number_of_replicas': '1',
                           'number_of_shards': '1'}}}}
            _ = index_template.pop("composed_of")
            if index_template != ref:
                return failure(key=tenant, error=f"Invalid template: {index_template}")
            return success(key=tenant, value="")

        f = fetch_index_templates.partial(hook=hook_get).expand(data=upstream)
        e = extract_fetch_index_template.expand(data=f)
        return push(e)


    def expected_monthly_aliases(tenant):
        num_months = retrieve("ilm_settings", tenant)["retention"]
        start_date = datetime.date.today().replace(day=1) + relativedelta(months=1)
        return [
            f"{tenant}-{(start_date - relativedelta(months=i)):%Y-%m-%d}"
            for i in range(num_months)
        ]

    @task_group
    def validate_monthly_indices_and_aliases(upstream: List[Result]):
        @task
        def flatten_results(results: List[List[Result]]) -> List[Result]:
            flattened = [item for sublist in results for item in sublist]
            print(f"Flattened result size: {len(flattened)}")
            print(f"OK monthly indices: {len([r for r in flattened if r['success']])}/{len(flattened)}")
            print(
                f"Missing monthly indices: {len([r for r in flattened if r['error'] == 'Missing index'])}/{len(flattened)}")
            print(
                f"Indices with missing aliases: {len([r for r in flattened if r['error'] == 'Alias not complete'])}/{len(flattened)}")
            print(
                f"Non unique monthly indices: {len([r for r in flattened if r['error'] == 'Monthly index not unique'])}/{len(flattened)}")
            return flattened

        @task
        def validate_monthly_indices_per_tenant(data: Result) -> List[Result]:
            tenant = data["key"]
            indices = retrieve("fetch_and_group_indices", tenant)
            results = []

            for monthly_alias in expected_monthly_aliases(tenant):
                monthly_indices = [index for index in indices if monthly_alias in index]
                if not monthly_indices:
                    results.append(failure(key=tenant, value=monthly_alias, error="Missing index"))
                    continue
                if len(monthly_indices) != 1:
                    results.append(failure(key=tenant, value=monthly_alias, error=f"Monthly index not unique"))
                    continue
                index = monthly_indices[0]
                aliases = retrieve("fetch_and_group_aliases", index)
                if not all([any(r.fullmatch(alias) for r in ALIAS_REGEX_MAPPING.values()) for alias in aliases]):
                    results.append(failure(key=tenant, value=index, error="Alias not complete"))
                    continue

                results.append(success(key=tenant, value=index))
            return results

        expanded = validate_monthly_indices_per_tenant.expand(data=upstream)
        return flatten_results(results=expanded)


    @task_group
    def missing_monthly_indices(upstream: List[Result]):
        @task
        def extract_missing_monthly_indices(data: List[Result]):
            extracted = [success(key=d["key"], value=d["value"]) for d in data if d["error"] == "Missing index"]
            for r in extracted:
                print(f"Missing monthly indices: {r['key']}/{r['value']}")
            return extracted

        @task
        def create_missing_monthly_indices(data: Result):
            return data

        return create_missing_monthly_indices.expand(data=extract_missing_monthly_indices(data=upstream))


    @task_group
    def indices_with_missing_aliases(upstream: List[Result]):
        @task
        def extract_indices_with_missing_aliases(data: List[Result]):
            extracted = [success(key=d["key"], value=d["value"]) for d in data if d["error"] == "Alias not complete"]
            for r in extracted:
                print(f"Indices with missing aliases: {r['key']}/{r['value']}")
            return extracted

        @task
        def assign_missing_aliases_to_indices(data: Result):
            return data

        return assign_missing_aliases_to_indices.expand(data=extract_indices_with_missing_aliases(data=upstream))


    fga = fetch_and_group_aliases()
    fgi = fetch_and_group_indices()
    ilm = ilm_settings(fgi)
    fga.set_downstream(ilm)

    validated = validate_monthly_indices_and_aliases(index_templates(ilm))

    indices_with_missing_aliases(validated)
    missing_monthly_indices(validated)


wip_dag()
