"""Behaviour of ``match_seaas_index``: which index names it accepts and the parts it parses from
them. A name it rejects is skipped by the DAGs, so a pattern that stops matching silently drops
that tenant's indices; the rejected names here are ones that must stay out."""

import pytest

from seaas_indices import match_seaas_index


class TestSurveysIndex:
    @pytest.mark.parametrize(
        "index, in_app_id, instance, instance_id, suffix",
        [
            ("seaas-acme_surveys-acme.medallia.com-acme-123-000002", "acme", "acme.medallia.com", "123", "000002"),
            ("seaas-acme2_surveys-acme.medallia.com-acme2-5-0", "acme2", "acme.medallia.com", "5", "0"),
            ("seaas-acme_eu_surveys-acme.medallia.eu-acme_eu-7-1", "acme_eu", "acme.medallia.eu", "7", "1"),
        ],
    )
    def test_parses_all_parts(self, index, in_app_id, instance, instance_id, suffix):
        match = match_seaas_index(index)

        assert match is not None
        assert match.group("in_app_id", "instance", "instance_id", "suffix") == (in_app_id, instance, instance_id, suffix)


class TestTopicBuilderIndex:
    @pytest.mark.parametrize(
        "index, in_app_id, instance, month, instance_id, suffix",
        [
            (
                "seaas-sretest20den2_topic-builder-sretest20den.medallia.com-sretest20den2-2026-07-01-120431-000035",
                "sretest20den2",
                "sretest20den.medallia.com",
                "2026-07-01",
                "120431",
                "000035",
            ),
            (
                "seaas-pkgdentest_topic-builder-pkgdentest.medallia.com-pkgdentest-2023-10-01-101880-0",
                "pkgdentest",
                "pkgdentest.medallia.com",
                "2023-10-01",
                "101880",
                "0",
            ),
            (
                "seaas-vaoitesd_topic-builder-vaoitesd.vaoitesd.va.gov-vaoitesd-2025-08-01-850100-000003",
                "vaoitesd",
                "vaoitesd.vaoitesd.va.gov",
                "2025-08-01",
                "850100",
                "000003",
            ),
        ],
    )
    def test_parses_all_parts(self, index, in_app_id, instance, month, instance_id, suffix):
        match = match_seaas_index(index)

        assert match is not None
        assert match.group("in_app_id", "instance", "month", "instance_id", "suffix") == (
            in_app_id,
            instance,
            month,
            instance_id,
            suffix,
        )


@pytest.mark.parametrize(
    "index, instance",
    [
        ("seaas-acme_surveys-localhost-acme-8-1", "localhost"),
        ("seaas-acme_surveys-acme-eu.sub.example.io-acme-7-1", "acme-eu.sub.example.io"),
        ("seaas-acme_topic-builder-acme.example.com-acme-2026-07-01-5-1", "acme.example.com"),
    ],
)
def test_instance_can_be_any_hostname(index, instance):
    match = match_seaas_index(index)

    assert match is not None
    assert match["instance"] == instance


@pytest.mark.parametrize(
    "index",
    [
        "seaas-acme_topic-builder-acme.medallia.com-acme-120431-000035",  # topic-builder needs the month
        "seaas-acme_surveys-acme.medallia.com-acme-2026-07-01-5-1",  # surveys has no month
        "seaas-acme_surveys-acme.medallia.com-other-1-1",  # in_app_id parts differ
        "seaas-acme_topic-builder-acme.medallia.com-other-2026-07-01-1-1",  # in_app_id parts differ
        "seaas-acme_surveys-acme..medallia.com-acme-1-1",  # empty hostname label
        "seaas-acme_surveys-acme.medallia.com-acme-123",  # no suffix
        "seaas-acme_surveys-acme.medallia.com-acme-abc-1",  # instance_id isn't a number
        "seaas-acme_surveys-acme.medallia.com-acme-1-1-old",  # trailing text
        "seaas-acme_other-acme.medallia.com-acme-1-1",  # unknown index type
        "acme_surveys-acme.medallia.com-acme-1-1",  # no seaas- prefix
        ".kibana_1",
    ],
)
def test_rejects_other_names(index):
    assert match_seaas_index(index) is None
