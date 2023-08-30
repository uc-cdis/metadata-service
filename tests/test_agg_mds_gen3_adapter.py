import respx
import httpx

from mds.agg_mds.adapters import get_metadata
from tenacity import RetryError, wait_none


@respx.mock
def test_gen3_adapter():
    json_response = {
        "GSE63878": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "link": "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE63878",
                "tags": [{"name": "Array", "category": "Data Type"}],
                "source": "Ichan School of Medicine at Mount Sinai",
                "funding": "",
                "study_description_summary": "The molecular factors involved in the development of Post-traumatic Stress Disorder (PTSD) remain poorly understood. Previous transcriptomic studies investigating the mechanisms of PTSD apply targeted approaches to identify individual genes under a cross-sectional framework lack a holistic view of the behaviours and properties of these genes at the system-level. Here we sought to apply an unsupervised gene-network-based approach to a prospective experimental design using whole-transcriptome RNA-Seq gene expression from peripheral blood leukocytes of U.S. Marines (N=188), obtained both pre- and post-deployment to conflict zones. We identified discrete groups of co-regulated genes (i.e., co-expression modules) and tested them for association to PTSD. We identified one module at both pre- and post-deployment containing putative causal signatures for PTSD development displaying an over-expression of genes enriched for functions of innate-immune response and interferon signalling (Type-I and Type-II). Importantly, these results were replicated in a second non-overlapping independent dataset of U.S. Marines (N=96), further outlining the role of innate immune and interferon signalling genes within co-expression modules to explain at least part of the causal pathophysiology for PTSD development. A second module, consequential of trauma exposure, contained PTSD resiliency signatures and an over-expression of genes involved in hemostasis and wound responsiveness suggesting that chronic levels of stress impair proper wound healing during/after exposure to the battlefield while highlighting the role of the hemostatic system as a clinical indicator of chronic-based stress. These findings provide novel insights for early preventative measures and advanced PTSD detection, which may lead to interventions that delay or perhaps abrogate the development of PTSD.\nWe used microarrays to characterize both prognostic and diagnostic molecular signatures associated to PTSD risk and PTSD status compared to control subjects.",
                "study_title": "Gene Networks Specific for Innate Immunity Define Post-traumatic Stress Disorder [Affymetrix]",
                "subjects_count": 48,
                "accession_number": "GSE63878",
                "data_files_count": 0,
                "contributor": "me.foo@smartsite.com",
                "other_metadata": {
                    "clinical_trials_id": "NCT1234567",
                },
                "repository": [
                    {
                        "repository_name": "ABC",
                        "repository_id": 123,
                    },
                    {
                        "repository_name": "XYZ",
                        "repository_id": "dummy",
                    },
                ],
            },
        }
    }

    field_mappings = {
        "tags": "path:tags",
        "_subjects_count": "path:subjects_count",
        "dbgap_accession_number": "path:study_id",
        "study_description": "path:study_description_summary",
        "number_of_datafiles": "path:data_files_count",
        "investigator": "path:contributor",
        "study_metadata.metadata_location.data_repositories": "path:repository",
        "study_metadata.metadata_location.clinical_trials_study_ID": "path:other_metadata.clinical_trials_id",
    }

    respx.get(
        "http://test/ok/mds/metadata?data=True&_guid_type=discovery_metadata&limit=1000&offset=0"
    ).mock(
        return_value=httpx.Response(
            status_code=200,
            json=json_response,
        )
    )

    expected = {
        "GSE63878": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "tags": [{"name": "Array", "category": "Data Type"}],
                "_subjects_count": 48,
                "dbgap_accession_number": None,
                "study_description": "The molecular factors involved in the development of Post-traumatic Stress Disorder (PTSD) remain poorly understood. Previous transcriptomic studies investigating the mechanisms of PTSD apply targeted approaches to identify individual genes under a cross-sectional framework lack a holistic view of the behaviours and properties of these genes at the system-level. Here we sought to apply an unsupervised gene-network-based approach to a prospective experimental design using whole-transcriptome RNA-Seq gene expression from peripheral blood leukocytes of U.S. Marines (N=188), obtained both pre- and post-deployment to conflict zones. We identified discrete groups of co-regulated genes (i.e., co-expression modules) and tested them for association to PTSD. We identified one module at both pre- and post-deployment containing putative causal signatures for PTSD development displaying an over-expression of genes enriched for functions of innate-immune response and interferon signalling (Type-I and Type-II). Importantly, these results were replicated in a second non-overlapping independent dataset of U.S. Marines (N=96), further outlining the role of innate immune and interferon signalling genes within co-expression modules to explain at least part of the causal pathophysiology for PTSD development. A second module, consequential of trauma exposure, contained PTSD resiliency signatures and an over-expression of genes involved in hemostasis and wound responsiveness suggesting that chronic levels of stress impair proper wound healing during/after exposure to the battlefield while highlighting the role of the hemostatic system as a clinical indicator of chronic-based stress. These findings provide novel insights for early preventative measures and advanced PTSD detection, which may lead to interventions that delay or perhaps abrogate the development of PTSD.\nWe used microarrays to characterize both prognostic and diagnostic molecular signatures associated to PTSD risk and PTSD status compared to control subjects.",
                "number_of_datafiles": 0,
                "investigator": "me.foo@smartsite.com",
                "study_metadata": {
                    "metadata_location": {
                        "data_repositories": [
                            {
                                "repository_name": "ABC",
                                "repository_id": 123,
                            },
                            {
                                "repository_name": "XYZ",
                                "repository_id": "dummy",
                            },
                        ],
                        "clinical_trials_study_ID": "NCT1234567",
                    }
                },
            },
        }
    }

    assert (
        get_metadata(
            "gen3",
            "http://test/ok/",
            None,
            mappings=field_mappings,
            keepOriginalFields=False,
        )
        == expected
    )

    field_mappings = {
        "tags": [],
        "_subjects_count": "path:subjects_count",
        "dbgap_accession_number": "path:study_id",
        "study_description": "path:study_description_summary",
        "number_of_datafiles": "path:data_files_count",
        "investigator": {"path": "contributor", "filters": ["strip_email"]},
        "study_metadata.metadata_location.data_repositories": [],
        "study_metadata.metadata_location.clinical_trials_study_ID": "path:other_metadata.clinical_trials_id",
        "study_metadata.citation.investigator": {
            "path": "contributor",
            "filters": ["strip_email"],
        },
    }

    expected = {
        "GSE63878": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "tags": [],
                "_subjects_count": 48,
                "dbgap_accession_number": None,
                "study_description": "The molecular factors involved in the development of Post-traumatic Stress Disorder (PTSD) remain poorly understood. Previous transcriptomic studies investigating the mechanisms of PTSD apply targeted approaches to identify individual genes under a cross-sectional framework lack a holistic view of the behaviours and properties of these genes at the system-level. Here we sought to apply an unsupervised gene-network-based approach to a prospective experimental design using whole-transcriptome RNA-Seq gene expression from peripheral blood leukocytes of U.S. Marines (N=188), obtained both pre- and post-deployment to conflict zones. We identified discrete groups of co-regulated genes (i.e., co-expression modules) and tested them for association to PTSD. We identified one module at both pre- and post-deployment containing putative causal signatures for PTSD development displaying an over-expression of genes enriched for functions of innate-immune response and interferon signalling (Type-I and Type-II). Importantly, these results were replicated in a second non-overlapping independent dataset of U.S. Marines (N=96), further outlining the role of innate immune and interferon signalling genes within co-expression modules to explain at least part of the causal pathophysiology for PTSD development. A second module, consequential of trauma exposure, contained PTSD resiliency signatures and an over-expression of genes involved in hemostasis and wound responsiveness suggesting that chronic levels of stress impair proper wound healing during/after exposure to the battlefield while highlighting the role of the hemostatic system as a clinical indicator of chronic-based stress. These findings provide novel insights for early preventative measures and advanced PTSD detection, which may lead to interventions that delay or perhaps abrogate the development of PTSD.\nWe used microarrays to characterize both prognostic and diagnostic molecular signatures associated to PTSD risk and PTSD status compared to control subjects.",
                "number_of_datafiles": 0,
                "investigator": "",
                "study_metadata": {
                    "citation": {"investigator": ""},
                    "metadata_location": {
                        "data_repositories": [],
                        "clinical_trials_study_ID": "NCT1234567",
                    },
                },
            },
        }
    }

    respx.get(
        "http://test/ok/mds/metadata?data=True&_guid_type=discovery_metadata&limit=64&offset=0"
    ).mock(
        return_value=httpx.Response(
            status_code=200,
            json=json_response,
        )
    )

    assert (
        get_metadata(
            "gen3",
            "http://test/ok/",
            filters=None,
            config={"batchSize": 64},
            mappings=field_mappings,
            keepOriginalFields=False,
        )
        == expected
    )
    expected = {
        "GSE63878": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "tags": [],
                "_subjects_count": 48,
                "dbgap_accession_number": "dg.333344.222",
                "study_description": "The molecular factors involved in the development of Post-traumatic Stress Disorder (PTSD) remain poorly understood. Previous transcriptomic studies investigating the mechanisms of PTSD apply targeted approaches to identify individual genes under a cross-sectional framework lack a holistic view of the behaviours and properties of these genes at the system-level. Here we sought to apply an unsupervised gene-network-based approach to a prospective experimental design using whole-transcriptome RNA-Seq gene expression from peripheral blood leukocytes of U.S. Marines (N=188), obtained both pre- and post-deployment to conflict zones. We identified discrete groups of co-regulated genes (i.e., co-expression modules) and tested them for association to PTSD. We identified one module at both pre- and post-deployment containing putative causal signatures for PTSD development displaying an over-expression of genes enriched for functions of innate-immune response and interferon signalling (Type-I and Type-II). Importantly, these results were replicated in a second non-overlapping independent dataset of U.S. Marines (N=96), further outlining the role of innate immune and interferon signalling genes within co-expression modules to explain at least part of the causal pathophysiology for PTSD development. A second module, consequential of trauma exposure, contained PTSD resiliency signatures and an over-expression of genes involved in hemostasis and wound responsiveness suggesting that chronic levels of stress impair proper wound healing during/after exposure to the battlefield while highlighting the role of the hemostatic system as a clinical indicator of chronic-based stress. These findings provide novel insights for early preventative measures and advanced PTSD detection, which may lead to interventions that delay or perhaps abrogate the development of PTSD.\nWe used microarrays to characterize both prognostic and diagnostic molecular signatures associated to PTSD risk and PTSD status compared to control subjects.",
                "number_of_datafiles": 0,
                "investigator": "",
                "study_metadata": {
                    "citation": {"investigator": ""},
                    "metadata_location": {
                        "data_repositories": [],
                        "clinical_trials_study_ID": "NCT1234567",
                    },
                },
            },
        }
    }

    per_item_override = {"GSE63878": {"dbgap_accession_number": "dg.333344.222"}}

    assert (
        get_metadata(
            "gen3",
            "http://test/ok/",
            None,
            config={"batchSize": 64},
            mappings=field_mappings,
            keepOriginalFields=False,
            perItemValues=per_item_override,
        )
        == expected
    )

    respx.get(
        "http://test/error/mds/metadata?data=True&_guid_type=discovery_metadata&limit=1000&offset=0"
    ).mock(
        return_value=httpx.Response(
            status_code=404,
            json=json_response,
        )
    )

    assert get_metadata("gen3", "http://test/error/", None, field_mappings) == {}

    # test for no url passed into Gen3Adapter
    assert get_metadata("gen3", None, None, None, field_mappings) == {}

    try:
        from mds.agg_mds.adapters import Gen3Adapter

        Gen3Adapter.getRemoteDataAsJson.retry.wait = wait_none()

        respx.get(
            "http://test/timeouterror/mds/metadata?data=True&_guid_type=discovery_metadata&limit=1000&offset=0"
        ).mock(side_effect=httpx.TimeoutException)

        get_metadata("gen3", "http://test/timeouterror/", None, field_mappings)
    except Exception as exc:
        assert isinstance(exc, RetryError)
