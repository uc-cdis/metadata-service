import pytest
import respx
import httpx
from argparse import Namespace
from mds.populate import (
    parse_config_from_file,
    parse_args,
    populate_metadata,
    main,
    filter_entries,
    populate_info,
    populate_drs_info,
    populate_config,
)
from mds.agg_mds.commons import (
    AdapterMDSInstance,
    MDSInstance,
    Commons,
    Settings,
    FieldDefinition,
    Config,
)
from mds.agg_mds import adapters
from mds.agg_mds import datastore
import json
from unittest.mock import patch, call, MagicMock
from conftest import AsyncMock
from tempfile import NamedTemporaryFile
from pathlib import Path


@pytest.mark.asyncio
async def test_parse_args():
    try:
        known_args = parse_args([])
    except BaseException as exception:
        assert exception.code == 2

    known_args = parse_args(["--config", "some/file.json"])
    assert known_args == Namespace(config="some/file.json")


@pytest.mark.asyncio
async def test_populate_metadata():
    with patch.object(datastore, "update_metadata", AsyncMock()) as mock_update:
        await populate_metadata(
            "my_commons",
            MDSInstance(
                mds_url="http://mds",
                commons_url="http://commons",
                columns_to_fields={"column1": "field1"},
            ),
            {
                "id1": {
                    "gen3_discovery": {
                        "column1": "some data",
                        "tags": [{"category": "my_category", "name": "my_name"}],
                    }
                }
            },
        )

        mock_update.assert_called_with(
            "my_commons",
            [
                {
                    "id1": {
                        "gen3_discovery": {
                            "column1": "some data",
                            "tags": [
                                {
                                    "category": "my_category",
                                    "name": "my_name",
                                },
                            ],
                            "commons_name": "my_commons",
                        }
                    }
                }
            ],
            ["id1"],
            {"my_category": ["my_name"]},
            {"commons_url": "http://commons"},
            False,
        )


@pytest.mark.asyncio
async def test_populate_info():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_datastore:
        with NamedTemporaryFile(mode="w+", delete=False) as fp:
            json.dump(
                {
                    "configuration": {
                        "schema": {
                            "_subjects_count": {"type": "integer"},
                            "study_description": {},
                        },
                    },
                    "gen3_commons": {
                        "mycommons": {
                            "mds_url": "http://mds",
                            "commons_url": "http://commons",
                            "columns_to_fields": {
                                "short_name": "name",
                                "full_name": "full_name",
                                "_subjects_count": "_subjects_count",
                                "study_id": "study_id",
                                "_unique_id": "_unique_id",
                                "study_description": "study_description",
                            },
                        },
                    },
                    "adapter_commons": {
                        "non-gen3": {
                            "mds_url": "http://non-gen3",
                            "commons_url": "non-gen3",
                            "adapter": "icpsr",
                        }
                    },
                },
                fp,
            )
        config = parse_config_from_file(Path(fp.name))
        await populate_info(config)
        mock_datastore.update_global_info.assert_has_calls(
            [
                call("aggregations", {}, False),
                call(
                    "schema",
                    {
                        "_subjects_count": {"type": "integer", "description": ""},
                        "study_description": {"type": "string", "description": ""},
                    },
                    False,
                ),
            ],
            any_order=True,
        )


@pytest.mark.asyncio
@respx.mock
async def test_populate_drs_info():
    mock_adapter = AsyncMock(return_value={})
    patch("mds.agg_mds.adapters.get_metadata", mock_adapter)
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_datastore:
        with NamedTemporaryFile(mode="w+", delete=False) as fp:
            json.dump(
                {
                    "configuration": {
                        "schema": {
                            "_subjects_count": {"type": "integer"},
                            "study_description": {},
                        },
                        "settings": {
                            "cache_drs": True,
                            "drs_indexd_server": "http://test",
                            "timestamp_entry": True,
                        },
                    },
                },
                fp,
            )

        json_data = [
            {
                "hints": [".*dg\\.XXTS.*"],
                "host": "https://mytest1.commons.io/",
                "name": "DataSTAGE",
                "type": "indexd",
            },
            {
                "hints": [".*dg\\.TSXX.*"],
                "host": "https://commons2.io/index/",
                "name": "Environmental DC",
                "type": "indexd",
            },
        ]

        respx.get("http://test/index/_dist").mock(
            return_value=httpx.Response(
                status_code=200,
                json=json_data,
            )
        )

        config = parse_config_from_file(Path(fp.name))
        await populate_drs_info(config)
        mock_datastore.update_global_info.assert_has_calls(
            [
                call(
                    "dg.XXTS",
                    {
                        "host": "mytest1.commons.io",
                        "name": "DataSTAGE",
                        "type": "indexd",
                    },
                    False,
                ),
                call(
                    "dg.TSXX",
                    {
                        "host": "commons2.io",
                        "name": "Environmental DC",
                        "type": "indexd",
                    },
                    False,
                ),
            ],
            any_order=True,
        )

        await populate_drs_info(config, True)
        mock_datastore.update_global_info.assert_has_calls(
            [
                call(
                    "dg.XXTS",
                    {
                        "host": "mytest1.commons.io",
                        "name": "DataSTAGE",
                        "type": "indexd",
                    },
                    True,
                ),
                call(
                    "dg.TSXX",
                    {
                        "host": "commons2.io",
                        "name": "Environmental DC",
                        "type": "indexd",
                    },
                    True,
                ),
            ],
            any_order=True,
        )


@pytest.mark.asyncio
async def test_populate_config():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_datastore:
        with NamedTemporaryFile(mode="w+", delete=False) as fp:
            json.dump(
                {
                    "configuration": {
                        "schema": {
                            "_subjects_count": {"type": "array"},
                            "study_description": {},
                        },
                    },
                    "gen3_commons": {
                        "mycommons": {
                            "mds_url": "http://mds",
                            "commons_url": "http://commons",
                            "columns_to_fields": {
                                "short_name": "name",
                                "full_name": "full_name",
                                "_subjects_count": "_subjects_count",
                                "study_id": "study_id",
                                "_unique_id": "_unique_id",
                                "study_description": "study_description",
                            },
                        },
                    },
                    "adapter_commons": {
                        "non-gen3": {
                            "mds_url": "http://non-gen3",
                            "commons_url": "non-gen3",
                            "adapter": "icpsr",
                        }
                    },
                },
                fp,
            )
        config = parse_config_from_file(Path(fp.name))
        await populate_config(config)
        mock_datastore.update_config_info.called_with(["_subjects_count"])


@pytest.mark.asyncio
async def test_populate_config_to_temp_index():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_datastore:
        with NamedTemporaryFile(mode="w+", delete=False) as fp:
            json.dump(
                {
                    "configuration": {
                        "schema": {
                            "_subjects_count": {"type": "array"},
                            "study_description": {},
                        },
                    },
                    "gen3_commons": {
                        "mycommons": {
                            "mds_url": "http://mds",
                            "commons_url": "http://commons",
                            "columns_to_fields": {
                                "short_name": "name",
                                "full_name": "full_name",
                                "_subjects_count": "_subjects_count",
                                "study_id": "study_id",
                                "_unique_id": "_unique_id",
                                "study_description": "study_description",
                            },
                        },
                    },
                    "adapter_commons": {
                        "non-gen3": {
                            "mds_url": "http://non-gen3",
                            "commons_url": "non-gen3",
                            "adapter": "icpsr",
                        }
                    },
                },
                fp,
            )
        config = parse_config_from_file(Path(fp.name))
        await populate_config(config, True)
        mock_datastore.update_config_info.called_with(
            ["_subjects_count"], use_temp_index=True
        )


@respx.mock
@pytest.mark.asyncio
async def test_populate_main():
    with patch("mds.config.USE_AGG_MDS", False):
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            await main(commons_config=None)
        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 1

    patch("mds.config.USE_AGG_MDS", True).start()
    patch.object(datastore, "init", AsyncMock()).start()
    patch.object(datastore, "drop_all_non_temp_indexes", AsyncMock()).start()
    patch.object(datastore, "drop_all_temp_indexes", AsyncMock()).start()
    patch.object(datastore, "create_indexes", AsyncMock()).start()
    patch.object(datastore, "create_temp_indexes", AsyncMock()).start()
    patch.object(datastore, "update_config_info", AsyncMock()).start()
    patch.object(datastore, "get_status", AsyncMock(return_value="OK")).start()
    patch.object(datastore, "close", AsyncMock()).start()
    patch.object(datastore, "update_global_info", AsyncMock()).start()
    patch.object(datastore, "update_metadata", AsyncMock()).start()
    patch.object(adapters, "get_metadata", MagicMock()).start()
    patch.object(datastore, "clone_temp_indexes_to_real_indexes", AsyncMock()).start()

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
            },
        }
    }

    respx.get(
        "http://test/ok//mds/metadata?data=True&_guid_type=discovery_metadata&limit=1000&offset=0"
    ).mock(
        return_value=httpx.Response(
            status_code=200,
            json=json_response,
        )
    )

    await main(
        Commons(
            configuration=Config(
                settings=Settings(),
                schema={
                    "_subjects_count": FieldDefinition(type="integer"),
                    "year_awarded": FieldDefinition(type="integer"),
                },
            ),
            gen3_commons={
                "my_commons": MDSInstance(
                    mds_url="http://test/ok/",
                    commons_url="test",
                    columns_to_fields={
                        "authz": "path:authz",
                        "tags": "path:tags",
                        "_subjects_count": "path:subjects_count",
                        "dbgap_accession_number": "path:study_id",
                        "study_description": "path:study_description_summary",
                        "number_of_datafiles": "path:data_files_count",
                        "investigator": "path:contributor",
                    },
                ),
            },
            adapter_commons={
                "adapter_commons": AdapterMDSInstance(
                    mds_url="",
                    commons_url="",
                    adapter="icpsr",
                ),
            },
        )
    )


@respx.mock
@pytest.mark.asyncio
async def test_populate_main_fail():
    patch("mds.config.USE_AGG_MDS", True).start()
    patch.object(datastore, "init", AsyncMock()).start()
    patch.object(datastore, "drop_all_temp_indexes", AsyncMock()).start()
    patch.object(datastore, "create_indexes", AsyncMock()).start()
    patch.object(datastore, "create_temp_indexes", AsyncMock()).start()
    patch.object(datastore, "update_config_info", AsyncMock()).start()
    patch.object(datastore, "get_status", AsyncMock(return_value="OK")).start()
    patch.object(datastore, "close", AsyncMock()).start()
    patch.object(datastore, "update_global_info", AsyncMock()).start()
    patch.object(datastore, "update_metadata", AsyncMock()).start()
    patch.object(adapters, "get_metadata", MagicMock()).start()
    patch.object(datastore, "clone_temp_indexes_to_real_indexes", AsyncMock()).start()

    existing_metadata = {
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
            },
        }
    }

    # Mock get_all_metadata call to return proper document
    get_all_metadata_mock = AsyncMock(return_value=existing_metadata)
    patch.object(datastore, "get_all_metadata", get_all_metadata_mock).start()

    # If drop_all is called, set get_all_metadata_mock return_value to None
    def wipe_return_value(mock: AsyncMock):
        mock.return_value = None

    drop_all_indexes_mock = AsyncMock(
        side_effect=wipe_return_value(get_all_metadata_mock)
    )
    patch.object(datastore, "drop_all_non_temp_indexes", drop_all_indexes_mock).start()

    respx.get(
        "http://testfail/ok//mds/metadata?data=True&_guid_type=discovery_metadata&limit=1000&offset=0"
    ).mock(return_value=httpx.Response(status_code=500))
    with pytest.raises(Exception):
        await main(
            Commons(
                configuration=Config(
                    settings=Settings(),
                    schema={
                        "_subjects_count": FieldDefinition(type="integer"),
                        "year_awarded": FieldDefinition(type="integer"),
                    },
                ),
                gen3_commons={
                    "my_commons": MDSInstance(
                        mds_url="http://testfail/ok/",
                        commons_url="test",
                        columns_to_fields={
                            "authz": "path:authz",
                            "tags": "path:tags",
                            "_subjects_count": "path:subjects_count",
                            "dbgap_accession_number": "path:study_id",
                            "study_description": "path:study_description_summary",
                            "number_of_datafiles": "path:data_files_count",
                            "investigator": "path:contributor",
                        },
                    ),
                },
                adapter_commons={
                    "adapter_commons": AdapterMDSInstance(
                        mds_url="",
                        commons_url="",
                        adapter="icpsr",
                    ),
                },
            )
        )

    # check that the get_all_metadata  return value has not been changed
    # since drop_all should not be called if an exception has been raised
    es = await datastore.init("test", 9200)
    assert (await es.get_all_metadata()) == existing_metadata

    respx.get(
        "http://test/ok//mds/metadata?data=True&_guid_type=discovery_metadata&limit=1000&offset=0"
    ).mock(
        return_value=httpx.Response(
            status_code=200,
            json=existing_metadata,
        )
    )

    # Unable to update temp index, raise exception
    patch.object(
        datastore,
        "update_metadata",
        AsyncMock(side_effect=Exception("Unable")),
    ).start()
    with pytest.raises(Exception):
        await main(
            Commons(
                configuration=Config(
                    settings=Settings(),
                    schema={
                        "_subjects_count": FieldDefinition(type="integer"),
                        "year_awarded": FieldDefinition(type="integer"),
                    },
                ),
                gen3_commons={
                    "my_commons": MDSInstance(
                        mds_url="http://test/ok/",
                        commons_url="test",
                        columns_to_fields={
                            "authz": "path:authz",
                            "tags": "path:tags",
                            "_subjects_count": "path:subjects_count",
                            "dbgap_accession_number": "path:study_id",
                            "study_description": "path:study_description_summary",
                            "number_of_datafiles": "path:data_files_count",
                            "investigator": "path:contributor",
                        },
                    ),
                },
                adapter_commons={
                    "adapter_commons": AdapterMDSInstance(
                        mds_url="",
                        commons_url="",
                        adapter="icpsr",
                    ),
                },
            )
        )

    assert (await es.get_all_metadata()) == existing_metadata


@pytest.mark.asyncio
async def test_filter_entries():
    resp = await filter_entries(
        MDSInstance(
            "http://mds",
            "http://commons",
            {
                "short_name": "name",
                "full_name": "full_name",
                "_subjects_count": "_subjects_count",
                "study_id": "study_id",
                "_unique_id": "_unique_id",
                "study_description": "study_description",
            },
            select_field={
                "field_name": "my_field",
                "field_value": 71,
            },
        ),
        [
            {
                "short_name": {"gen3_discovery": {"my_field": 71}},
            },
            {
                "short_name": {"different_field": {"my_field": 0}},
            },
            {
                "short_name": {"gen3_discovery": {"my_field": 0}},
            },
        ],
    )
    assert resp == [{"short_name": {"gen3_discovery": {"my_field": 71}}}]


def test_parse_config_from_file():
    with NamedTemporaryFile(mode="w+", delete=False) as fp:
        json.dump(
            {
                "configuration": {
                    "schema": {
                        "_subjects_count": {"type": "integer"},
                        "study_description": {},
                    },
                },
                "gen3_commons": {
                    "mycommons": {
                        "mds_url": "http://mds",
                        "commons_url": "http://commons",
                        "columns_to_fields": {
                            "short_name": "name",
                            "full_name": "full_name",
                            "_subjects_count": "_subjects_count",
                            "study_id": "study_id",
                            "_unique_id": "_unique_id",
                            "study_description": "study_description",
                        },
                    },
                },
                "adapter_commons": {
                    "non-gen3": {
                        "mds_url": "http://non-gen3",
                        "commons_url": "non-gen3",
                        "adapter": "icpsr",
                    }
                },
            },
            fp,
        )
    config = parse_config_from_file(Path(fp.name))
    assert (
        config.to_json()
        == Commons(
            configuration=Config(
                settings=Settings(),
                schema={
                    "_subjects_count": FieldDefinition(type="integer"),
                    "study_description": FieldDefinition(type="string"),
                },
            ),
            gen3_commons={
                "mycommons": MDSInstance(
                    "http://mds",
                    "http://commons",
                    {
                        "short_name": "name",
                        "full_name": "full_name",
                        "_subjects_count": "_subjects_count",
                        "study_id": "study_id",
                        "_unique_id": "_unique_id",
                        "study_description": "study_description",
                    },
                )
            },
            adapter_commons={
                "non-gen3": AdapterMDSInstance(
                    "http://non-gen3",
                    "non-gen3",
                    "icpsr",
                )
            },
        ).to_json()
    )

    assert parse_config_from_file(Path("dummmy_files")) is None

    try:
        parse_config_from_file(Path("/"))
    except Exception as exc:
        assert isinstance(exc, IOError) is True
