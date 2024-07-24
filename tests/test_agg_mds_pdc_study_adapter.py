import respx
import httpx

from mds.agg_mds.adapters import get_metadata


@respx.mock
def test_get_metadata_pdc_study():
    json_response = r"""
    {
        "data": {
            "getPaginatedUIStudy": {
                "total": 1,
                "uiStudies": [
                    {
                        "study_id": "study_id_1",
                        "pdc_study_id": "PDC000123",
                        "submitter_id_name": "APOLLO LUAD - Phosphoproteome - FeNTA",
                        "study_description": "We present a deep proteogenomic profiling study of 87 lung adenocarcinoma (LUAD) tumors obtained in the United States",
                        "study_submitter_id": "APOLLO LUAD - Phosphoproteome - FeNTA",
                        "program_name": "Applied Proteogenomics OrganizationaL Learning and Outcomes - APOLLO",
                        "project_name": "APOLLO1",
                        "disease_type": "Lung Adenocarcinoma;Other",
                        "primary_site": "Bronchus and lung;Not Reported",
                        "analytical_fraction": "Phosphoproteome",
                        "experiment_type": "TMT11",
                        "embargo_date": null,
                        "cases_count": 101,
                        "aliquots_count": 101,
                        "filesCount": [
                            {
                            "file_type": "Document",
                            "data_category": "Other Metadata",
                            "files_count": 1,
                            "__typename": "File"
                            },
                            {
                            "file_type": "Open Standard",
                            "data_category": "Peptide Spectral Matches",
                            "files_count": 120,
                            "__typename": "File"
                            }
                            ],
                            "supplementaryFilesCount": [
                                {
                                "data_category": "Other Metadata",
                                "file_type": "Document",
                                "files_count": 1,
                                "__typename": "File"
                                }
                            ],
                        "nonSupplementaryFilesCount": [
                            {
                            "data_category": "Peptide Spectral Matches",
                            "file_type": "Open Standard",
                            "files_count": 120,
                            "__typename": "File"
                            },
                            {
                            "data_category": "Peptide Spectral Matches",
                            "file_type": "Text",
                            "files_count": 120,
                            "__typename": "File"
                            }
                        ],
                        "contacts": [],
                        "versions": [
                            {
                            "number": "1",
                            "__typename": "Version"
                            }
                        ],
                        "__typename": "UIStudy"
                    }
                ]
            }
        }
    }
    """

    field_mappings = {
        "authz": "/VA",
        "tags": [
            {"name": "path:disease_type", "category": "disease_type"},
            {"name": "path:primary_site", "category": "primary_site"},
        ],
        "_unique_id": "path:study_id",
        "pdc_study_id": "path:pdc_study_id",
        "submitter_id_name": "path:submitter_id_name",
        "study_description": "path:study_description",
        "study_submitter_id": "path:study_submitter_id",
        "program_name": "path:program_name",
        "project_name": "path:project_name",
        "disease_type": "path:disease_type",
        "primary_site": "path:primary_site",
        "analytical_fraction": "path:analytical_fraction",
        "experiment_type": "path:experiment_type",
        "embargo_date": "path:embargo_date",
        "cases_count": "path:cases_count",
        "aliquots_count": "path:aliquots_count",
        "filesCount": "path:filesCount",
        "supplementaryFilesCount": "path:supplementaryFilesCount",
        "nonSupplementaryFilesCount": "path:nonSupplementaryFilesCount",
        "contacts": "path:contacts",
        "versions": "path:versions",
        "__typename": "path:__typename",
        "$myVar.id#$!_+~*/": "path:invalid_path",
    }

    query = """
                    query FilteredStudiesDataPaginated(
                    $offset_value: Int
                    $limit_value: Int
                    $sort_value: String
                    $program_name_filter: String!
                    $project_name_filter: String!
                    $study_name_filter: String!
                    $disease_filter: String!
                    $filterValue: String!
                    $analytical_frac_filter: String!
                    $exp_type_filter: String!
                    $ethnicity_filter: String!
                    $race_filter: String!
                    $gender_filter: String!
                    $tumor_grade_filter: String!
                    $sample_type_filter: String!
                    $acquisition_type_filter: String!
                    $data_category_filter: String!
                    $file_type_filter: String!
                    $access_filter: String!
                    $downloadable_filter: String!
                    $biospecimen_status_filter: String!
                    $case_status_filter: String!
                    $getAll: Boolean!
                    ) {
                    getPaginatedUIStudy(
                        offset: $offset_value
                        limit: $limit_value
                        sort: $sort_value
                        program_name: $program_name_filter
                        project_name: $project_name_filter
                        study_name: $study_name_filter
                        disease_type: $disease_filter
                        primary_site: $filterValue
                        analytical_fraction: $analytical_frac_filter
                        experiment_type: $exp_type_filter
                        ethnicity: $ethnicity_filter
                        race: $race_filter
                        gender: $gender_filter
                        tumor_grade: $tumor_grade_filter
                        sample_type: $sample_type_filter
                        acquisition_type: $acquisition_type_filter
                        data_category: $data_category_filter
                        file_type: $file_type_filter
                        access: $access_filter
                        downloadable: $downloadable_filter
                        biospecimen_status: $biospecimen_status_filter
                        case_status: $case_status_filter
                        getAll: $getAll
                    ) {
                        total
                        uiStudies {
                        study_id
                        pdc_study_id
                        submitter_id_name
                        study_description
                        study_submitter_id
                        program_name
                        project_name
                        disease_type
                        primary_site
                        analytical_fraction
                        experiment_type
                        embargo_date
                        cases_count
                        aliquots_count
                        filesCount {
                            file_type
                            data_category
                            files_count
                            __typename
                        }
                        supplementaryFilesCount {
                            data_category
                            file_type
                            files_count
                            __typename
                        }
                        nonSupplementaryFilesCount {
                            data_category
                            file_type
                            files_count
                            __typename
                        }
                        contacts {
                            name
                            institution
                            email
                            url
                            __typename
                        }
                        versions {
                            number
                            __typename
                        }
                        __typename
                        }
                        pagination {
                        count
                        sort
                        from
                        page
                        total
                        pages
                        size
                        __typename
                        }
                        __typename
                    }
                    }
                """

    variables = {
        "offset_value": 0,
        "limit_value": 10,
        "sort_value": "",
        "program_name_filter": "Applied Proteogenomics OrganizationaL Learning and Outcomes - APOLLO",
        "project_name_filter": "",
        "study_name_filter": "",
        "disease_filter": "",
        "filterValue": "",
        "analytical_frac_filter": "",
        "exp_type_filter": "",
        "ethnicity_filter": "",
        "race_filter": "",
        "gender_filter": "",
        "tumor_grade_filter": "",
        "sample_type_filter": "",
        "acquisition_type_filter": "",
        "data_category_filter": "",
        "file_type_filter": "",
        "access_filter": "",
        "downloadable_filter": "",
        "biospecimen_status_filter": "",
        "case_status_filter": "",
        "getAll": False,
    }

    respx.post(
        "http://test/ok",
        json={"query": query, "variables": variables},
    ).mock(return_value=httpx.Response(status_code=200, content=json_response))

    filters = {"size": 5}

    assert (
        get_metadata("pdcstudy", None, filters=filters, mappings=field_mappings) == {}
    )

    expected_result = {
        "study_id_1": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "authz": "/VA",
                "tags": [
                    {"name": "Lung Adenocarcinoma", "category": "disease_type"},
                    {"name": "Other", "category": "disease_type"},
                    {"name": "Bronchus and lung", "category": "primary_site"},
                    {"name": "Not Reported", "category": "primary_site"},
                ],
                "_unique_id": "study_id_1",
                "pdc_study_id": "PDC000123",
                "submitter_id_name": "APOLLO LUAD - Phosphoproteome - FeNTA",
                "study_description": "We present a deep proteogenomic profiling study of 87 lung adenocarcinoma (LUAD) tumors obtained in the United States",
                "study_submitter_id": "APOLLO LUAD - Phosphoproteome - FeNTA",
                "program_name": "Applied Proteogenomics OrganizationaL Learning and Outcomes - APOLLO",
                "project_name": "APOLLO1",
                "disease_type": "Lung Adenocarcinoma;Other",
                "primary_site": "Bronchus and lung;Not Reported",
                "analytical_fraction": "Phosphoproteome",
                "experiment_type": "TMT11",
                "embargo_date": None,
                "cases_count": 101,
                "aliquots_count": 101,
                "filesCount": [
                    {
                        "file_type": "Document",
                        "data_category": "Other Metadata",
                        "files_count": 1,
                        "__typename": "File",
                    },
                    {
                        "file_type": "Open Standard",
                        "data_category": "Peptide Spectral Matches",
                        "files_count": 120,
                        "__typename": "File",
                    },
                ],
                "supplementaryFilesCount": [
                    {
                        "data_category": "Other Metadata",
                        "file_type": "Document",
                        "files_count": 1,
                        "__typename": "File",
                    }
                ],
                "nonSupplementaryFilesCount": [
                    {
                        "data_category": "Peptide Spectral Matches",
                        "file_type": "Open Standard",
                        "files_count": 120,
                        "__typename": "File",
                    },
                    {
                        "data_category": "Peptide Spectral Matches",
                        "file_type": "Text",
                        "files_count": 120,
                        "__typename": "File",
                    },
                ],
                "contacts": [],
                "versions": [{"number": "1", "__typename": "Version"}],
                "__typename": "UIStudy",
            },
        },
    }

    actual_result = get_metadata(
        "pdcstudy", "http://test/ok", filters=filters, mappings=field_mappings
    )
    assert actual_result == expected_result

    expected_result = {
        "study_id_1": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "authz": "/VA",
                "tags": [
                    {"name": "Lung Adenocarcinoma", "category": "disease_type"},
                    {"name": "Other", "category": "disease_type"},
                    {"name": "Bronchus and lung", "category": "primary_site"},
                    {"name": "Not Reported", "category": "primary_site"},
                ],
                "_unique_id": "study_id_1",
                "study_id": "study_id_1",
                "pdc_study_id": "PDC000123",
                "submitter_id_name": "APOLLO LUAD - Phosphoproteome - FeNTA",
                "study_description": "We present a deep proteogenomic profiling study of 87 lung adenocarcinoma (LUAD) tumors obtained in the United States",
                "study_submitter_id": "APOLLO LUAD - Phosphoproteome - FeNTA",
                "program_name": "Applied Proteogenomics OrganizationaL Learning and Outcomes - APOLLO",
                "project_name": "APOLLO1",
                "disease_type": "Lung Adenocarcinoma;Other",
                "primary_site": "Bronchus and lung;Not Reported",
                "analytical_fraction": "Phosphoproteome",
                "experiment_type": "TMT11",
                "embargo_date": None,
                "cases_count": 101,
                "aliquots_count": 101,
                "filesCount": [
                    {
                        "file_type": "Document",
                        "data_category": "Other Metadata",
                        "files_count": 1,
                        "__typename": "File",
                    },
                    {
                        "file_type": "Open Standard",
                        "data_category": "Peptide Spectral Matches",
                        "files_count": 120,
                        "__typename": "File",
                    },
                ],
                "supplementaryFilesCount": [
                    {
                        "data_category": "Other Metadata",
                        "file_type": "Document",
                        "files_count": 1,
                        "__typename": "File",
                    }
                ],
                "nonSupplementaryFilesCount": [
                    {
                        "data_category": "Peptide Spectral Matches",
                        "file_type": "Open Standard",
                        "files_count": 120,
                        "__typename": "File",
                    },
                    {
                        "data_category": "Peptide Spectral Matches",
                        "file_type": "Text",
                        "files_count": 120,
                        "__typename": "File",
                    },
                ],
                "contacts": [],
                "versions": [{"number": "1", "__typename": "Version"}],
                "__typename": "UIStudy",
            },
        },
    }

    actual_result = get_metadata(
        "pdcstudy",
        "http://test/ok",
        filters=filters,
        mappings=field_mappings,
        keepOriginalFields=True,
    )
    assert actual_result == expected_result

    respx.post(
        "http://test/ok",
        json={"query": query, "variables": variables},
    ).mock(side_effect=httpx.TimeoutException)

    assert (
        get_metadata(
            "pdcstudy", "http://test/ok", filters=filters, mappings=field_mappings
        )
        == {}
    )

    respx.post(
        "http://test/ok",
        json={"query": query, "variables": variables},
    ).mock(side_effect=httpx.HTTPError("This is a HTTP Error"))

    assert (
        get_metadata(
            "pdcstudy", "http://test/ok", filters=filters, mappings=field_mappings
        )
        == {}
    )

    respx.post(
        "http://test/ok",
        json={"query": query, "variables": variables},
    ).mock(side_effect=Exception)

    assert (
        get_metadata(
            "pdcstudy", "http://test/ok", filters=filters, mappings=field_mappings
        )
        == {}
    )
