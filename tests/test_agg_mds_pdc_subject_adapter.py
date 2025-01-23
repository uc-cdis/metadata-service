import respx
import httpx

from mds.agg_mds.adapters import get_metadata


@respx.mock
def test_get_metadata_pdc_subject():
    json_response = r"""
    {
        "data": {
        "getPaginatedUICase" : { "total": 2 },
            "getPaginatedUIClinical": {
            "total": 2,
            "uiClinical": [
                {
                "aliquot_id": "test_aliquot_id_0",
                "sample_id": "test_sample_id_0",
                "case_id": "test_case_id_0",
                "aliquot_submitter_id": "AP-ABCD",
                "aliquot_is_ref": "No",
                "aliquot_status": "Qualified",
                "aliquot_quantity": null,
                "aliquot_volume": null,
                "amount": null,
                "analyte_type": null,
                "concentration": null,
                "case_status": "Qualified",
                "sample_status": "Qualified",
                "sample_submitter_id": "AP-ABCD",
                "sample_is_ref": null,
                "biospecimen_anatomic_site": null,
                "biospecimen_laterality": null,
                "composition": "Solid Tissue",
                "current_weight": null,
                "days_to_collection": null,
                "days_to_sample_procurement": null,
                "diagnosis_pathologically_confirmed": null,
                "freezing_method": null,
                "initial_weight": null,
                "intermediate_dimension": null,
                "longest_dimension": null,
                "method_of_sample_procurement": null,
                "pathology_report_uuid": null,
                "preservation_method": null,
                "sample_type_id": null,
                "shortest_dimension": null,
                "time_between_clamping_and_freezing": null,
                "time_between_excision_and_freezing": null,
                "tissue_type": "Tumor",
                "tumor_code": null,
                "tumor_code_id": null,
                "tumor_descriptor": null,
                "case_submitter_id": "AP-ABCD",
                "program_name": "Applied Proteogenomics OrganizationaL Learning and Outcomes - APOLLO",
                "project_name": "APOLLO1",
                "sample_type": "Primary Tumor",
                "disease_type": "Test Type",
                "primary_site": "Bronchus and lung",
                "tissue_collection_type": null,
                "sample_ordinal": null,
                "__typename": "UICase"
                },
                {
                "aliquot_id": "test_aliquot_id_1",
                "sample_id": "test_sample_id_1",
                "case_id": "test_case_id_1",
                "aliquot_submitter_id": "AP-DCBA",
                "aliquot_is_ref": "No",
                "aliquot_status": "Qualified",
                "aliquot_quantity": null,
                "aliquot_volume": null,
                "amount": null,
                "analyte_type": null,
                "concentration": null,
                "case_status": "Qualified",
                "sample_status": "Qualified",
                "sample_submitter_id": "AP-DCBA",
                "sample_is_ref": null,
                "biospecimen_anatomic_site": null,
                "biospecimen_laterality": null,
                "composition": "Solid Tissue",
                "current_weight": null,
                "days_to_collection": null,
                "days_to_sample_procurement": null,
                "diagnosis_pathologically_confirmed": null,
                "freezing_method": null,
                "initial_weight": null,
                "intermediate_dimension": null,
                "longest_dimension": null,
                "method_of_sample_procurement": null,
                "pathology_report_uuid": null,
                "preservation_method": null,
                "sample_type_id": null,
                "shortest_dimension": null,
                "time_between_clamping_and_freezing": null,
                "time_between_excision_and_freezing": null,
                "tissue_type": "Tumor",
                "tumor_code": null,
                "tumor_code_id": null,
                "tumor_descriptor": null,
                "case_submitter_id": "AP-DCBA",
                "program_name": "Applied Proteogenomics OrganizationaL Learning and Outcomes - APOLLO",
                "project_name": "APOLLO1",
                "sample_type": "Primary Tumor",
                "disease_type": "Test Type 1",
                "primary_site": "Feet",
                "tissue_collection_type": null,
                "sample_ordinal": null,
                "__typename": "UICase"
                }]
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
        "_unique_id": "path:case_id",
        "subject_apollo_id": "path:sample_submitter_id",
        "aliquot_id": "path:aliquot_id",
        "sample_id": "path:sample_id",
        "aliquot_submitter_id": "path:aliquot_submitter_id",
        "aliquot_is_ref": "path:aliquot_is_ref",
        "aliquot_status": "path:aliquot_status",
        "aliquot_quantity": "path:aliquot_quantity",
        "aliquot_volume": "path:aliquot_volume",
        "amount": "path:amount",
        "analyte_type": "path:analyte_type",
        "concentration": "path:concentration",
        "case_status": "path:case_status",
        "sample_status": "path:sample_status",
        "sample_is_ref": "path:sample_is_ref",
        "biospecimen_anatomic_site": "path:biospecimen_anatomic_site",
        "biospecimen_laterality": "path:biospecimen_laterality",
        "composition": "path:composition",
        "current_weight": "path:current_weight",
        "days_to_collection": "path:days_to_collection",
        "days_to_sample_procurement": "path:days_to_sample_procurement",
        "diagnosis_pathologically_confirmed": "path:diagnosis_pathologically_confirmed",
        "freezing_method": "path:freezing_method",
        "initial_weight": "path:initial_weight",
        "intermediate_dimension": "path:intermediate_dimension",
        "longest_dimension": "path:longest_dimension",
        "method_of_sample_procurement": "path:method_of_sample_procurement",
        "pathology_report_uuid": "path:pathology_report_uuid",
        "preservation_method": "path:preservation_method",
        "sample_type_id": "path:sample_type_id",
        "shortest_dimension": "path:shortest_dimension",
        "time_between_clamping_and_freezing": "path:time_between_clamping_and_freezing",
        "time_between_excision_and_freezing": "path:time_between_excision_and_freezing",
        "tissue_type": "path:tissue_type",
        "tumor_code": "path:tumor_code",
        "tumor_code_id": "path:tumor_code_id",
        "tumor_descriptor": "path:tumor_descriptor",
        "case_submitter_id": "path:case_submitter_id",
        "program_name": "path:program_name",
        "project_name": "path:project_name",
        "sample_type": "path:sample_type",
        "disease_type": "path:disease_type",
        "primary_site": "path:primary_site",
        "tissue_collection_type": "path:tissue_collection_type",
        "sample_ordinal": "path:sample_ordinal",
        "__typename": "path:__typename",
    }

    query = """
                    query FilteredCasesDataPaginated($offset_value: Int, $limit_value: Int, $sort_value: String, $program_name_filter: String!, $project_name_filter: String!, $study_name_filter: String!, $disease_filter: String!, $filterValue: String!, $analytical_frac_filter: String!, $exp_type_filter: String!, $ethnicity_filter: String!, $race_filter: String!, $gender_filter: String!, $tumor_grade_filter: String!, $sample_type_filter: String!, $acquisition_type_filter: String!, $data_category_filter: String!, $file_type_filter: String!, $access_filter: String!, $downloadable_filter: String!, $biospecimen_status_filter: String!, $case_status_filter: String!, $getAll: Boolean!) {
                    getPaginatedUICase(offset: $offset_value, limit: $limit_value, sort: $sort_value, program_name: $program_name_filter, project_name: $project_name_filter, study_name: $study_name_filter, disease_type: $disease_filter, primary_site: $filterValue, analytical_fraction: $analytical_frac_filter, experiment_type: $exp_type_filter, ethnicity: $ethnicity_filter, race: $race_filter, gender: $gender_filter, tumor_grade: $tumor_grade_filter, sample_type: $sample_type_filter, acquisition_type: $acquisition_type_filter, data_category: $data_category_filter, file_type: $file_type_filter, access: $access_filter, downloadable: $downloadable_filter, biospecimen_status: $biospecimen_status_filter, case_status: $case_status_filter, getAll: $getAll) {
                        total
                        uiCases {
                        aliquot_id
                        sample_id
                        case_id
                        aliquot_submitter_id
                        aliquot_is_ref
                        aliquot_status
                        aliquot_quantity
                        aliquot_volume
                        amount
                        analyte_type
                        concentration
                        case_status
                        sample_status
                        sample_submitter_id
                        sample_is_ref
                        biospecimen_anatomic_site
                        biospecimen_laterality
                        composition
                        current_weight
                        days_to_collection
                        days_to_sample_procurement
                        diagnosis_pathologically_confirmed
                        freezing_method
                        initial_weight
                        intermediate_dimension
                        longest_dimension
                        method_of_sample_procurement
                        pathology_report_uuid
                        preservation_method
                        sample_type_id
                        shortest_dimension
                        time_between_clamping_and_freezing
                        time_between_excision_and_freezing
                        tissue_type
                        tumor_code
                        tumor_code_id
                        tumor_descriptor
                        case_submitter_id
                        program_name
                        project_name
                        sample_type
                        disease_type
                        primary_site
                        tissue_collection_type
                        sample_ordinal
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
        "limit_value": 1000,
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

    respx.post("http://test/ok").mock(
        return_value=httpx.Response(status_code=200, content=json_response)
    )

    filters = {"size": 5}

    results = get_metadata("pdcsubject", None, filters=filters, mappings=field_mappings)
    assert results == {}

    expected_result = {
        "test_case_id_0": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "authz": "/VA",
                "tags": [
                    {"name": "Test Type", "category": "disease_type"},
                    {"name": "Bronchus and lung", "category": "primary_site"},
                ],
                "_unique_id": "test_case_id_0",
                "subject_apollo_id": "AP-ABCD",
                "aliquot_id": "test_aliquot_id_0",
                "sample_id": "test_sample_id_0",
                "aliquot_submitter_id": "AP-ABCD",
                "aliquot_is_ref": "No",
                "aliquot_status": "Qualified",
                "aliquot_quantity": None,
                "aliquot_volume": None,
                "amount": None,
                "analyte_type": None,
                "concentration": None,
                "case_status": "Qualified",
                "sample_status": "Qualified",
                "sample_is_ref": None,
                "biospecimen_anatomic_site": None,
                "biospecimen_laterality": None,
                "composition": "Solid Tissue",
                "current_weight": None,
                "days_to_collection": None,
                "days_to_sample_procurement": None,
                "diagnosis_pathologically_confirmed": None,
                "freezing_method": None,
                "initial_weight": None,
                "intermediate_dimension": None,
                "longest_dimension": None,
                "method_of_sample_procurement": None,
                "pathology_report_uuid": None,
                "preservation_method": None,
                "sample_type_id": None,
                "shortest_dimension": None,
                "time_between_clamping_and_freezing": None,
                "time_between_excision_and_freezing": None,
                "tissue_type": "Tumor",
                "tumor_code": None,
                "tumor_code_id": None,
                "tumor_descriptor": None,
                "case_submitter_id": "AP-ABCD",
                "program_name": "Applied Proteogenomics OrganizationaL Learning and Outcomes - APOLLO",
                "project_name": "APOLLO1",
                "sample_type": "Primary Tumor",
                "disease_type": "Test Type",
                "primary_site": "Bronchus and lung",
                "tissue_collection_type": None,
                "sample_ordinal": None,
                "__typename": "UICase",
            },
        },
        "test_case_id_1": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "authz": "/VA",
                "tags": [
                    {"name": "Test Type 1", "category": "disease_type"},
                    {"name": "Feet", "category": "primary_site"},
                ],
                "_unique_id": "test_case_id_1",
                "subject_apollo_id": "AP-DCBA",
                "aliquot_id": "test_aliquot_id_1",
                "sample_id": "test_sample_id_1",
                "aliquot_submitter_id": "AP-DCBA",
                "aliquot_is_ref": "No",
                "aliquot_status": "Qualified",
                "aliquot_quantity": None,
                "aliquot_volume": None,
                "amount": None,
                "analyte_type": None,
                "concentration": None,
                "case_status": "Qualified",
                "sample_status": "Qualified",
                "sample_is_ref": None,
                "biospecimen_anatomic_site": None,
                "biospecimen_laterality": None,
                "composition": "Solid Tissue",
                "current_weight": None,
                "days_to_collection": None,
                "days_to_sample_procurement": None,
                "diagnosis_pathologically_confirmed": None,
                "freezing_method": None,
                "initial_weight": None,
                "intermediate_dimension": None,
                "longest_dimension": None,
                "method_of_sample_procurement": None,
                "pathology_report_uuid": None,
                "preservation_method": None,
                "sample_type_id": None,
                "shortest_dimension": None,
                "time_between_clamping_and_freezing": None,
                "time_between_excision_and_freezing": None,
                "tissue_type": "Tumor",
                "tumor_code": None,
                "tumor_code_id": None,
                "tumor_descriptor": None,
                "case_submitter_id": "AP-DCBA",
                "program_name": "Applied Proteogenomics OrganizationaL Learning and Outcomes - APOLLO",
                "project_name": "APOLLO1",
                "sample_type": "Primary Tumor",
                "disease_type": "Test Type 1",
                "primary_site": "Feet",
                "tissue_collection_type": None,
                "sample_ordinal": None,
                "__typename": "UICase",
            },
        },
    }

    results = get_metadata(
        "pdcsubject", "http://test/ok", filters=filters, mappings=field_mappings
    )
    assert results == expected_result

    expected_result = {
        "test_case_id_0": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "authz": "/VA",
                "tags": [
                    {"name": "Test Type", "category": "disease_type"},
                    {"name": "Bronchus and lung", "category": "primary_site"},
                ],
                "_unique_id": "test_case_id_0",
                "subject_apollo_id": "AP-ABCD",
                "aliquot_id": "test_aliquot_id_0",
                "sample_id": "test_sample_id_0",
                "case_id": "test_case_id_0",
                "aliquot_submitter_id": "AP-ABCD",
                "aliquot_is_ref": "No",
                "aliquot_status": "Qualified",
                "aliquot_quantity": None,
                "aliquot_volume": None,
                "amount": None,
                "analyte_type": None,
                "concentration": None,
                "case_status": "Qualified",
                "sample_status": "Qualified",
                "sample_submitter_id": "AP-ABCD",
                "sample_is_ref": None,
                "biospecimen_anatomic_site": None,
                "biospecimen_laterality": None,
                "composition": "Solid Tissue",
                "current_weight": None,
                "days_to_collection": None,
                "days_to_sample_procurement": None,
                "diagnosis_pathologically_confirmed": None,
                "freezing_method": None,
                "initial_weight": None,
                "intermediate_dimension": None,
                "longest_dimension": None,
                "method_of_sample_procurement": None,
                "pathology_report_uuid": None,
                "preservation_method": None,
                "sample_type_id": None,
                "shortest_dimension": None,
                "time_between_clamping_and_freezing": None,
                "time_between_excision_and_freezing": None,
                "tissue_type": "Tumor",
                "tumor_code": None,
                "tumor_code_id": None,
                "tumor_descriptor": None,
                "case_submitter_id": "AP-ABCD",
                "program_name": "Applied Proteogenomics OrganizationaL Learning and Outcomes - APOLLO",
                "project_name": "APOLLO1",
                "sample_type": "Primary Tumor",
                "disease_type": "Test Type",
                "primary_site": "Bronchus and lung",
                "tissue_collection_type": None,
                "sample_ordinal": None,
                "__typename": "UICase",
            },
        },
        "test_case_id_1": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "authz": "/VA",
                "tags": [
                    {"name": "Test Type 1", "category": "disease_type"},
                    {"name": "Feet", "category": "primary_site"},
                ],
                "_unique_id": "test_case_id_1",
                "subject_apollo_id": "AP-DCBA",
                "aliquot_id": "test_aliquot_id_1",
                "sample_id": "test_sample_id_1",
                "case_id": "test_case_id_1",
                "aliquot_submitter_id": "AP-DCBA",
                "aliquot_is_ref": "No",
                "aliquot_status": "Qualified",
                "aliquot_quantity": None,
                "aliquot_volume": None,
                "amount": None,
                "analyte_type": None,
                "concentration": None,
                "case_status": "Qualified",
                "sample_status": "Qualified",
                "sample_submitter_id": "AP-DCBA",
                "sample_is_ref": None,
                "biospecimen_anatomic_site": None,
                "biospecimen_laterality": None,
                "composition": "Solid Tissue",
                "current_weight": None,
                "days_to_collection": None,
                "days_to_sample_procurement": None,
                "diagnosis_pathologically_confirmed": None,
                "freezing_method": None,
                "initial_weight": None,
                "intermediate_dimension": None,
                "longest_dimension": None,
                "method_of_sample_procurement": None,
                "pathology_report_uuid": None,
                "preservation_method": None,
                "sample_type_id": None,
                "shortest_dimension": None,
                "time_between_clamping_and_freezing": None,
                "time_between_excision_and_freezing": None,
                "tissue_type": "Tumor",
                "tumor_code": None,
                "tumor_code_id": None,
                "tumor_descriptor": None,
                "case_submitter_id": "AP-DCBA",
                "program_name": "Applied Proteogenomics OrganizationaL Learning and Outcomes - APOLLO",
                "project_name": "APOLLO1",
                "sample_type": "Primary Tumor",
                "disease_type": "Test Type 1",
                "primary_site": "Feet",
                "tissue_collection_type": None,
                "sample_ordinal": None,
                "__typename": "UICase",
            },
        },
    }

    actual_result = get_metadata(
        "pdcsubject",
        "http://test/ok",
        filters=filters,
        mappings=field_mappings,
        keepOriginalFields=True,
    )
    assert actual_result == expected_result

    respx.post(
        "http://test/ok",
    ).mock(side_effect=httpx.TimeoutException)

    assert (
        get_metadata(
            "pdcsubject", "http://test/ok", filters=filters, mappings=field_mappings
        )
        == {}
    )

    respx.post("http://test/ok").mock(
        side_effect=httpx.HTTPError("This is a HTTP Error")
    )

    assert (
        get_metadata(
            "pdcsubject", "http://test/ok", filters=filters, mappings=field_mappings
        )
        == {}
    )

    respx.post("http://test/ok").mock(side_effect=Exception)

    assert (
        get_metadata(
            "pdcsubject", "http://test/ok", filters=filters, mappings=field_mappings
        )
        == {}
    )
