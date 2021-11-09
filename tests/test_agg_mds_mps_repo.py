
import json
from mds.agg_mds.adapters import get_metadata, get_json_path_value
from tenacity import RetryError, wait_none
import httpx
import respx



@respx.mock
def test_get_metadata_mps():
  
  # note this is only the first part of the json response but 
  # as the last part are for individual assays/groups within study
  json_response = {
    'id': '23',
    'name': 'Motif-PS-Chipshop',
    'data_group': 'Taylor_MPS',
    'study_types': 'CC',
    'start_date': '2015-09-30',
    'description': 'Motif polystyrene devices, using different flow setups. Human hepatocytes fresh isolated were used in. (3 cell model)',
    }
    
    
  field_mappings = {
    "tags": [
        {
            "name":"MPS", 
            "category":"Commons"
        },
        {
            "name":"Physiological", 
            "category":"Data Type"
        }
    ],
    "authz": "",
    "sites": "",
    "summary": "path:description",
    "study_url" : "path:url",
    "location": "path:data_group",
    "subjects": "",
    "__manifest": "",
    "study_name": "path:name",
    "study_type": "path:study_types",
    "institutions": "path:data_group",
    "year_awarded": "",
    "investigators": "path:data_group",
    "project_title": "path:title",
    "protocol_name": "",
    "study_summary": "",
    "_file_manifest": "",
    "dataset_1_type": "",
    "dataset_2_type": "",
    "dataset_3_type": "",
    "dataset_4_type": "",
    "dataset_5_type": "",
    "project_number": "",
    "dataset_1_title": "",
    "dataset_2_title": "",
    "dataset_3_title": "",
    "dataset_4_title": "",
    "dataset_5_title": "",
    "administering_ic": "",
    "advSearchFilters": [],
    "dataset_category": "",
    "research_program": "",
    "research_question": "",
    "study_description": "",
    "clinical_trial_link": "",
    "dataset_description": "",
    "research_focus_area": "",
    "dataset_1_description": "",
    "dataset_2_description": "",
    "dataset_3_description": "",
    "dataset_4_description": "",
    "dataset_5_description": "",
    "data_availability": ""
 }
 
 
assert get_metadata("mps", "http://test/ok", filters=None, config=None) == {}
 
assert (
        get_metadata("mps", "http://test/ok", filters=None, config={"batchSize": 256})
        == {}
  )
    
assert (
        get_metadata("mps", mds_url=None, filters={"study_ids": [23]}, config=None)
        == {}
    )
    
respx.get(
        "http://test/ok23",
        status_code=200,
        content=json_response,
    )
    
assert get_metadata(
        "mps",
        "http://test/ok",
        filters={"study_ids": [23]},
        mappings=field_mappings,
        keepOriginalFields=True,
    )=={
      'MPS_study_23': {'_guid_type': 'discovery_metadata',
  'gen3_discovery': {'tags': [{'name': 'MPS', 'category': 'Commons'},
      {'name': 'Physiological', 'category': 'Data Type'}],
    'authz': '',
    'sites': '',
    'summary': '',
    'study_url': '',
    'location': '',
    'subjects': '',
    '__manifest': '',
    'study_name': '',
    'study_type': '',
    'institutions': '',
    'year_awarded': '',
    'investigators': '',
    'project_title': '',
    'protocol_name': '',
    'study_summary': '',
    '_file_manifest': '',
    'dataset_1_type': '',
    'dataset_2_type': '',
    'dataset_3_type': '',
    'dataset_4_type': '',
    'dataset_5_type': '',
    'project_number': '',
    'dataset_1_title': '',
    'dataset_2_title': '',
    'dataset_3_title': '',
    'dataset_4_title': '',
    'dataset_5_title': '',
    'administering_ic': '',
    'advSearchFilters': [],
    'dataset_category': '',
    'research_program': '',
    'research_question': '',
    'study_description': '',
    'clinical_trial_link': '',
    'dataset_description': '',
    'research_focus_area': '',
    'dataset_1_description': '',
    'dataset_2_description': '',
    'dataset_3_description': '',
    'dataset_4_description': '',
    'dataset_5_description': '',
    'data_availability': ''}}}

try:
        from mds.agg_mds.adapters import MPSAdapter

        MPSAdapter.getRemoteDataAsJson.retry.wait = wait_none()

        respx.get(
            "http://test/timeouterror?verb=GetRecord&metadataPrefix=oai_dc&identifier=64",
            content=httpx.TimeoutException,
        )
        get_metadata(
            "mps",
            "http://test/timeouterror/",
            filters={"study_ids": [23]},
            mappings=field_mappings,
            keepOriginalFields=True,
        )
except Exception as exc:
    assert isinstance(exc, RetryError) == True

@respx.mock
def test_get_metadata_icpsr():
    xml_response = """<?xml version="1.0" encoding="UTF-8"?>
    <OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
      <responseDate>2021-07-02T14:52:01.240-04:00</responseDate>
      <request verb="GetRecord">https://www.icpsr.umich.edu/icpsrweb/neutral/oai/studies</request>
      <GetRecord>
        <record>
          <header>
            <identifier>6425</identifier>
            <datestamp>1995-03-16T01:01:01Z</datestamp>
          </header>
          <metadata>
            <oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/" xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
              <dc:title>103rd Congressional District Geographic Entity File, 1990: [United States]</dc:title>
              <dc:contributor>Inter-university Consortium for Political and Social Research [distributor]</dc:contributor>
              <dc:creator>United States. Bureau of the Census</dc:creator>
              <dc:description>These data describe the geographic relationships of the 103rd congressional districts to selected governmental and statistical geographic entities for the entire United States, American Samoa, Guam, Puerto Rico, and the Virgin Islands. Each record represents a census geographic tabulation unit (GTUB), a unique combination of geographic codes expressing specific geographic relationships. This file provides the following information: state, congressional district, county and county subdivision, place, American Indian/Alaska Native area, urbanized area, urban/rural descriptor, and Metropolitan Statistical Area/Primary Metropolitan Statistical Area (MSA/PMSA).</dc:description>
              <dc:identifier>6425</dc:identifier>
              <dc:identifier>http://doi.org/10.3886/ICPSR06425.v1</dc:identifier>
              <dc:date>03-16-1995</dc:date>
              <dc:type>administrative records data</dc:type>
              <dc:source>equivalency files and listings submitted by the appropriate agency or official within each state, usually from the Secretary of State's Office</dc:source>
              <dc:coverage>1990</dc:coverage>
            </oai_dc:dc>
          </metadata>
        </record>
      </GetRecord>
    </OAI-PMH>"""



    assert get_metadata("icpsr", "http://test/ok", filters=None, config=None) == {}

    assert (
        get_metadata("icpsr", "http://test/ok", filters=None, config={"batchSize": 256})
        == {}
    )

    assert (
        get_metadata("icpsr", mds_url=None, filters={"study_ids": [6425]}, config=None)
        == {}
    )

    respx.get(
        "http://test/ok?verb=GetRecord&metadataPrefix=oai_dc&identifier=6425",
        status_code=200,
        content=xml_response,
    )

    assert get_metadata(
        "icpsr",
        "http://test/ok",
        filters={"study_ids": [6425]},
        mappings=field_mappings,
        keepOriginalFields=True,
    ) == {
        "10.3886/ICPSR06425.v1": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "ipcsr_study_id": "6425",
                "title": "103rd Congressional District Geographic Entity File, 1990: [United States]",
                "contributor": "Inter-university Consortium for Political and Social Research [distributor]",
                "creator": "United States. Bureau of the Census",
                "description": "These data describe the geographic relationships of the 103rd congressional districts to selected governmental and statistical geographic entities for the entire United States, American Samoa, Guam, Puerto Rico, and the Virgin Islands. Each record represents a census geographic tabulation unit (GTUB), a unique combination of geographic codes expressing specific geographic relationships. This file provides the following information: state, congressional district, county and county subdivision, place, American Indian/Alaska Native area, urbanized area, urban/rural descriptor, and Metropolitan Statistical Area/Primary Metropolitan Statistical Area (MSA/PMSA).",
                "identifier": "10.3886/ICPSR06425.v1",
                "date": "03-16-1995",
                "type": "administrative records data",
                "source": "equivalency files and listings submitted by the appropriate agency or official within each state, usually from the Secretary of State's Office",
                "coverage": "1990",
                "tags": [],
                "authz": "",
                "sites": "",
                "summary": "These data describe the geographic relationships of the 103rd congressional districts to selected governmental and statistical geographic entities for the entire United States, American Samoa, Guam, Puerto Rico, and the Virgin Islands. Each record represents a census geographic tabulation unit (GTUB), a unique combination of geographic codes expressing specific geographic relationships. This file provides the following information: state, congressional district, county and county subdivision, place, American Indian/Alaska Native area, urbanized area, urban/rural descriptor, and Metropolitan Statistical Area/Primary Metropolitan Statistical Area (MSA/PMSA).",
                "location": "1",
                "study_url": "https://www.icpsr.umich.edu/web/NAHDAP/studies/6425",
                "subjects": "",
                "__manifest": "",
                "study_name": "",
                "study_type": "",
                "institutions": "Inter-university Consortium for Political and Social Research [distributor]",
                "year_awarded": "",
                "investigators": "United States. Bureau of the Census",
                "project_title": "103rd Congressional District Geographic Entity File, 1990: [United States]",
                "protocol_name": "",
                "study_summary": "",
                "_file_manifest": "",
                "dataset_1_type": "",
                "dataset_2_type": "",
                "dataset_3_type": "",
                "dataset_4_type": "",
                "dataset_5_type": "",
                "project_number": "10.3886/ICPSR06425.v1",
                "dataset_1_title": "",
                "dataset_2_title": "",
                "dataset_3_title": "",
                "dataset_4_title": "",
                "dataset_5_title": "",
                "administering_ic": "",
                "advSearchFilters": [],
                "dataset_category": "administrative records data",
                "research_program": "",
                "research_question": "",
                "study_description": "",
                "clinical_trial_link": "",
                "dataset_description": "",
                "research_focus_area": "",
                "dataset_1_description": "",
                "dataset_2_description": "",
                "dataset_3_description": "",
                "dataset_4_description": "",
                "dataset_5_description": "",
            },
        }
    }

    item_values = {
        "10.3886/ICPSR06425.v1": {
            "__manifest": [
                {
                    "md5sum": "7cf87",
                    "file_name": "TEDS-D-2018-DS0001-bndl-data-spss.zip",
                    "file_size": 69297783,
                    "object_id": "dg.XXXX/208f4c52-771e-409a-b810-4bcba3c03c51",
                }
            ]
        }
    }

    assert get_metadata(
        "icpsr",
        "http://test/ok",
        filters={"study_ids": [6425]},
        mappings=field_mappings,
        perItemValues=item_values,
        keepOriginalFields=True,
    ) == {
        "10.3886/ICPSR06425.v1": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "ipcsr_study_id": "6425",
                "title": "103rd Congressional District Geographic Entity File, 1990: [United States]",
                "contributor": "Inter-university Consortium for Political and Social Research [distributor]",
                "creator": "United States. Bureau of the Census",
                "description": "These data describe the geographic relationships of the 103rd congressional districts to selected governmental and statistical geographic entities for the entire United States, American Samoa, Guam, Puerto Rico, and the Virgin Islands. Each record represents a census geographic tabulation unit (GTUB), a unique combination of geographic codes expressing specific geographic relationships. This file provides the following information: state, congressional district, county and county subdivision, place, American Indian/Alaska Native area, urbanized area, urban/rural descriptor, and Metropolitan Statistical Area/Primary Metropolitan Statistical Area (MSA/PMSA).",
                "identifier": "10.3886/ICPSR06425.v1",
                "date": "03-16-1995",
                "type": "administrative records data",
                "source": "equivalency files and listings submitted by the appropriate agency or official within each state, usually from the Secretary of State's Office",
                "coverage": "1990",
                "tags": [],
                "authz": "",
                "sites": "",
                "summary": "These data describe the geographic relationships of the 103rd congressional districts to selected governmental and statistical geographic entities for the entire United States, American Samoa, Guam, Puerto Rico, and the Virgin Islands. Each record represents a census geographic tabulation unit (GTUB), a unique combination of geographic codes expressing specific geographic relationships. This file provides the following information: state, congressional district, county and county subdivision, place, American Indian/Alaska Native area, urbanized area, urban/rural descriptor, and Metropolitan Statistical Area/Primary Metropolitan Statistical Area (MSA/PMSA).",
                "location": "1",
                "study_url": "https://www.icpsr.umich.edu/web/NAHDAP/studies/6425",
                "subjects": "",
                "__manifest": [
                    {
                        "md5sum": "7cf87",
                        "file_name": "TEDS-D-2018-DS0001-bndl-data-spss.zip",
                        "file_size": 69297783,
                        "object_id": "dg.XXXX/208f4c52-771e-409a-b810-4bcba3c03c51",
                    }
                ],
                "study_name": "",
                "study_type": "",
                "institutions": "Inter-university Consortium for Political and Social Research [distributor]",
                "year_awarded": "",
                "investigators": "United States. Bureau of the Census",
                "project_title": "103rd Congressional District Geographic Entity File, 1990: [United States]",
                "protocol_name": "",
                "study_summary": "",
                "_file_manifest": "",
                "dataset_1_type": "",
                "dataset_2_type": "",
                "dataset_3_type": "",
                "dataset_4_type": "",
                "dataset_5_type": "",
                "project_number": "10.3886/ICPSR06425.v1",
                "dataset_1_title": "",
                "dataset_2_title": "",
                "dataset_3_title": "",
                "dataset_4_title": "",
                "dataset_5_title": "",
                "administering_ic": "",
                "advSearchFilters": [],
                "dataset_category": "administrative records data",
                "research_program": "",
                "research_question": "",
                "study_description": "",
                "clinical_trial_link": "",
                "dataset_description": "",
                "research_focus_area": "",
                "dataset_1_description": "",
                "dataset_2_description": "",
                "dataset_3_description": "",
                "dataset_4_description": "",
                "dataset_5_description": "",
            },
        }
    }

    # test bad XML response
    respx.get(
        "http://test/ok?verb=GetRecord&metadataPrefix=oai_dc&identifier=64257",
        status_code=200,
        content="<dsfsdfsd>",
    )

    assert (
        get_metadata(
            "icpsr",
            "http://test/ok",
            filters={"study_ids": [64257]},
            mappings=field_mappings,
            keepOriginalFields=True,
        )
        == {}
    )

    respx.get(
        "http://test/ok?verb=GetRecord&metadataPrefix=oai_dc&identifier=64",
        status_code=404,
        content={},
    )

    assert (
        get_metadata(
            "icpsr",
            "http://test/ok",
            filters={"study_ids": [64]},
            mappings=field_mappings,
            keepOriginalFields=True,
        )
        == {}
    )

    try:
        from mds.agg_mds.adapters import ISCPSRDublin

        ISCPSRDublin.getRemoteDataAsJson.retry.wait = wait_none()

        respx.get(
            "http://test/timeouterror?verb=GetRecord&metadataPrefix=oai_dc&identifier=64",
            content=httpx.TimeoutException,
        )
        get_metadata(
            "icpsr",
            "http://test/timeouterror/",
            filters={"study_ids": [64]},
            mappings=field_mappings,
            keepOriginalFields=True,
        )
    except Exception as exc:
        assert isinstance(exc, RetryError) == True

