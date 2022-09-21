from more_itertools import side_effect
import respx
import json
from mds.agg_mds.adapters import get_metadata, get_json_path_value
from tenacity import RetryError, wait_none
import httpx


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

    field_mappings = {
        "tags": [],
        "authz": "",
        "sites": "",
        "study_description_summary": {"path": "description", "filters": ["strip_html"]},
        "study_url": {"path": "ipcsr_study_id", "filters": ["add_icpsr_source_url"]},
        "location": "path:coverage[0]",
        "subjects": "",
        "__manifest": "",
        "study_name_title": "",
        "study_type": "",
        "institutions": "path:contributor",
        "year_awarded": "",
        "investigators_name": "path:creator",
        "project_title": "path:title",
        "protocol_name": "",
        "study_summary": "",
        "_file_manifest": "",
        "dataset_1_type": "",
        "dataset_2_type": "",
        "dataset_3_type": "",
        "dataset_4_type": "",
        "dataset_5_type": "",
        "project_number": "path:identifier",
        "dataset_1_title": "",
        "dataset_2_title": "",
        "dataset_3_title": "",
        "dataset_4_title": "",
        "dataset_5_title": "",
        "administering_ic": "",
        "advSearchFilters": [],
        "dataset_category": "path:type",
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
    }

    respx.get(
        "http://test/ok?verb=GetRecord&metadataPrefix=oai_dc&identifier=6425"
    ).mock(
        return_value=httpx.Response(
            status_code=200,
            json=xml_response,
        )
    )

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
        "http://test/ok?verb=GetRecord&metadataPrefix=oai_dc&identifier=6425"
    ).mock(
        return_value=httpx.Response(
            status_code=200,
            content=xml_response,
        )
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
                "study_description_summary": "These data describe the geographic relationships of the 103rd congressional districts to selected governmental and statistical geographic entities for the entire United States, American Samoa, Guam, Puerto Rico, and the Virgin Islands. Each record represents a census geographic tabulation unit (GTUB), a unique combination of geographic codes expressing specific geographic relationships. This file provides the following information: state, congressional district, county and county subdivision, place, American Indian/Alaska Native area, urbanized area, urban/rural descriptor, and Metropolitan Statistical Area/Primary Metropolitan Statistical Area (MSA/PMSA).",
                "location": "1",
                "study_url": "https://www.icpsr.umich.edu/web/NAHDAP/studies/6425",
                "subjects": "",
                "__manifest": "",
                "study_name_title": "",
                "study_type": "",
                "institutions": "Inter-university Consortium for Political and Social Research [distributor]",
                "year_awarded": "",
                "investigators_name": "United States. Bureau of the Census",
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
                "study_description_summary": "These data describe the geographic relationships of the 103rd congressional districts to selected governmental and statistical geographic entities for the entire United States, American Samoa, Guam, Puerto Rico, and the Virgin Islands. Each record represents a census geographic tabulation unit (GTUB), a unique combination of geographic codes expressing specific geographic relationships. This file provides the following information: state, congressional district, county and county subdivision, place, American Indian/Alaska Native area, urbanized area, urban/rural descriptor, and Metropolitan Statistical Area/Primary Metropolitan Statistical Area (MSA/PMSA).",
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
                "study_name_title": "",
                "study_type": "",
                "institutions": "Inter-university Consortium for Political and Social Research [distributor]",
                "year_awarded": "",
                "investigators_name": "United States. Bureau of the Census",
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
        "http://test/ok?verb=GetRecord&metadataPrefix=oai_dc&identifier=64257"
    ).mock(
        return_value=httpx.Response(
            status_code=200,
            content="<dsfsdfsd>",
        )
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

    respx.get("http://test/ok?verb=GetRecord&metadataPrefix=oai_dc&identifier=64").mock(
        return_value=httpx.Response(
            status_code=404,
            content={},
        )
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
            "http://test/timeouterror?verb=GetRecord&metadataPrefix=oai_dc&identifier=64"
        ).mock(side_effect=httpx.TimeoutException)

        get_metadata(
            "icpsr",
            "http://test/timeouterror/",
            filters={"study_ids": [64]},
            mappings=field_mappings,
            keepOriginalFields=True,
        )
    except Exception as exc:
        assert isinstance(exc, RetryError) == True


@respx.mock
def test_get_metadata_clinicaltrials():
    json_response = r"""{
  "FullStudiesResponse":{
    "APIVrs":"1.01.03",
    "DataVrs":"2021:07:06 22:12:28.413",
    "Expression":"heart attack",
    "NStudiesAvail":382563,
    "NStudiesFound":8322,
    "MinRank":1,
    "MaxRank":1,
    "NStudiesReturned":1,
    "FullStudies":[
      {
        "Rank":1,
        "Study":{
          "ProtocolSection":{
            "IdentificationModule":{
              "NCTId":"NCT01874691",
              "OrgStudyIdInfo":{
                "OrgStudyId":"2011BAI11B02-A"
              },
              "SecondaryIdInfoList":{
                "SecondaryIdInfo":[
                  {
                    "SecondaryId":"2011BAI11B02",
                    "SecondaryIdType":"Other Grant/Funding Number",
                    "SecondaryIdDomain":"Chinese Ministry of Science and Technology"
                  }
                ]
              },
              "Organization":{
                "OrgFullName":"Chinese Academy of Medical Sciences, Fuwai Hospital",
                "OrgClass":"OTHER"
              },
              "BriefTitle":"China Acute Myocardial Infarction Registry",
              "OfficialTitle":"China Acute Myocardial Infarction Registry",
              "Acronym":"CAMIRegistry"
            },
            "StatusModule":{
              "StatusVerifiedDate":"January 2018",
              "OverallStatus":"Completed",
              "ExpandedAccessInfo":{
                "HasExpandedAccess":"No"
              },
              "StartDateStruct":{
                "StartDate":"January 2013",
                "StartDateType":"Actual"
              },
              "PrimaryCompletionDateStruct":{
                "PrimaryCompletionDate":"December 2016",
                "PrimaryCompletionDateType":"Actual"
              },
              "CompletionDateStruct":{
                "CompletionDate":"December 2016",
                "CompletionDateType":"Actual"
              },
              "StudyFirstSubmitDate":"June 4, 2013",
              "StudyFirstSubmitQCDate":"June 7, 2013",
              "StudyFirstPostDateStruct":{
                "StudyFirstPostDate":"June 11, 2013",
                "StudyFirstPostDateType":"Estimate"
              },
              "LastUpdateSubmitDate":"January 23, 2018",
              "LastUpdatePostDateStruct":{
                "LastUpdatePostDate":"January 25, 2018",
                "LastUpdatePostDateType":"Actual"
              }
            },
            "SponsorCollaboratorsModule":{
              "ResponsibleParty":{
                "ResponsiblePartyType":"Principal Investigator",
                "ResponsiblePartyInvestigatorFullName":"Yuejin Yang",
                "ResponsiblePartyInvestigatorTitle":"Doctor",
                "ResponsiblePartyInvestigatorAffiliation":"Chinese Academy of Medical Sciences, Fuwai Hospital"
              },
              "LeadSponsor":{
                "LeadSponsorName":"Chinese Academy of Medical Sciences, Fuwai Hospital",
                "LeadSponsorClass":"OTHER"
              }
            },
            "OversightModule":{
              "OversightHasDMC":"Yes"
            },
            "DescriptionModule":{
              "BriefSummary":"This study is to build a Chinese national registry and surveillance system for acute myocardial infarction(AMI) to obtain real-world information about current status of characteristics, risk factors, diagnosis, treatment and outcomes of Chinese AMI patients; And to propose scientific precaution strategies aimed to prevent effectively from the incidence of AMI; And to optimize the management and outcomes of AMI patients through implementation of guideline recommendations in clinical practice, and analysis and development of effective treatment strategies; And to create cost-effective assessment system.",
              "DetailedDescription":"The aim of the study is to establish the national platform for surveillance, clinical research and translational medicine in China, designed to facilitate efforts to improve the quality of AMI patient care and thus decrease morbidity and mortality associated with AMI."
            },
            "ConditionsModule":{
              "ConditionList":{
                "Condition":[
                  "Acute Myocardial Infarction"
                ]
              },
              "KeywordList":{
                "Keyword":[
                  "acute myocardial infarction",
                  "China",
                  "Registry"
                ]
              }
            },
            "DesignModule":{
              "StudyType":"Observational",
              "PatientRegistry":"Yes",
              "TargetDuration":"2 Years",
              "DesignInfo":{
                "DesignObservationalModelList":{
                  "DesignObservationalModel":[
                    "Case-Only"
                  ]
                },
                "DesignTimePerspectiveList":{
                  "DesignTimePerspective":[
                    "Prospective"
                  ]
                }
              },
              "EnrollmentInfo":{
                "EnrollmentCount":"20000",
                "EnrollmentType":"Actual"
              }
            },
            "ArmsInterventionsModule":{
              "ArmGroupList":{
                "ArmGroup":[
                  {
                    "ArmGroupLabel":"acute myocardial infarction",
                    "ArmGroupDescription":"acute myocardial infarction including ST-elevation and non ST-elevation myocardial infarction"
                  }
                ]
              }
            },
            "OutcomesModule":{
              "PrimaryOutcomeList":{
                "PrimaryOutcome":[
                  {
                    "PrimaryOutcomeMeasure":"In-hospital mortality of the patients with acute myocardial infarction in different-level hospitals across China",
                    "PrimaryOutcomeDescription":"Different-level hospitals include Provincial-level, city-level, County-level hospitals from all over China.",
                    "PrimaryOutcomeTimeFrame":"the duration of hospital stay, an resources average of 2 weeks"
                  }
                ]
              },
              "SecondaryOutcomeList":{
                "SecondaryOutcome":[
                  {
                    "SecondaryOutcomeMeasure":"The rate of the application of thrombolysis and primary percutaneous coronary intervention for Chinese patients with acute myocardial infarction in different-level hospitals",
                    "SecondaryOutcomeDescription":"In different-level hospitals, How many patients with acute myocardial infarction receive thrombolysis and/or primary percutaneous coronary intervention within 24 hours from the onset,respectively?",
                    "SecondaryOutcomeTimeFrame":"24 hours"
                  },{
                    "SecondaryOutcomeMeasure":"provoking factors of Chinese patients with AMI across different areas and different population in China",
                    "SecondaryOutcomeDescription":"The different factors that can provoke the onset of acute myocardial infarction, for example, excess exercise, overload work, heavy smoking, heavy drinking of alcohol and so on.",
                    "SecondaryOutcomeTimeFrame":"24 hours"
                  }
                ]
              },
              "OtherOutcomeList":{
                "OtherOutcome":[
                  {
                    "OtherOutcomeMeasure":"the in-hospital cost of Chinese patients with acute myocardial infarction",
                    "OtherOutcomeTimeFrame":"the duration of hospital stay, an resources average of 2 weeks"
                  }
                ]
              }
            },
            "EligibilityModule":{
              "EligibilityCriteria":"Inclusion Criteria:\n\nEligible patients must be admitted within 7 days of acute ischemic symptoms and diagnosed acute ST-elevation or non ST-elevation myocardial infarction. Diagnosis criteria must meet Universal Definition for AMI (2012). All participating hospitals are required to enroll consecutive patients with AMI.\n\nExclusion Criteria:\n\nMyocardial infarction related to percutaneous coronary intervention and coronary artery bypass grafting.",
              "HealthyVolunteers":"No",
              "Gender":"All",
              "StdAgeList":{
                "StdAge":[
                  "Child",
                  "Adult",
                  "Older Adult"
                ]
              },
              "StudyPopulation":"Eligible patients admitted within 7 days of acute ischemic symptoms and diagnosed acute ST-elevation or non ST-elevation myocardial infarction.",
              "SamplingMethod":"Probability Sample"
            },
            "ContactsLocationsModule":{
              "OverallOfficialList":{
                "OverallOfficial":[
                  {
                    "OverallOfficialName":"Yuejin Yang, MD.",
                    "OverallOfficialAffiliation":"Fuwai Hospital, Chinse Academy of Medical Sciences",
                    "OverallOfficialRole":"Study Chair"
                  }
                ]
              },
              "LocationList":{
                "Location":[
                  {
                    "LocationFacility":"Fuwai Hospital",
                    "LocationCity":"Beijing",
                    "LocationState":"Beijing",
                    "LocationZip":"100037",
                    "LocationCountry":"China"
                  }
                ]
              }
            },
            "ReferencesModule":{
              "ReferenceList":{
                "Reference":[
                  {
                    "ReferencePMID":"20031882",
                    "ReferenceType":"background",
                    "ReferenceCitation":"Peterson ED, Roe MT, Rumsfeld JS, Shaw RE, Brindis RG, Fonarow GC, Cannon CP. A call to ACTION (acute coronary treatment and intervention outcomes network): a national effort to promote timely clinical feedback and support continuous quality improvement for acute myocardial infarction. Circ Cardiovasc Qual Outcomes. 2009 Sep;2(5):491-9. doi: 10.1161/CIRCOUTCOMES.108.847145."
                  },{
                    "ReferencePMID":"31567475",
                    "ReferenceType":"derived",
                    "ReferenceCitation":"Song CX, Fu R, Yang JG, Xu HY, Gao XJ, Wang CY, Zheng Y, Jia SB, Dou KF, Yang YJ; CAMI Registry study group. Angiographic characteristics and in-hospital mortality among patients with ST-segment elevation myocardial infarction presenting without typical chest pain: an analysis of China Acute Myocardial Infarction registry. Chin Med J (Engl). 2019 Oct 5;132(19):2286-2291. doi: 10.1097/CM9.0000000000000432."
                  },{
                    "ReferencePMID":"31515430",
                    "ReferenceType":"derived",
                    "ReferenceCitation":"Song C, Fu R, Li S, Yang J, Wang Y, Xu H, Gao X, Liu J, Liu Q, Wang C, Dou K, Yang Y. Simple risk score based on the China Acute Myocardial Infarction registry for predicting in-hospital mortality among patients with non-ST-segment elevation myocardial infarction: results of a prospective observational cohort study. BMJ Open. 2019 Sep 12;9(9):e030772. doi: 10.1136/bmjopen-2019-030772."
                  },{
                    "ReferencePMID":"31471442",
                    "ReferenceType":"derived",
                    "ReferenceCitation":"Song C, Fu R, Dou K, Yang J, Xu H, Gao X, Wang H, Liu S, Fan X, Yang Y. Association between smoking and in-hospital mortality in patients with acute myocardial infarction: results from a prospective, multicentre, observational study in China. BMJ Open. 2019 Aug 30;9(8):e030252. doi: 10.1136/bmjopen-2019-030252."
                  },{
                    "ReferencePMID":"31255895",
                    "ReferenceType":"derived",
                    "ReferenceCitation":"Leng W, Yang J, Fan X, Sun Y, Xu H, Gao X, Wang Y, Li W, Xu Y, Han Y, Jia S, Zheng Y, Yang Y; behalf CAMI Registry investigators. Contemporary invasive management and in-hospital outcomes of patients with non-ST-segment elevation myocardial infarction in China: Findings from China Acute Myocardial Infarction (CAMI) Registry. Am Heart J. 2019 Sep;215:1-11. doi: 10.1016/j.ahj.2019.05.015. Epub 2019 Jun 6."
                  },{
                    "ReferencePMID":"30807351",
                    "ReferenceType":"derived",
                    "ReferenceCitation":"Fu R, Song CX, Dou KF, Yang JG, Xu HY, Gao XJ, Liu QQ, Xu H, Yang YJ. Differences in symptoms and pre-hospital delay among acute myocardial infarction patients according to ST-segment elevation on electrocardiogram: an analysis of China Acute Myocardial Infarction (CAMI) registry. Chin Med J (Engl). 2019 Mar 5;132(5):519-524. doi: 10.1097/CM9.0000000000000122."
                  },{
                    "ReferencePMID":"28052755",
                    "ReferenceType":"derived",
                    "ReferenceCitation":"Dai Y, Yang J, Gao Z, Xu H, Sun Y, Wu Y, Gao X, Li W, Wang Y, Gao R, Yang Y; CAMI Registry study group. Atrial fibrillation in patients hospitalized with acute myocardial infarction: analysis of the china acute myocardial infarction (CAMI) registry. BMC Cardiovasc Disord. 2017 Jan 4;17(1):2. doi: 10.1186/s12872-016-0442-9."
                  },{
                    "ReferencePMID":"27530939",
                    "ReferenceType":"derived",
                    "ReferenceCitation":"Sun H, Yang YJ, Xu HY, Yang JG, Gao XJ, Wu Y, Li W, Wang Y, Liu J, Jin C, Song L; CAMI Registry Study Group. [Survey of medical care resources of acute myocardial infarction in different regions and levels of hospitals in China]. Zhonghua Xin Xue Guan Bing Za Zhi. 2016 Jul 24;44(7):565-9. doi: 10.3760/cma.j.issn.0253-3758.2016.07.003. Chinese."
                  }
                ]
              }
            }
          },
          "DerivedSection":{
            "MiscInfoModule":{
              "VersionHolder":"July 07, 2021"
            },
            "ConditionBrowseModule":{
              "ConditionMeshList":{
                "ConditionMesh":[
                  {
                    "ConditionMeshId":"D000009203",
                    "ConditionMeshTerm":"Myocardial Infarction"
                  },{
                    "ConditionMeshId":"D000007238",
                    "ConditionMeshTerm":"Infarction"
                  }
                ]
              },
              "ConditionAncestorList":{
                "ConditionAncestor":[
                  {
                    "ConditionAncestorId":"D000007511",
                    "ConditionAncestorTerm":"Ischemia"
                  },{
                    "ConditionAncestorId":"D000010335",
                    "ConditionAncestorTerm":"Pathologic Processes"
                  },{
                    "ConditionAncestorId":"D000009336",
                    "ConditionAncestorTerm":"Necrosis"
                  },{
                    "ConditionAncestorId":"D000017202",
                    "ConditionAncestorTerm":"Myocardial Ischemia"
                  },{
                    "ConditionAncestorId":"D000006331",
                    "ConditionAncestorTerm":"Heart Diseases"
                  },{
                    "ConditionAncestorId":"D000002318",
                    "ConditionAncestorTerm":"Cardiovascular Diseases"
                  },{
                    "ConditionAncestorId":"D000014652",
                    "ConditionAncestorTerm":"Vascular Diseases"
                  }
                ]
              },
              "ConditionBrowseLeafList":{
                "ConditionBrowseLeaf":[
                  {
                    "ConditionBrowseLeafId":"M10738",
                    "ConditionBrowseLeafName":"Myocardial Infarction",
                    "ConditionBrowseLeafAsFound":"Myocardial Infarction",
                    "ConditionBrowseLeafRelevance":"high"
                  },{
                    "ConditionBrowseLeafId":"M8865",
                    "ConditionBrowseLeafName":"Infarction",
                    "ConditionBrowseLeafAsFound":"Infarction",
                    "ConditionBrowseLeafRelevance":"high"
                  },{
                    "ConditionBrowseLeafId":"M9126",
                    "ConditionBrowseLeafName":"Ischemia",
                    "ConditionBrowseLeafRelevance":"low"
                  },{
                    "ConditionBrowseLeafId":"M10867",
                    "ConditionBrowseLeafName":"Necrosis",
                    "ConditionBrowseLeafRelevance":"low"
                  },{
                    "ConditionBrowseLeafId":"M5129",
                    "ConditionBrowseLeafName":"Coronary Artery Disease",
                    "ConditionBrowseLeafRelevance":"low"
                  },{
                    "ConditionBrowseLeafId":"M18089",
                    "ConditionBrowseLeafName":"Myocardial Ischemia",
                    "ConditionBrowseLeafRelevance":"low"
                  },{
                    "ConditionBrowseLeafId":"M8002",
                    "ConditionBrowseLeafName":"Heart Diseases",
                    "ConditionBrowseLeafRelevance":"low"
                  },{
                    "ConditionBrowseLeafId":"M15983",
                    "ConditionBrowseLeafName":"Vascular Diseases",
                    "ConditionBrowseLeafRelevance":"low"
                  }
                ]
              },
              "ConditionBrowseBranchList":{
                "ConditionBrowseBranch":[
                  {
                    "ConditionBrowseBranchAbbrev":"BC14",
                    "ConditionBrowseBranchName":"Heart and Blood Diseases"
                  },{
                    "ConditionBrowseBranchAbbrev":"All",
                    "ConditionBrowseBranchName":"All Conditions"
                  },{
                    "ConditionBrowseBranchAbbrev":"BC23",
                    "ConditionBrowseBranchName":"Symptoms and General Pathology"
                  }
                ]
              }
            }
          }
        }
      }
    ]
  }
}"""
    field_mappings = {
        "tags": [],
        "authz": "",
        "sites": "",
        "study_description_summary": "path:BriefSummary",
        "location": "",
        "subjects": "path:EnrollmentCount",
        "__manifest": "",
        "study_name_title": "",
        "study_type": "",
        "study_url": {"path": "NCTId", "filters": ["add_clinical_trials_source_url"]},
        "institutions": "path:LeadSponsorName",
        "year_awarded": "",
        "investigators_name": "path:OverallOfficial[0].OverallOfficialName",
        "protocol_name": "",
        "study_summary": "",
        "_file_manifest": "",
        "dataset_1_type": "",
        "dataset_2_type": "",
        "dataset_3_type": "",
        "dataset_4_type": "",
        "dataset_5_type": "",
        "project_number": "path:OrgStudyId",
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
    }

    item_values = {"NCT01874691": {"__manifest": {"filename": "foo.zip "}}}

    respx.get("http://test/ok?expr=heart+attack&fmt=json&min_rnk=1&max_rnk=1").mock(
        return_value=httpx.Response(status_code=200, content=json_response)
    )

    assert get_metadata("clinicaltrials", "http://test/ok", filters=None) == {}

    assert get_metadata(
        "clinicaltrials",
        "http://test/ok",
        filters={"term": "heart attack", "maxItems": 1},
        mappings=field_mappings,
        perItemValues=item_values,
        keepOriginalFields=True,
    ) == {
        "NCT01874691": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "NCTId": "NCT01874691",
                "OrgStudyId": "2011BAI11B02-A",
                "SecondaryIdInfo": [
                    {
                        "SecondaryId": "2011BAI11B02",
                        "SecondaryIdType": "Other Grant/Funding Number",
                        "SecondaryIdDomain": "Chinese Ministry of Science and Technology",
                    }
                ],
                "OrgFullName": "Chinese Academy of Medical Sciences, Fuwai Hospital",
                "OrgClass": "OTHER",
                "BriefTitle": "China Acute Myocardial Infarction Registry",
                "OfficialTitle": "China Acute Myocardial Infarction Registry",
                "Acronym": "CAMIRegistry",
                "StatusVerifiedDate": "January 2018",
                "OverallStatus": "Completed",
                "HasExpandedAccess": "No",
                "StartDate": "January 2013",
                "StartDateType": "Actual",
                "PrimaryCompletionDate": "December 2016",
                "PrimaryCompletionDateType": "Actual",
                "CompletionDate": "December 2016",
                "CompletionDateType": "Actual",
                "StudyFirstSubmitDate": "June 4, 2013",
                "StudyFirstSubmitQCDate": "June 7, 2013",
                "StudyFirstPostDate": "June 11, 2013",
                "StudyFirstPostDateType": "Estimate",
                "LastUpdateSubmitDate": "January 23, 2018",
                "LastUpdatePostDate": "January 25, 2018",
                "LastUpdatePostDateType": "Actual",
                "ResponsiblePartyType": "Principal Investigator",
                "ResponsiblePartyInvestigatorFullName": "Yuejin Yang",
                "ResponsiblePartyInvestigatorTitle": "Doctor",
                "ResponsiblePartyInvestigatorAffiliation": "Chinese Academy of Medical Sciences, Fuwai Hospital",
                "LeadSponsorName": "Chinese Academy of Medical Sciences, Fuwai Hospital",
                "LeadSponsorClass": "OTHER",
                "OversightHasDMC": "Yes",
                "BriefSummary": "This study is to build a Chinese national registry and surveillance system for acute myocardial infarction(AMI) to obtain real-world information about current status of characteristics, risk factors, diagnosis, treatment and outcomes of Chinese AMI patients; And to propose scientific precaution strategies aimed to prevent effectively from the incidence of AMI; And to optimize the management and outcomes of AMI patients through implementation of guideline recommendations in clinical practice, and analysis and development of effective treatment strategies; And to create cost-effective assessment system.",
                "DetailedDescription": "The aim of the study is to establish the national platform for surveillance, clinical research and translational medicine in China, designed to facilitate efforts to improve the quality of AMI patient care and thus decrease morbidity and mortality associated with AMI.",
                "Condition": ["Acute Myocardial Infarction"],
                "Keyword": ["acute myocardial infarction", "China", "Registry"],
                "StudyType": "Observational",
                "PatientRegistry": "Yes",
                "TargetDuration": "2 Years",
                "DesignObservationalModel": ["Case-Only"],
                "DesignTimePerspective": ["Prospective"],
                "EnrollmentCount": "20000",
                "EnrollmentType": "Actual",
                "ArmGroup": [
                    {
                        "ArmGroupLabel": "acute myocardial infarction",
                        "ArmGroupDescription": "acute myocardial infarction including ST-elevation and non ST-elevation myocardial infarction",
                    }
                ],
                "PrimaryOutcome": [
                    {
                        "PrimaryOutcomeMeasure": "In-hospital mortality of the patients with acute myocardial infarction in different-level hospitals across China",
                        "PrimaryOutcomeDescription": "Different-level hospitals include Provincial-level, city-level, County-level hospitals from all over China.",
                        "PrimaryOutcomeTimeFrame": "the duration of hospital stay, an resources average of 2 weeks",
                    }
                ],
                "SecondaryOutcome": [
                    {
                        "SecondaryOutcomeMeasure": "The rate of the application of thrombolysis and primary percutaneous coronary intervention for Chinese patients with acute myocardial infarction in different-level hospitals",
                        "SecondaryOutcomeDescription": "In different-level hospitals, How many patients with acute myocardial infarction receive thrombolysis and/or primary percutaneous coronary intervention within 24 hours from the onset,respectively?",
                        "SecondaryOutcomeTimeFrame": "24 hours",
                    },
                    {
                        "SecondaryOutcomeMeasure": "provoking factors of Chinese patients with AMI across different areas and different population in China",
                        "SecondaryOutcomeDescription": "The different factors that can provoke the onset of acute myocardial infarction, for example, excess exercise, overload work, heavy smoking, heavy drinking of alcohol and so on.",
                        "SecondaryOutcomeTimeFrame": "24 hours",
                    },
                ],
                "OtherOutcome": [
                    {
                        "OtherOutcomeMeasure": "the in-hospital cost of Chinese patients with acute myocardial infarction",
                        "OtherOutcomeTimeFrame": "the duration of hospital stay, an resources average of 2 weeks",
                    }
                ],
                "EligibilityCriteria": "Inclusion Criteria:\n\nEligible patients must be admitted within 7 days of acute ischemic symptoms and diagnosed acute ST-elevation or non ST-elevation myocardial infarction. Diagnosis criteria must meet Universal Definition for AMI (2012). All participating hospitals are required to enroll consecutive patients with AMI.\n\nExclusion Criteria:\n\nMyocardial infarction related to percutaneous coronary intervention and coronary artery bypass grafting.",
                "HealthyVolunteers": "No",
                "Gender": "All",
                "StdAge": ["Child", "Adult", "Older Adult"],
                "StudyPopulation": "Eligible patients admitted within 7 days of acute ischemic symptoms and diagnosed acute ST-elevation or non ST-elevation myocardial infarction.",
                "SamplingMethod": "Probability Sample",
                "OverallOfficial": [
                    {
                        "OverallOfficialName": "Yuejin Yang, MD.",
                        "OverallOfficialAffiliation": "Fuwai Hospital, Chinse Academy of Medical Sciences",
                        "OverallOfficialRole": "Study Chair",
                    }
                ],
                "Location": [
                    {
                        "LocationFacility": "Fuwai Hospital",
                        "LocationCity": "Beijing",
                        "LocationState": "Beijing",
                        "LocationZip": "100037",
                        "LocationCountry": "China",
                    }
                ],
                "Reference": [
                    {
                        "ReferencePMID": "20031882",
                        "ReferenceType": "background",
                        "ReferenceCitation": "Peterson ED, Roe MT, Rumsfeld JS, Shaw RE, Brindis RG, Fonarow GC, Cannon CP. A call to ACTION (acute coronary treatment and intervention outcomes network): a national effort to promote timely clinical feedback and support continuous quality improvement for acute myocardial infarction. Circ Cardiovasc Qual Outcomes. 2009 Sep;2(5):491-9. doi: 10.1161/CIRCOUTCOMES.108.847145.",
                    },
                    {
                        "ReferencePMID": "31567475",
                        "ReferenceType": "derived",
                        "ReferenceCitation": "Song CX, Fu R, Yang JG, Xu HY, Gao XJ, Wang CY, Zheng Y, Jia SB, Dou KF, Yang YJ; CAMI Registry study group. Angiographic characteristics and in-hospital mortality among patients with ST-segment elevation myocardial infarction presenting without typical chest pain: an analysis of China Acute Myocardial Infarction registry. Chin Med J (Engl). 2019 Oct 5;132(19):2286-2291. doi: 10.1097/CM9.0000000000000432.",
                    },
                    {
                        "ReferencePMID": "31515430",
                        "ReferenceType": "derived",
                        "ReferenceCitation": "Song C, Fu R, Li S, Yang J, Wang Y, Xu H, Gao X, Liu J, Liu Q, Wang C, Dou K, Yang Y. Simple risk score based on the China Acute Myocardial Infarction registry for predicting in-hospital mortality among patients with non-ST-segment elevation myocardial infarction: results of a prospective observational cohort study. BMJ Open. 2019 Sep 12;9(9):e030772. doi: 10.1136/bmjopen-2019-030772.",
                    },
                    {
                        "ReferencePMID": "31471442",
                        "ReferenceType": "derived",
                        "ReferenceCitation": "Song C, Fu R, Dou K, Yang J, Xu H, Gao X, Wang H, Liu S, Fan X, Yang Y. Association between smoking and in-hospital mortality in patients with acute myocardial infarction: results from a prospective, multicentre, observational study in China. BMJ Open. 2019 Aug 30;9(8):e030252. doi: 10.1136/bmjopen-2019-030252.",
                    },
                    {
                        "ReferencePMID": "31255895",
                        "ReferenceType": "derived",
                        "ReferenceCitation": "Leng W, Yang J, Fan X, Sun Y, Xu H, Gao X, Wang Y, Li W, Xu Y, Han Y, Jia S, Zheng Y, Yang Y; behalf CAMI Registry investigators. Contemporary invasive management and in-hospital outcomes of patients with non-ST-segment elevation myocardial infarction in China: Findings from China Acute Myocardial Infarction (CAMI) Registry. Am Heart J. 2019 Sep;215:1-11. doi: 10.1016/j.ahj.2019.05.015. Epub 2019 Jun 6.",
                    },
                    {
                        "ReferencePMID": "30807351",
                        "ReferenceType": "derived",
                        "ReferenceCitation": "Fu R, Song CX, Dou KF, Yang JG, Xu HY, Gao XJ, Liu QQ, Xu H, Yang YJ. Differences in symptoms and pre-hospital delay among acute myocardial infarction patients according to ST-segment elevation on electrocardiogram: an analysis of China Acute Myocardial Infarction (CAMI) registry. Chin Med J (Engl). 2019 Mar 5;132(5):519-524. doi: 10.1097/CM9.0000000000000122.",
                    },
                    {
                        "ReferencePMID": "28052755",
                        "ReferenceType": "derived",
                        "ReferenceCitation": "Dai Y, Yang J, Gao Z, Xu H, Sun Y, Wu Y, Gao X, Li W, Wang Y, Gao R, Yang Y; CAMI Registry study group. Atrial fibrillation in patients hospitalized with acute myocardial infarction: analysis of the china acute myocardial infarction (CAMI) registry. BMC Cardiovasc Disord. 2017 Jan 4;17(1):2. doi: 10.1186/s12872-016-0442-9.",
                    },
                    {
                        "ReferencePMID": "27530939",
                        "ReferenceType": "derived",
                        "ReferenceCitation": "Sun H, Yang YJ, Xu HY, Yang JG, Gao XJ, Wu Y, Li W, Wang Y, Liu J, Jin C, Song L; CAMI Registry Study Group. [Survey of medical care resources of acute myocardial infarction in different regions and levels of hospitals in China]. Zhonghua Xin Xue Guan Bing Za Zhi. 2016 Jul 24;44(7):565-9. doi: 10.3760/cma.j.issn.0253-3758.2016.07.003. Chinese.",
                    },
                ],
                "VersionHolder": "July 07, 2021",
                "ConditionMesh": [
                    {
                        "ConditionMeshId": "D000009203",
                        "ConditionMeshTerm": "Myocardial Infarction",
                    },
                    {
                        "ConditionMeshId": "D000007238",
                        "ConditionMeshTerm": "Infarction",
                    },
                ],
                "ConditionAncestor": [
                    {
                        "ConditionAncestorId": "D000007511",
                        "ConditionAncestorTerm": "Ischemia",
                    },
                    {
                        "ConditionAncestorId": "D000010335",
                        "ConditionAncestorTerm": "Pathologic Processes",
                    },
                    {
                        "ConditionAncestorId": "D000009336",
                        "ConditionAncestorTerm": "Necrosis",
                    },
                    {
                        "ConditionAncestorId": "D000017202",
                        "ConditionAncestorTerm": "Myocardial Ischemia",
                    },
                    {
                        "ConditionAncestorId": "D000006331",
                        "ConditionAncestorTerm": "Heart Diseases",
                    },
                    {
                        "ConditionAncestorId": "D000002318",
                        "ConditionAncestorTerm": "Cardiovascular Diseases",
                    },
                    {
                        "ConditionAncestorId": "D000014652",
                        "ConditionAncestorTerm": "Vascular Diseases",
                    },
                ],
                "ConditionBrowseLeaf": [
                    {
                        "ConditionBrowseLeafId": "M10738",
                        "ConditionBrowseLeafName": "Myocardial Infarction",
                        "ConditionBrowseLeafAsFound": "Myocardial Infarction",
                        "ConditionBrowseLeafRelevance": "high",
                    },
                    {
                        "ConditionBrowseLeafId": "M8865",
                        "ConditionBrowseLeafName": "Infarction",
                        "ConditionBrowseLeafAsFound": "Infarction",
                        "ConditionBrowseLeafRelevance": "high",
                    },
                    {
                        "ConditionBrowseLeafId": "M9126",
                        "ConditionBrowseLeafName": "Ischemia",
                        "ConditionBrowseLeafRelevance": "low",
                    },
                    {
                        "ConditionBrowseLeafId": "M10867",
                        "ConditionBrowseLeafName": "Necrosis",
                        "ConditionBrowseLeafRelevance": "low",
                    },
                    {
                        "ConditionBrowseLeafId": "M5129",
                        "ConditionBrowseLeafName": "Coronary Artery Disease",
                        "ConditionBrowseLeafRelevance": "low",
                    },
                    {
                        "ConditionBrowseLeafId": "M18089",
                        "ConditionBrowseLeafName": "Myocardial Ischemia",
                        "ConditionBrowseLeafRelevance": "low",
                    },
                    {
                        "ConditionBrowseLeafId": "M8002",
                        "ConditionBrowseLeafName": "Heart Diseases",
                        "ConditionBrowseLeafRelevance": "low",
                    },
                    {
                        "ConditionBrowseLeafId": "M15983",
                        "ConditionBrowseLeafName": "Vascular Diseases",
                        "ConditionBrowseLeafRelevance": "low",
                    },
                ],
                "ConditionBrowseBranch": [
                    {
                        "ConditionBrowseBranchAbbrev": "BC14",
                        "ConditionBrowseBranchName": "Heart and Blood Diseases",
                    },
                    {
                        "ConditionBrowseBranchAbbrev": "All",
                        "ConditionBrowseBranchName": "All Conditions",
                    },
                    {
                        "ConditionBrowseBranchAbbrev": "BC23",
                        "ConditionBrowseBranchName": "Symptoms and General Pathology",
                    },
                ],
                "tags": [],
                "authz": "",
                "sites": "",
                "study_description_summary": "This study is to build a Chinese national registry and surveillance system for acute myocardial infarction(AMI) to obtain real-world information about current status of characteristics, risk factors, diagnosis, treatment and outcomes of Chinese AMI patients; And to propose scientific precaution strategies aimed to prevent effectively from the incidence of AMI; And to optimize the management and outcomes of AMI patients through implementation of guideline recommendations in clinical practice, and analysis and development of effective treatment strategies; And to create cost-effective assessment system.",
                "location": "Fuwai Hospital, Beijing, Beijing",
                "subjects": "20000",
                "__manifest": {"filename": "foo.zip "},
                "study_name_title": "",
                "study_type": "",
                "study_url": "https://clinicaltrials.gov/ct2/show/NCT01874691",
                "institutions": "Chinese Academy of Medical Sciences, Fuwai Hospital",
                "year_awarded": "",
                "investigators_name": "Yuejin Yang, MD.",
                "protocol_name": "",
                "study_summary": "",
                "_file_manifest": "",
                "dataset_1_type": "",
                "dataset_2_type": "",
                "dataset_3_type": "",
                "dataset_4_type": "",
                "dataset_5_type": "",
                "project_number": "2011BAI11B02-A",
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
            },
        }
    }

    json_response1 = r"""{
      "FullStudiesResponse":{
        "APIVrs":"1.01.03",
        "DataVrs":"2021:07:06 22:12:28.413",
        "Expression":"heart attack",
        "NStudiesAvail":382563,
        "NStudiesFound":8322,
        "MinRank":1,
        "MaxRank":3,
        "NStudiesReturned":3,
        "FullStudies":[
          {
            "Rank":1,
            "Study":{
              "ProtocolSection":{
                "IdentificationModule":{
                  "NCTId":"1"
                },
                "DescriptionModule":{
                  "BriefSummary":"summary1"
                },
                "DesignModule":{
                  "EnrollmentInfo":{
                    "EnrollmentCount":"100"
                  }
                }
              }
            }
          },
          {
            "Rank":2,
            "Study":{
              "ProtocolSection":{
                "IdentificationModule":{
                  "NCTId":"2"
                },
                "DescriptionModule":{
                  "BriefSummary":"summary2"
                },
                "DesignModule":{
                  "EnrollmentInfo":{
                    "EnrollmentCount":"100"
                  }
                }
              }
            }
          },
          {
            "Rank":3,
            "Study":{
              "ProtocolSection":{
                "IdentificationModule":{
                  "NCTId":"3"
                },
                "DescriptionModule":{
                  "BriefSummary":"summary3"
                },
                "DesignModule":{
                  "EnrollmentInfo":{
                    "EnrollmentCount":"100"
                  }
                }
              }
            }
          }
        ]
      }
    }"""

    json_response2 = r"""{
      "FullStudiesResponse":{
        "APIVrs":"1.01.03",
        "DataVrs":"2021:07:06 22:12:28.413",
        "Expression":"heart attack",
        "NStudiesAvail":382563,
        "NStudiesFound":8322,
        "MinRank":4,
        "MaxRank":5,
        "NStudiesReturned":2,
        "FullStudies":[
          {
            "Rank":4,
            "Study":{
              "ProtocolSection":{
                "IdentificationModule":{
                  "NCTId":"4"
                },
                "DescriptionModule":{
                  "BriefSummary":"summary4"
                },
                "DesignModule":{
                  "EnrollmentInfo":{
                    "EnrollmentCount":"100"
                  }
                }
              }
            }
          },
          {
            "Rank":5,
            "Study":{
              "ProtocolSection":{
                "IdentificationModule":{
                  "NCTId":"5"
                },
                "DescriptionModule":{
                  "BriefSummary":"summary5"
                },
                "DesignModule":{
                  "EnrollmentInfo":{
                    "EnrollmentCount":"100"
                  }
                }
              }
            }
          }
        ]
      }
    }"""

    json_response3 = r"""{
      "BadFullStudiesResponse":{
        "APIVrs":"1.01.03",
        "DataVrs":"2021:07:06 22:12:28.413",
        "Expression":"heart attack",
        "NStudiesAvail":382563,
        "NStudiesFound":8322,
        "MinRank":4,
        "MaxRank":5,
        "NStudiesReturned":2,
        "FullStudies":[
          {
            "Rank":4,
            "Study":{
              "ProtocolSection":{
                "IdentificationModule":{
                  "NCTId":"4"
                },
                "DescriptionModule":{
                  "BriefSummary":"summary4"
                },
                "DesignModule":{
                  "EnrollmentInfo":{
                    "EnrollmentCount":"100"
                  }
                }
              }
            }
          },
          {
            "Rank":5,
            "Study":{
              "ProtocolSection":{
                "IdentificationModule":{
                  "NCTId":"5"
                },
                "DescriptionModule":{
                  "BriefSummary":"summary5"
                },
                "DesignModule":{
                  "EnrollmentInfo":{
                    "EnrollmentCount":"100"
                  }
                }
              }
            }
          }
        ]
      }
    }"""

    respx.get("http://test/ok?expr=heart+attack&fmt=json&min_rnk=1&max_rnk=3").mock(
        return_value=httpx.Response(
            status_code=200,
            content=json_response1,
        )
    )

    respx.get("http://test/ok?expr=heart+attack&fmt=json&min_rnk=4&max_rnk=6").mock(
        return_value=httpx.Response(
            status_code=200,
            content=json_response2,
        )
    )

    get_metadata(
        "clinicaltrials",
        "http://test/ok",
        filters={"term": "heart attack", "maxItems": 5, "batchSize": 3},
        mappings=field_mappings,
        perItemValues=item_values,
        keepOriginalFields=True,
    )

    respx.get("http://test/ok?expr=heart+attack&fmt=json&min_rnk=4&max_rnk=6").mock(
        return_value=httpx.Response(
            status_code=200,
            content=json_response2,
        )
    )

    get_metadata(
        "clinicaltrials",
        "http://test/ok",
        filters={"term": "heart attack", "maxItems": 5, "batchSize": 3},
        mappings=field_mappings,
        perItemValues=item_values,
        keepOriginalFields=True,
    )

    get_metadata(
        "clinicaltrials",
        None,
        filters={"term": "heart attack", "maxItems": 5, "batchSize": 3},
        mappings=field_mappings,
        perItemValues=item_values,
        keepOriginalFields=True,
    )

    get_metadata(
        "clinicaltrials",
        "http://test/ok",
        filters={"term_bad": "heart attack", "maxItems": 5, "batchSize": 3},
        mappings=field_mappings,
        perItemValues=item_values,
        keepOriginalFields=True,
    )

    ## test missing fields

    respx.get(
        "http://test/error404?expr=heart+attack+false&fmt=json&min_rnk=4&max_rnk=6"
    ).mock(
        return_value=httpx.Response(
            status_code=404,
            content={},
        )
    )

    assert (
        get_metadata(
            "clinicaltrials",
            "http://test/error404",
            filters={"term": "heart+attack+false", "maxItems": 5, "batchSize": 5},
            mappings=field_mappings,
            keepOriginalFields=True,
        )
        == {}
    )

    ## test bad responses

    respx.get(
        "http://test/ok?expr=should+error+bad+field&fmt=json&min_rnk=1&max_rnk=1"
    ).mock(return_value=httpx.Response(status_code=200, content=json_response3))

    assert (
        get_metadata(
            "clinicaltrials",
            "http://test/ok",
            filters={"term": "should+error+bad+field", "maxItems": 1, "batchSize": 1},
            mappings=field_mappings,
            perItemValues=item_values,
            keepOriginalFields=True,
        )
        == {}
    )

    try:
        from mds.agg_mds.adapters import ClinicalTrials

        ClinicalTrials.getRemoteDataAsJson.retry.wait = wait_none()

        respx.get(
            "http://test/ok?expr=should+error+timeout&fmt=json&min_rnk=1&max_rnk=1"
        ).mock(side_effect=httpx.TimeoutException)

        get_metadata(
            "clinicaltrials",
            "http://test/ok",
            filters={"term": "should+error+timeout", "maxItems": 1, "batchSize": 1},
            mappings=field_mappings,
            perItemValues=item_values,
            keepOriginalFields=True,
        )
    except Exception as exc:
        assert isinstance(exc, RetryError) == True


@respx.mock
def test_get_metadata_pdaps():
    json_response = r"""{
  "monqcle_exists": false,
  "name": "laws-regulating-administration-of-naloxone",
  "title": "laws-regulating-administration-of-naloxone",
  "display_id": "599ddda695679f66768b4569",
  "preview": [
    {
      "_id": {
        "$id": "599ddda695679f66768b4569"
      },
      "naaddressoaayn": {
        "details": {
          "name": "NAAddressOAAYN",
          "description": "Jurisdiction has a naloxone law",
          "note": "naaddressoaayn",
          "slug": "naaddressoaayn",
          "weight": 4,
          "question": "Does the jurisdiction have a naloxone access law?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "nahealthcrimproyn": {
        "details": {
          "name": "NAHealthCrimProYN",
          "description": "Prescribers immune from criminal liability",
          "note": "nahealthcrimproyn",
          "slug": "nahealthcrimproyn",
          "weight": 5,
          "question": "Do prescribers have immunity from criminal prosecution for prescribing, dispensing or distributing naloxone to a layperson?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": true
        },
        "list": "root"
      },
      "nanapimm1yn": {
        "details": {
          "name": "NANAPImm1YN",
          "description": "Naloxone program participation required",
          "note": "nanapimm1yn",
          "slug": "nanapimm1yn",
          "weight": 6,
          "question": "Is participation in a naloxone administration program required as a condition of immunity?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "narcimm1yn": {
        "details": {
          "name": "NARCImm1YN",
          "description": "Acting with reasonable care required",
          "note": "narcimm1yn",
          "slug": "narcimm1yn",
          "weight": 7,
          "question": "Are prescribers required to act with reasonable care?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "nahealthcivliayn": {
        "details": {
          "name": "NAHealthCivLiaYN",
          "description": "Prescribers immune from civil liability",
          "note": "nahealthcivliayn",
          "slug": "nahealthcivliayn",
          "weight": 8,
          "question": "Do prescribers have immunity from civil liability for prescribing, dispensing or distributing naloxone to a layperson?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": true
        },
        "list": "root"
      },
      "nanapimm2yn": {
        "details": {
          "name": "NANAPImm2YN",
          "description": "Naloxone program participation required",
          "note": "nanapimm2yn",
          "slug": "nanapimm2yn",
          "weight": 9,
          "question": "Is participation in a naloxone administration program required as a condition of immunity?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "narcimm2yn": {
        "details": {
          "name": "NARCImm2YN",
          "description": "Acting with reasonable care required",
          "note": "narcimm2yn",
          "slug": "narcimm2yn",
          "weight": 10,
          "question": "Are prescribers required to act with reasonable care?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "naloxone-thirdparty": {
        "details": {
          "name": "Naloxone_ThirdParty",
          "description": "Third party prescription authorized",
          "note": "naloxone-thirdparty",
          "slug": "naloxone-thirdparty",
          "weight": 19,
          "question": "Are prescriptions of naloxone authorized to third parties?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": true
        },
        "list": "root"
      },
      "naloxone-thirdprog": {
        "details": {
          "name": "Naloxone_ThirdProg",
          "description": "Naloxone program participation required",
          "note": "naloxone-thirdprog",
          "slug": "naloxone-thirdprog",
          "weight": 20,
          "question": "Is naloxone program participation required for a third party prescription?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "naloxone-thirdcare": {
        "details": {
          "name": "Naloxone_ThirdCare",
          "description": "Acting with reasonable care required",
          "note": "naloxone-thirdcare",
          "slug": "naloxone-thirdcare",
          "weight": 21,
          "question": "Are prescribers required to act with reasonable care?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "naloxone-standing": {
        "details": {
          "name": "Naloxone_Standing",
          "description": "Prescription by standing order authorized",
          "note": "naloxone-standing",
          "slug": "naloxone-standing",
          "weight": 14,
          "question": "Is prescription by a standing order authorized?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": true
        },
        "list": "root"
      },
      "naloxone-standingprog": {
        "details": {
          "name": "Naloxone_StandingProg",
          "description": "Naloxone program participation required",
          "note": "naloxone-standingprog",
          "slug": "naloxone-standingprog",
          "weight": 15,
          "question": "Is participation in a naloxone administration program required for prescription by a standing order?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "naloxone-standingcare": {
        "details": {
          "name": "Naloxone_StandingCare",
          "description": "Acting with reasonable care required",
          "note": "naloxone-standingcare",
          "slug": "naloxone-standingcare",
          "weight": 16,
          "question": "Are prescribers required to act with reasonable care?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "naimmcrimprolpyn": {
        "details": {
          "name": "NAImmCrimProLPYN",
          "description": "Lay administrator immune from criminal prosecution",
          "note": "naimmcrimprolpyn",
          "slug": "naimmcrimprolpyn",
          "weight": 27,
          "question": "Is a layperson immune from criminal liability when administering naloxone?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": true
        },
        "list": "root"
      },
      "nanapimm3yn": {
        "details": {
          "name": "NANAPImm3YN",
          "description": "Naloxone program participation required",
          "note": "nanapimm3yn",
          "slug": "nanapimm3yn",
          "weight": 28,
          "question": "Is participation in a naloxone administration program required as a condition of immunity?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "narcimm3yn": {
        "details": {
          "name": "NARCImm3YN",
          "description": "Acting with reasonable care required",
          "note": "narcimm3yn",
          "slug": "narcimm3yn",
          "weight": 29,
          "question": "Are laypeople required to act with reasonable care?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "naimmcivlialpyn": {
        "details": {
          "name": "NAImmCivLiaLPYN",
          "description": "Lay administrator immune from civil liability",
          "note": "naimmcivlialpyn",
          "slug": "naimmcivlialpyn",
          "weight": 30,
          "question": "Is a layperson immune from civil liability when administering naloxone?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": true
        },
        "list": "root"
      },
      "nanapimm4yn": {
        "details": {
          "name": "NANAPImm4YN",
          "description": "Naloxone program participation required",
          "note": "nanapimm4yn",
          "slug": "nanapimm4yn",
          "weight": 31,
          "question": "Is participation in a naloxone administration program required as a condition of immunity?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "narcimm4yn": {
        "details": {
          "name": "NARCImm4YN",
          "description": "Acting with reasonable care required",
          "note": "narcimm4yn",
          "slug": "narcimm4yn",
          "weight": 32,
          "question": "Are laypeople required to act with reasonable care?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "naloxone-crimpossesion": {
        "details": {
          "name": "Naloxone_CrimPossesion",
          "description": "Removes criminal liability for possession of naloxone without a prescription",
          "note": "naloxone-crimpossesion",
          "slug": "naloxone-crimpossesion",
          "weight": 33,
          "question": "Does the law remove criminal liability for possession of naloxone without a prescription?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": true
        },
        "list": "root"
      },
      "naloxone-crimpossessionprog": {
        "details": {
          "name": "Naloxone_CrimPossessionProg",
          "description": "Naloxone program participation required",
          "note": "naloxone-crimpossessionprog",
          "slug": "naloxone-crimpossessionprog",
          "weight": 34,
          "question": "Is participation in a naloxone administration program required as a condition of immunity?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "naloxone-crimpossesioncare": {
        "details": {
          "name": "Naloxone_CrimPossesionCare",
          "description": "Acting with reasonable care required",
          "note": "naloxone-crimpossesioncare",
          "slug": "naloxone-crimpossesioncare",
          "weight": 25,
          "question": "Is acting with reasonable care required as a condition of immunity?",
          "type": "Categorical - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": true,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "naloxone-presprof": {
        "details": {
          "name": "Naloxone_PresProf",
          "description": "Prescribers immune from professional sanctions",
          "note": "naloxone-presprof",
          "slug": "naloxone-presprof",
          "weight": 11,
          "question": "Do prescribers have immunity from professional sanctions for prescribing, dispensing, or distributing naloxone to a layperson?",
          "type": "Binary - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": false,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "naloxone-dispcrim": {
        "details": {
          "name": "Naloxone_DispCrim",
          "description": "Dispensers immune from criminal liability",
          "note": "naloxone-dispcrim",
          "slug": "naloxone-dispcrim",
          "weight": 12,
          "question": "Do dispensers have immunity from criminal prosecution for prescribing, dispensing or distributing naloxone to a layperson?",
          "type": "Binary - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": false,
          "show_citation": true,
          "has_children": true
        },
        "list": "root"
      },
      "naloxone-dcrimpro": {
        "details": {
          "name": "Naloxone_DCrimPro",
          "description": "Naloxone program participation required",
          "note": "naloxone-dcrimpro",
          "slug": "naloxone-dcrimpro",
          "weight": 13,
          "question": "Is participation in a naloxone program required as a condition of immunity?",
          "type": "Binary - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": false,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "naloxone-dcrimcare": {
        "details": {
          "name": "Naloxone_DCrimCare",
          "description": "Acting with reasonable care required",
          "note": "naloxone-dcrimcare",
          "slug": "naloxone-dcrimcare",
          "weight": 14,
          "question": "Are dispensers required to act with reasonable care?",
          "type": "Binary - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": false,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "naloxone-dispciv": {
        "details": {
          "name": "Naloxone_DispCiv",
          "description": "Dispenser immune from civil liability",
          "note": "naloxone-dispciv",
          "slug": "naloxone-dispciv",
          "weight": 15,
          "question": "Do dispensers have immunity from civil liability for prescribing, dispensing or distributing naloxone to a layperson?",
          "type": "Binary - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": false,
          "show_citation": true,
          "has_children": true
        },
        "list": "root"
      },
      "naloxone-dcivprog": {
        "details": {
          "name": "Naloxone_DCivProg",
          "description": "Naloxone program participation required",
          "note": "naloxone-dcivprog",
          "slug": "naloxone-dcivprog",
          "weight": 16,
          "question": "Is participation in a naloxone program required as a condition of immunity?",
          "type": "Binary - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": false,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "naloxone-dcivcare": {
        "details": {
          "name": "Naloxone_DCivCare",
          "description": "Acting with reasonable care required",
          "note": "naloxone-dcivcare",
          "slug": "naloxone-dcivcare",
          "weight": 17,
          "question": "Are dispensers required to act with reasonable care?",
          "type": "Binary - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": false,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "naloxone-presprof-disp": {
        "details": {
          "name": "Naloxone_PresProf_Disp",
          "description": "Dispenser immune from professional sanctions",
          "note": "naloxone-presprof-disp",
          "slug": "naloxone-presprof-disp",
          "weight": 18,
          "question": "Do dispensers have immunity from professional sanctions for prescribing, dispensing, or distributing naloxone to a layperson?",
          "type": "Binary - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": false,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "pharmacist-dispensing": {
        "details": {
          "name": "Pharmacist_dispensing",
          "description": "Pharmacist dispensing without patient-specific prescription",
          "note": "pharmacist-dispensing",
          "slug": "pharmacist-dispensing",
          "weight": 25,
          "question": "Are pharmacists allowed to dispense or distribute naloxone without a patient-specific prescription from another medical professional?",
          "type": "Binary - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": false,
          "show_citation": true,
          "has_children": true
        },
        "list": "root"
      },
      "pharmacist-dispensing-method": {
        "details": {
          "name": "Pharmacist_dispensing_method",
          "description": "Pharmacist dispensing method",
          "note": "pharmacist-dispensing-method",
          "slug": "pharmacist-dispensing-method",
          "weight": 26,
          "question": "How are pharmacists allowed to dispense or distribute naloxone without a patient-specific prescription from another medical professional?",
          "type": "Categorical - check all that apply",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Standing order\r\nProtocol order\r\nNaloxone-specific collaborative practice agreement\r\nPharmacist prescriptive authority\r\nDirectly authorized by legislature",
          "is_dich": true,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "crim-possess-reasonable": {
        "details": {
          "name": "Crim_possess_reasonable",
          "description": "Reasonable care",
          "note": "crim-possess-reasonable",
          "slug": "crim-possess-reasonable",
          "weight": 35,
          "question": "Is acting with reasonable care required as a condition of immunity?",
          "type": "Binary - mutually exclusive",
          "parent": false,
          "hide_for_map_question_view": false,
          "hide_for_map_query_view": false,
          "answers": "Yes\r\nNo",
          "is_dich": false,
          "show_citation": true,
          "has_children": false
        },
        "list": "root"
      },
      "list": {
        "naaddressoaayn": "root",
        "nahealthcrimproyn": "root",
        "nanapimm1yn": "nahealthcrimproyn",
        "narcimm1yn": "nahealthcrimproyn",
        "nahealthcivliayn": "root",
        "nanapimm2yn": "nahealthcivliayn",
        "narcimm2yn": "nahealthcivliayn",
        "naloxone-thirdparty": "root",
        "naloxone-thirdprog": "naloxone-thirdparty",
        "naloxone-thirdcare": "naloxone-thirdparty",
        "naloxone-standing": "root",
        "naloxone-standingprog": "naloxone-standing",
        "naloxone-standingcare": "naloxone-standing",
        "naimmcrimprolpyn": "root",
        "nanapimm3yn": "naimmcrimprolpyn",
        "narcimm3yn": "naimmcrimprolpyn",
        "naimmcivlialpyn": "root",
        "nanapimm4yn": "naimmcivlialpyn",
        "narcimm4yn": "naimmcivlialpyn",
        "naloxone-crimpossesion": "root",
        "naloxone-crimpossessionprog": "naloxone-crimpossesion",
        "naloxone-crimpossesioncare": "naloxone-crimpossesion",
        "naloxone-presprof": "root",
        "naloxone-dispcrim": "root",
        "naloxone-dcrimpro": "naloxone-dispcrim",
        "naloxone-dcrimcare": "naloxone-dispcrim",
        "naloxone-dispciv": "root",
        "naloxone-dcivprog": "naloxone-dispciv",
        "naloxone-dcivcare": "naloxone-dispciv",
        "naloxone-presprof-disp": "root",
        "pharmacist-dispensing": "root",
        "pharmacist-dispensing-method": "pharmacist-dispensing",
        "crim-possess-reasonable": "naloxone-crimpossesion"
      },
      "Preview": {
        "id": "57b45d83d6c9e7e8693ccdfa",
        "title": "Naloxone Overdose Prevention Laws",
        "permissions": {
          "users": [],
          "groups": []
        },
        "dataset": "93ccdaa",
        "datasets": {
          "93ccdaa": "Naloxone Overdose Prevention Laws"
        },
        "updated": {
          "date": "2017-08-23 15:55:17.000000",
          "timezone_type": 3,
          "timezone": "America/New_York"
        }
      },
      "published": false,
      "created_at": {
        "sec": 1503518117,
        "usec": 998000
      },
      "created_by": "elizabeth.platt@temple.edu",
      "title": "Naloxone Overdose Prevention Laws",
      "description": "\nUnintentional drug overdose is a leading cause of preventable death in the United States. Administering naloxone hydrochloride (“naloxone”) can reverse an opioid overdose and prevent these unintentional deaths. This dataset focuses on state laws that provide civil or criminal immunity to licensed healthcare providers or lay responders for opioid antagonist administration.</span></span></p>\r\n\n\nThis is a longitudinal dataset displaying laws from January 1, 2001 through July 1, 2017.</span></p>",
      "learn_more": "",
      "learn_more_pdf": "Naloxone_EI.pdf",
      "learn_more_id": "57bdd0aad6c9e7a5405365b3",
      "data_data": "20170725 Naloxone Stat Data.xlsx",
      "data_id": "599d96be95679f156f8b4567",
      "data_data_lawatlas": "20170725 Naloxone Stat Data.xlsx",
      "codebook_description": "",
      "codebook_curator": "",
      "codebook_id": "599d969195679f066f8b4567",
      "data_codebook": "20170725_Naloxone_Codebook.pdf",
      "data_codebook_lawatlas": "20170725_Naloxone_Codebook.pdf",
      "data_protocol": "20170725 Naloxone Protocol.pdf",
      "protocol_id": "599d96a295679f026f8b4567",
      "data_protocol_lawatlas": "20170725 Naloxone Protocol.pdf",
      "final_report": "20170725_Naloxone_Report.pdf",
      "report_id": "599d96c995679f026f8b4568",
      "final_report_lawatlas": "20170725_Naloxone_Report.pdf",
      "sidebar": "\n\nRelated Resources</span>\n</span></strong></span></p>\r\n\n\nPublic Health Law Research Knowledge Asset: Naloxone for Community Opioid Overdose Reversal</a></span></span></p>\r\n\n\nPHLR-Funded Research</span></span>: </strong>How can public health law support intervention in drug overdoses?</a></span></p>\r\n\n\nNetwork for Public Health Law: Fact Sheet:  Naloxone Access and Overdose Good Samaritan Laws</a> </span> </span></p>",
      "sidebar_profile": "\n\nSubject Matter Expert</strong></span></p>\r\n\n\nCorey Davis, JD MSPH</a></span></span></p>",
      "faq": "\n\nAs of July 1, 2017, all states have a naloxone access law. </span></p>",
      "use_slider": true,
      "use_month_slider": false,
      "use_all_query": false,
      "show_fed_color_on_map": false,
      "start_year": "2001",
      "end_year": "2017",
      "current_year": "2017",
      "use_oneyear": true,
      "use_overlaps": false,
      "use_geo": false,
      "start_coordinates_type": "",
      "start_coordinates_label": "",
      "start_coordinates": "",
      "show_state_label": false,
      "all_states": true,
      "include_states": [],
      "hide_start_here": false,
      "hide_map": false,
      "hide_faq": false,
      "hide_table_controls": false,
      "use_state_for_start_query": false
    }
  ],
  "published": true
}"""

    field_mappings = {
        "tags": [],
        "authz": "",
        "sites": "",
        "study_description_summary": "path:preview[0].description",
        "location": "United States",
        "subjects": "",
        "__manifest": "",
        "study_name_title": "",
        "study_type": "",
        "institutions": "Temple University",
        "year_awarded": "",
        "investigators_name": "path:preview[0].created_by",
        "project_title": "path:preview[0].title",
        "protocol_name": "",
        "study_summary": "path:preview[0].description",
        "_file_manifest": "",
        "dataset_1_type": "Data",
        "dataset_2_type": "Codebook",
        "dataset_3_type": "Protocol",
        "dataset_4_type": "Final report",
        "dataset_5_type": "",
        "project_number": "path:display_id",
        "dataset_1_title": "path:preview[0].data_data",
        "dataset_2_title": "path:preview[0].data_codebook",
        "dataset_3_title": "path:preview[0].data_protocol",
        "dataset_4_title": "path:preview[0].final_report",
        "dataset_5_title": "",
        "administering_ic": "NIDA",
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
    }

    item_values = {
        "599ddda695679f66768b4569": {
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

    ## failed calls

    respx.request(
        "get",
        "http://test/ok/siteitem/laws-regulating-administration-of-naloxone/get_by_dataset?site_key=56e805b9d6c9e75c1ac8cb12",
    ).mock(
        return_value=httpx.Response(
            status_code=200,
            content=json_response,
        )
    )

    assert get_metadata("pdaps", "http://test/ok/", filters=None) == {}

    assert (
        get_metadata(
            "pdaps",
            None,
            filters={"datasets": ["laws-regulating-administration-of-naloxone"]},
        )
        == {}
    )

    assert get_metadata(
        "pdaps",
        "http://test/ok/",
        filters={"datasets": ["laws-regulating-administration-of-naloxone"]},
        mappings=field_mappings,
        perItemValues=item_values,
        keepOriginalFields=True,
    ) == json.loads(
        r"""{
  "599ddda695679f66768b4569": {
    "_guid_type": "discovery_metadata",
    "gen3_discovery": {
      "monqcle_exists": false,
      "name": "laws-regulating-administration-of-naloxone",
      "title": "laws-regulating-administration-of-naloxone",
      "display_id": "599ddda695679f66768b4569",
      "preview": [
        {
          "_id": {
            "$id": "599ddda695679f66768b4569"
          },
          "naaddressoaayn": {
            "details": {
              "name": "NAAddressOAAYN",
              "description": "Jurisdiction has a naloxone law",
              "note": "naaddressoaayn",
              "slug": "naaddressoaayn",
              "weight": 4,
              "question": "Does the jurisdiction have a naloxone access law?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "nahealthcrimproyn": {
            "details": {
              "name": "NAHealthCrimProYN",
              "description": "Prescribers immune from criminal liability",
              "note": "nahealthcrimproyn",
              "slug": "nahealthcrimproyn",
              "weight": 5,
              "question": "Do prescribers have immunity from criminal prosecution for prescribing, dispensing or distributing naloxone to a layperson?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": true
            },
            "list": "root"
          },
          "nanapimm1yn": {
            "details": {
              "name": "NANAPImm1YN",
              "description": "Naloxone program participation required",
              "note": "nanapimm1yn",
              "slug": "nanapimm1yn",
              "weight": 6,
              "question": "Is participation in a naloxone administration program required as a condition of immunity?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "narcimm1yn": {
            "details": {
              "name": "NARCImm1YN",
              "description": "Acting with reasonable care required",
              "note": "narcimm1yn",
              "slug": "narcimm1yn",
              "weight": 7,
              "question": "Are prescribers required to act with reasonable care?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "nahealthcivliayn": {
            "details": {
              "name": "NAHealthCivLiaYN",
              "description": "Prescribers immune from civil liability",
              "note": "nahealthcivliayn",
              "slug": "nahealthcivliayn",
              "weight": 8,
              "question": "Do prescribers have immunity from civil liability for prescribing, dispensing or distributing naloxone to a layperson?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": true
            },
            "list": "root"
          },
          "nanapimm2yn": {
            "details": {
              "name": "NANAPImm2YN",
              "description": "Naloxone program participation required",
              "note": "nanapimm2yn",
              "slug": "nanapimm2yn",
              "weight": 9,
              "question": "Is participation in a naloxone administration program required as a condition of immunity?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "narcimm2yn": {
            "details": {
              "name": "NARCImm2YN",
              "description": "Acting with reasonable care required",
              "note": "narcimm2yn",
              "slug": "narcimm2yn",
              "weight": 10,
              "question": "Are prescribers required to act with reasonable care?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "naloxone-thirdparty": {
            "details": {
              "name": "Naloxone_ThirdParty",
              "description": "Third party prescription authorized",
              "note": "naloxone-thirdparty",
              "slug": "naloxone-thirdparty",
              "weight": 19,
              "question": "Are prescriptions of naloxone authorized to third parties?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": true
            },
            "list": "root"
          },
          "naloxone-thirdprog": {
            "details": {
              "name": "Naloxone_ThirdProg",
              "description": "Naloxone program participation required",
              "note": "naloxone-thirdprog",
              "slug": "naloxone-thirdprog",
              "weight": 20,
              "question": "Is naloxone program participation required for a third party prescription?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "naloxone-thirdcare": {
            "details": {
              "name": "Naloxone_ThirdCare",
              "description": "Acting with reasonable care required",
              "note": "naloxone-thirdcare",
              "slug": "naloxone-thirdcare",
              "weight": 21,
              "question": "Are prescribers required to act with reasonable care?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "naloxone-standing": {
            "details": {
              "name": "Naloxone_Standing",
              "description": "Prescription by standing order authorized",
              "note": "naloxone-standing",
              "slug": "naloxone-standing",
              "weight": 14,
              "question": "Is prescription by a standing order authorized?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": true
            },
            "list": "root"
          },
          "naloxone-standingprog": {
            "details": {
              "name": "Naloxone_StandingProg",
              "description": "Naloxone program participation required",
              "note": "naloxone-standingprog",
              "slug": "naloxone-standingprog",
              "weight": 15,
              "question": "Is participation in a naloxone administration program required for prescription by a standing order?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "naloxone-standingcare": {
            "details": {
              "name": "Naloxone_StandingCare",
              "description": "Acting with reasonable care required",
              "note": "naloxone-standingcare",
              "slug": "naloxone-standingcare",
              "weight": 16,
              "question": "Are prescribers required to act with reasonable care?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "naimmcrimprolpyn": {
            "details": {
              "name": "NAImmCrimProLPYN",
              "description": "Lay administrator immune from criminal prosecution",
              "note": "naimmcrimprolpyn",
              "slug": "naimmcrimprolpyn",
              "weight": 27,
              "question": "Is a layperson immune from criminal liability when administering naloxone?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": true
            },
            "list": "root"
          },
          "nanapimm3yn": {
            "details": {
              "name": "NANAPImm3YN",
              "description": "Naloxone program participation required",
              "note": "nanapimm3yn",
              "slug": "nanapimm3yn",
              "weight": 28,
              "question": "Is participation in a naloxone administration program required as a condition of immunity?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "narcimm3yn": {
            "details": {
              "name": "NARCImm3YN",
              "description": "Acting with reasonable care required",
              "note": "narcimm3yn",
              "slug": "narcimm3yn",
              "weight": 29,
              "question": "Are laypeople required to act with reasonable care?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "naimmcivlialpyn": {
            "details": {
              "name": "NAImmCivLiaLPYN",
              "description": "Lay administrator immune from civil liability",
              "note": "naimmcivlialpyn",
              "slug": "naimmcivlialpyn",
              "weight": 30,
              "question": "Is a layperson immune from civil liability when administering naloxone?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": true
            },
            "list": "root"
          },
          "nanapimm4yn": {
            "details": {
              "name": "NANAPImm4YN",
              "description": "Naloxone program participation required",
              "note": "nanapimm4yn",
              "slug": "nanapimm4yn",
              "weight": 31,
              "question": "Is participation in a naloxone administration program required as a condition of immunity?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "narcimm4yn": {
            "details": {
              "name": "NARCImm4YN",
              "description": "Acting with reasonable care required",
              "note": "narcimm4yn",
              "slug": "narcimm4yn",
              "weight": 32,
              "question": "Are laypeople required to act with reasonable care?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "naloxone-crimpossesion": {
            "details": {
              "name": "Naloxone_CrimPossesion",
              "description": "Removes criminal liability for possession of naloxone without a prescription",
              "note": "naloxone-crimpossesion",
              "slug": "naloxone-crimpossesion",
              "weight": 33,
              "question": "Does the law remove criminal liability for possession of naloxone without a prescription?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": true
            },
            "list": "root"
          },
          "naloxone-crimpossessionprog": {
            "details": {
              "name": "Naloxone_CrimPossessionProg",
              "description": "Naloxone program participation required",
              "note": "naloxone-crimpossessionprog",
              "slug": "naloxone-crimpossessionprog",
              "weight": 34,
              "question": "Is participation in a naloxone administration program required as a condition of immunity?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "naloxone-crimpossesioncare": {
            "details": {
              "name": "Naloxone_CrimPossesionCare",
              "description": "Acting with reasonable care required",
              "note": "naloxone-crimpossesioncare",
              "slug": "naloxone-crimpossesioncare",
              "weight": 25,
              "question": "Is acting with reasonable care required as a condition of immunity?",
              "type": "Categorical - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": true,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "naloxone-presprof": {
            "details": {
              "name": "Naloxone_PresProf",
              "description": "Prescribers immune from professional sanctions",
              "note": "naloxone-presprof",
              "slug": "naloxone-presprof",
              "weight": 11,
              "question": "Do prescribers have immunity from professional sanctions for prescribing, dispensing, or distributing naloxone to a layperson?",
              "type": "Binary - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": false,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "naloxone-dispcrim": {
            "details": {
              "name": "Naloxone_DispCrim",
              "description": "Dispensers immune from criminal liability",
              "note": "naloxone-dispcrim",
              "slug": "naloxone-dispcrim",
              "weight": 12,
              "question": "Do dispensers have immunity from criminal prosecution for prescribing, dispensing or distributing naloxone to a layperson?",
              "type": "Binary - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": false,
              "show_citation": true,
              "has_children": true
            },
            "list": "root"
          },
          "naloxone-dcrimpro": {
            "details": {
              "name": "Naloxone_DCrimPro",
              "description": "Naloxone program participation required",
              "note": "naloxone-dcrimpro",
              "slug": "naloxone-dcrimpro",
              "weight": 13,
              "question": "Is participation in a naloxone program required as a condition of immunity?",
              "type": "Binary - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": false,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "naloxone-dcrimcare": {
            "details": {
              "name": "Naloxone_DCrimCare",
              "description": "Acting with reasonable care required",
              "note": "naloxone-dcrimcare",
              "slug": "naloxone-dcrimcare",
              "weight": 14,
              "question": "Are dispensers required to act with reasonable care?",
              "type": "Binary - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": false,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "naloxone-dispciv": {
            "details": {
              "name": "Naloxone_DispCiv",
              "description": "Dispenser immune from civil liability",
              "note": "naloxone-dispciv",
              "slug": "naloxone-dispciv",
              "weight": 15,
              "question": "Do dispensers have immunity from civil liability for prescribing, dispensing or distributing naloxone to a layperson?",
              "type": "Binary - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": false,
              "show_citation": true,
              "has_children": true
            },
            "list": "root"
          },
          "naloxone-dcivprog": {
            "details": {
              "name": "Naloxone_DCivProg",
              "description": "Naloxone program participation required",
              "note": "naloxone-dcivprog",
              "slug": "naloxone-dcivprog",
              "weight": 16,
              "question": "Is participation in a naloxone program required as a condition of immunity?",
              "type": "Binary - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": false,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "naloxone-dcivcare": {
            "details": {
              "name": "Naloxone_DCivCare",
              "description": "Acting with reasonable care required",
              "note": "naloxone-dcivcare",
              "slug": "naloxone-dcivcare",
              "weight": 17,
              "question": "Are dispensers required to act with reasonable care?",
              "type": "Binary - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": false,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "naloxone-presprof-disp": {
            "details": {
              "name": "Naloxone_PresProf_Disp",
              "description": "Dispenser immune from professional sanctions",
              "note": "naloxone-presprof-disp",
              "slug": "naloxone-presprof-disp",
              "weight": 18,
              "question": "Do dispensers have immunity from professional sanctions for prescribing, dispensing, or distributing naloxone to a layperson?",
              "type": "Binary - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": false,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "pharmacist-dispensing": {
            "details": {
              "name": "Pharmacist_dispensing",
              "description": "Pharmacist dispensing without patient-specific prescription",
              "note": "pharmacist-dispensing",
              "slug": "pharmacist-dispensing",
              "weight": 25,
              "question": "Are pharmacists allowed to dispense or distribute naloxone without a patient-specific prescription from another medical professional?",
              "type": "Binary - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": false,
              "show_citation": true,
              "has_children": true
            },
            "list": "root"
          },
          "pharmacist-dispensing-method": {
            "details": {
              "name": "Pharmacist_dispensing_method",
              "description": "Pharmacist dispensing method",
              "note": "pharmacist-dispensing-method",
              "slug": "pharmacist-dispensing-method",
              "weight": 26,
              "question": "How are pharmacists allowed to dispense or distribute naloxone without a patient-specific prescription from another medical professional?",
              "type": "Categorical - check all that apply",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Standing order\r\nProtocol order\r\nNaloxone-specific collaborative practice agreement\r\nPharmacist prescriptive authority\r\nDirectly authorized by legislature",
              "is_dich": true,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "crim-possess-reasonable": {
            "details": {
              "name": "Crim_possess_reasonable",
              "description": "Reasonable care",
              "note": "crim-possess-reasonable",
              "slug": "crim-possess-reasonable",
              "weight": 35,
              "question": "Is acting with reasonable care required as a condition of immunity?",
              "type": "Binary - mutually exclusive",
              "parent": false,
              "hide_for_map_question_view": false,
              "hide_for_map_query_view": false,
              "answers": "Yes\r\nNo",
              "is_dich": false,
              "show_citation": true,
              "has_children": false
            },
            "list": "root"
          },
          "list": {
            "naaddressoaayn": "root",
            "nahealthcrimproyn": "root",
            "nanapimm1yn": "nahealthcrimproyn",
            "narcimm1yn": "nahealthcrimproyn",
            "nahealthcivliayn": "root",
            "nanapimm2yn": "nahealthcivliayn",
            "narcimm2yn": "nahealthcivliayn",
            "naloxone-thirdparty": "root",
            "naloxone-thirdprog": "naloxone-thirdparty",
            "naloxone-thirdcare": "naloxone-thirdparty",
            "naloxone-standing": "root",
            "naloxone-standingprog": "naloxone-standing",
            "naloxone-standingcare": "naloxone-standing",
            "naimmcrimprolpyn": "root",
            "nanapimm3yn": "naimmcrimprolpyn",
            "narcimm3yn": "naimmcrimprolpyn",
            "naimmcivlialpyn": "root",
            "nanapimm4yn": "naimmcivlialpyn",
            "narcimm4yn": "naimmcivlialpyn",
            "naloxone-crimpossesion": "root",
            "naloxone-crimpossessionprog": "naloxone-crimpossesion",
            "naloxone-crimpossesioncare": "naloxone-crimpossesion",
            "naloxone-presprof": "root",
            "naloxone-dispcrim": "root",
            "naloxone-dcrimpro": "naloxone-dispcrim",
            "naloxone-dcrimcare": "naloxone-dispcrim",
            "naloxone-dispciv": "root",
            "naloxone-dcivprog": "naloxone-dispciv",
            "naloxone-dcivcare": "naloxone-dispciv",
            "naloxone-presprof-disp": "root",
            "pharmacist-dispensing": "root",
            "pharmacist-dispensing-method": "pharmacist-dispensing",
            "crim-possess-reasonable": "naloxone-crimpossesion"
          },
          "Preview": {
            "id": "57b45d83d6c9e7e8693ccdfa",
            "title": "Naloxone Overdose Prevention Laws",
            "permissions": {
              "users": [],
              "groups": []
            },
            "dataset": "93ccdaa",
            "datasets": {
              "93ccdaa": "Naloxone Overdose Prevention Laws"
            },
            "updated": {
              "date": "2017-08-23 15:55:17.000000",
              "timezone_type": 3,
              "timezone": "America/New_York"
            }
          },
          "published": false,
          "created_at": {
            "sec": 1503518117,
            "usec": 998000
          },
          "created_by": "elizabeth.platt@temple.edu",
          "title": "Naloxone Overdose Prevention Laws",
          "description": "\nUnintentional drug overdose is a leading cause of preventable death in the United States. Administering naloxone hydrochloride (“naloxone”) can reverse an opioid overdose and prevent these unintentional deaths. This dataset focuses on state laws that provide civil or criminal immunity to licensed healthcare providers or lay responders for opioid antagonist administration.</span></span></p>\r\n\n\nThis is a longitudinal dataset displaying laws from January 1, 2001 through July 1, 2017.</span></p>",
          "learn_more": "",
          "learn_more_pdf": "Naloxone_EI.pdf",
          "learn_more_id": "57bdd0aad6c9e7a5405365b3",
          "data_data": "20170725 Naloxone Stat Data.xlsx",
          "data_id": "599d96be95679f156f8b4567",
          "data_data_lawatlas": "20170725 Naloxone Stat Data.xlsx",
          "codebook_description": "",
          "codebook_curator": "",
          "codebook_id": "599d969195679f066f8b4567",
          "data_codebook": "20170725_Naloxone_Codebook.pdf",
          "data_codebook_lawatlas": "20170725_Naloxone_Codebook.pdf",
          "data_protocol": "20170725 Naloxone Protocol.pdf",
          "protocol_id": "599d96a295679f026f8b4567",
          "data_protocol_lawatlas": "20170725 Naloxone Protocol.pdf",
          "final_report": "20170725_Naloxone_Report.pdf",
          "report_id": "599d96c995679f026f8b4568",
          "final_report_lawatlas": "20170725_Naloxone_Report.pdf",
          "sidebar": "\n\nRelated Resources</span>\n</span></strong></span></p>\r\n\n\nPublic Health Law Research Knowledge Asset: Naloxone for Community Opioid Overdose Reversal</a></span></span></p>\r\n\n\nPHLR-Funded Research</span></span>: </strong>How can public health law support intervention in drug overdoses?</a></span></p>\r\n\n\nNetwork for Public Health Law: Fact Sheet:  Naloxone Access and Overdose Good Samaritan Laws</a> </span> </span></p>",
          "sidebar_profile": "\n\nSubject Matter Expert</strong></span></p>\r\n\n\nCorey Davis, JD MSPH</a></span></span></p>",
          "faq": "\n\nAs of July 1, 2017, all states have a naloxone access law. </span></p>",
          "use_slider": true,
          "use_month_slider": false,
          "use_all_query": false,
          "show_fed_color_on_map": false,
          "start_year": "2001",
          "end_year": "2017",
          "current_year": "2017",
          "use_oneyear": true,
          "use_overlaps": false,
          "use_geo": false,
          "start_coordinates_type": "",
          "start_coordinates_label": "",
          "start_coordinates": "",
          "show_state_label": false,
          "all_states": true,
          "include_states": [],
          "hide_start_here": false,
          "hide_map": false,
          "hide_faq": false,
          "hide_table_controls": false,
          "use_state_for_start_query": false
        }
      ],
      "published": true,
      "tags": [],
      "authz": "",
      "sites": "",
      "study_description_summary": "\nUnintentional drug overdose is a leading cause of preventable death in the United States. Administering naloxone hydrochloride (“naloxone”) can reverse an opioid overdose and prevent these unintentional deaths. This dataset focuses on state laws that provide civil or criminal immunity to licensed healthcare providers or lay responders for opioid antagonist administration.</span></span></p>\r\n\n\nThis is a longitudinal dataset displaying laws from January 1, 2001 through July 1, 2017.</span></p>",
      "location": "United States",
      "subjects": "",
      "__manifest": [
        {
          "md5sum": "7cf87",
          "file_name": "TEDS-D-2018-DS0001-bndl-data-spss.zip",
          "file_size": 69297783,
          "object_id": "dg.XXXX/208f4c52-771e-409a-b810-4bcba3c03c51"
        }
      ],
      "study_name_title": "",
      "study_type": "",
      "institutions": "Temple University",
      "year_awarded": "",
      "investigators_name": "elizabeth.platt@temple.edu",
      "project_title": "Naloxone Overdose Prevention Laws",
      "protocol_name": "",
      "study_summary": "\nUnintentional drug overdose is a leading cause of preventable death in the United States. Administering naloxone hydrochloride (“naloxone”) can reverse an opioid overdose and prevent these unintentional deaths. This dataset focuses on state laws that provide civil or criminal immunity to licensed healthcare providers or lay responders for opioid antagonist administration.</span></span></p>\r\n\n\nThis is a longitudinal dataset displaying laws from January 1, 2001 through July 1, 2017.</span></p>",
      "_file_manifest": "",
      "dataset_1_type": "Data",
      "dataset_2_type": "Codebook",
      "dataset_3_type": "Protocol",
      "dataset_4_type": "Final report",
      "dataset_5_type": "",
      "project_number": "599ddda695679f66768b4569",
      "dataset_1_title": "20170725 Naloxone Stat Data.xlsx",
      "dataset_2_title": "20170725_Naloxone_Codebook.pdf",
      "dataset_3_title": "20170725 Naloxone Protocol.pdf",
      "dataset_4_title": "20170725_Naloxone_Report.pdf",
      "dataset_5_title": "",
      "administering_ic": "NIDA",
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
      "dataset_5_description": ""
    }
  }
}
"""
    )

    respx.request(
        "get",
        "http://test/err404/siteitem/laws-regulating-administration-of-naloxone/get_by_dataset?site_key=56e805b9d6c9e75c1ac8cb12",
    ).mock(
        return_value=httpx.Response(
            status_code=404,
            json={},
        )
    )

    get_metadata(
        "pdaps",
        "http://test/err404",
        filters={"datasets": ["laws-regulating-administration-of-naloxone"]},
        mappings=field_mappings,
        perItemValues=item_values,
        keepOriginalFields=True,
    ) == {}

    get_metadata(
        "pdaps",
        "http://test/ok",
        filters={"datasets_error": ["laws-regulating-administration-of-naloxone"]},
        mappings=field_mappings,
        perItemValues=item_values,
        keepOriginalFields=True,
    ) == {}

    try:
        from mds.agg_mds.adapters import PDAPS

        PDAPS.getRemoteDataAsJson.retry.wait = wait_none()

        respx.get(
            "http://test/timeouterror/siteitem/laws-regulating-administration-of-naloxone/get_by_dataset?site_key=56e805b9d6c9e75c1ac8cb12"
        ).mock(side_effect=httpx.TimeoutException)

        get_metadata(
            "pdaps",
            "http://test/timeouterror",
            filters={"datasets": ["laws-regulating-administration-of-naloxone"]},
            mappings=field_mappings,
            perItemValues=item_values,
            keepOriginalFields=True,
        )
    except Exception as exc:
        assert isinstance(exc, RetryError) == True


@respx.mock
def test_get_metadata_mps():
    json_response = {
        "id": "23",
        "name": "Motif-PS-Chipshop",
        "data_group": "Taylor_MPS",
        "study_types": "CC",
        "start_date": "2015-09-30",
        "description": "Motif polystyrene devices, using different flow setups. Human hepatocytes fresh isolated were used in. (3 cell model)",
    }

    field_mappings = {
        "tags": [
            {"name": "MPS", "category": "Commons"},
            {"name": "Physiological", "category": "Data Type"},
        ],
        "authz": "",
        "sites": "",
        "summary": "path:description",
        "study_url": "path:url",
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
        "data_availability": "",
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

    # Test remote client-side error handling
    respx.get("http://test/err404/23/").mock(
        return_value=httpx.Response(
            status_code=404,
            json={},
        )
    )
    assert (
        get_metadata(
            "mps",
            "http://test/err404",
            filters={"study_ids": [23]},
            mappings=field_mappings,
            keepOriginalFields=False,
        )
        == {}
    )

    # Test general exception handling
    respx.get("http://test/textresponse/23/").mock(
        return_value=httpx.Response(
            status_code=200,
            text="hello!",
        )
    )
    assert (
        get_metadata(
            "mps",
            "http://test/textresponse",
            filters={"study_ids": [23]},
            mappings=field_mappings,
            keepOriginalFields=False,
        )
        == {}
    )

    respx.get("http://test/ok/23/").mock(
        return_value=httpx.Response(
            status_code=200,
            json=json_response,
        )
    )

    assert get_metadata(
        "mps",
        "http://test/ok",
        filters={"study_ids": [23]},
        mappings=field_mappings,
        keepOriginalFields=False,
    ) == {
        "MPS_study_23": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "tags": [
                    {"name": "MPS", "category": "Commons"},
                    {"name": "Physiological", "category": "Data Type"},
                ],
                "authz": "",
                "sites": "",
                "summary": "Motif polystyrene devices, using different flow setups. Human hepatocytes fresh isolated were used in. (3 cell model)",
                "study_url": "",
                "location": "Taylor_MPS",
                "subjects": "",
                "__manifest": "",
                "study_name": "Motif-PS-Chipshop",
                "study_type": "CC",
                "institutions": "Taylor_MPS",
                "year_awarded": "",
                "investigators": "Taylor_MPS",
                "project_title": "",
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
                "data_availability": "",
            },
        }
    }

    assert get_metadata(
        "mps",
        "http://test/ok",
        filters={"study_ids": [23]},
        mappings=field_mappings,
        keepOriginalFields=True,
    ) == {
        "MPS_study_23": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "tags": [
                    {"name": "MPS", "category": "Commons"},
                    {"name": "Physiological", "category": "Data Type"},
                ],
                "authz": "",
                "sites": "",
                "summary": "Motif polystyrene devices, using different flow setups. Human hepatocytes fresh isolated were used in. (3 cell model)",
                "study_url": "",
                "location": "Taylor_MPS",
                "subjects": "",
                "__manifest": "",
                "study_name": "Motif-PS-Chipshop",
                "study_type": "CC",
                "institutions": "Taylor_MPS",
                "year_awarded": "",
                "investigators": "Taylor_MPS",
                "project_title": "",
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
                "data_availability": "",
                "id": "23",
                "name": "Motif-PS-Chipshop",
                "data_group": "Taylor_MPS",
                "study_types": "CC",
                "start_date": "2015-09-30",
                "description": "Motif polystyrene devices, using different flow setups. Human hepatocytes fresh isolated were used in. (3 cell model)",
            },
        }
    }
    try:
        from mds.agg_mds.adapters import MPSAdapter

        MPSAdapter.getRemoteDataAsJson.retry.wait = wait_none()

        respx.get("http://test/timeouterror/23/",).mock(
            side_effect=httpx.TimeoutException,
        )

        get_metadata(
            "mps",
            "http://test/timeouterror",
            filters={"study_ids": [23]},
            mappings=field_mappings,
            keepOriginalFields=True,
        )

    except Exception as exc:
        assert isinstance(exc, RetryError) == True


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
                "dbgap_accession_number": "",
                "study_description": "The molecular factors involved in the development of Post-traumatic Stress Disorder (PTSD) remain poorly understood. Previous transcriptomic studies investigating the mechanisms of PTSD apply targeted approaches to identify individual genes under a cross-sectional framework lack a holistic view of the behaviours and properties of these genes at the system-level. Here we sought to apply an unsupervised gene-network-based approach to a prospective experimental design using whole-transcriptome RNA-Seq gene expression from peripheral blood leukocytes of U.S. Marines (N=188), obtained both pre- and post-deployment to conflict zones. We identified discrete groups of co-regulated genes (i.e., co-expression modules) and tested them for association to PTSD. We identified one module at both pre- and post-deployment containing putative causal signatures for PTSD development displaying an over-expression of genes enriched for functions of innate-immune response and interferon signalling (Type-I and Type-II). Importantly, these results were replicated in a second non-overlapping independent dataset of U.S. Marines (N=96), further outlining the role of innate immune and interferon signalling genes within co-expression modules to explain at least part of the causal pathophysiology for PTSD development. A second module, consequential of trauma exposure, contained PTSD resiliency signatures and an over-expression of genes involved in hemostasis and wound responsiveness suggesting that chronic levels of stress impair proper wound healing during/after exposure to the battlefield while highlighting the role of the hemostatic system as a clinical indicator of chronic-based stress. These findings provide novel insights for early preventative measures and advanced PTSD detection, which may lead to interventions that delay or perhaps abrogate the development of PTSD.\nWe used microarrays to characterize both prognostic and diagnostic molecular signatures associated to PTSD risk and PTSD status compared to control subjects.",
                "number_of_datafiles": 0,
                "investigator": "me.foo@smartsite.com",
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
    }

    expected = {
        "GSE63878": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "tags": [],
                "_subjects_count": 48,
                "dbgap_accession_number": "",
                "study_description": "The molecular factors involved in the development of Post-traumatic Stress Disorder (PTSD) remain poorly understood. Previous transcriptomic studies investigating the mechanisms of PTSD apply targeted approaches to identify individual genes under a cross-sectional framework lack a holistic view of the behaviours and properties of these genes at the system-level. Here we sought to apply an unsupervised gene-network-based approach to a prospective experimental design using whole-transcriptome RNA-Seq gene expression from peripheral blood leukocytes of U.S. Marines (N=188), obtained both pre- and post-deployment to conflict zones. We identified discrete groups of co-regulated genes (i.e., co-expression modules) and tested them for association to PTSD. We identified one module at both pre- and post-deployment containing putative causal signatures for PTSD development displaying an over-expression of genes enriched for functions of innate-immune response and interferon signalling (Type-I and Type-II). Importantly, these results were replicated in a second non-overlapping independent dataset of U.S. Marines (N=96), further outlining the role of innate immune and interferon signalling genes within co-expression modules to explain at least part of the causal pathophysiology for PTSD development. A second module, consequential of trauma exposure, contained PTSD resiliency signatures and an over-expression of genes involved in hemostasis and wound responsiveness suggesting that chronic levels of stress impair proper wound healing during/after exposure to the battlefield while highlighting the role of the hemostatic system as a clinical indicator of chronic-based stress. These findings provide novel insights for early preventative measures and advanced PTSD detection, which may lead to interventions that delay or perhaps abrogate the development of PTSD.\nWe used microarrays to characterize both prognostic and diagnostic molecular signatures associated to PTSD risk and PTSD status compared to control subjects.",
                "number_of_datafiles": 0,
                "investigator": "",
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

    get_metadata(
        "gen3",
        "http://test/ok/",
        filters=None,
        config={"batchSize": 64},
        mappings=field_mappings,
        keepOriginalFields=False,
    ) == expected

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
            },
        }
    }

    per_item_override = {"GSE63878": {"dbgap_accession_number": "dg.333344.222"}}

    get_metadata(
        "gen3",
        "http://test/ok/",
        None,
        config={"batchSize": 64},
        mappings=field_mappings,
        keepOriginalFields=False,
        perItemValues=per_item_override,
    ) == expected

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
        assert isinstance(exc, RetryError) == True


@respx.mock
def test_get_metadata_harvard_dataverse():
    dataset_json_response = r"""{
        "status": "OK",
        "data": {
            "id": 3820814,
            "identifier": "DVN/5B8YM8",
            "persistentUrl": "https://doi.org/10.7910/DVN/5B8YM8",
            "protocol": "doi",
            "authority": "10.7910",
            "publisher": "Harvard Dataverse",
            "publicationDate": "2020-04-29",
            "storageIdentifier": "s3://10.7910/DVN/5B8YM8",
            "metadataLanguage": "undefined",
            "latestVersion": {
                "id": 300092,
                "datasetId": 3820814,
                "datasetPersistentId": "doi:10.7910/DVN/5B8YM8",
                "storageIdentifier": "s3://10.7910/DVN/5B8YM8",
                "versionNumber": 24,
                "versionMinorNumber": 0,
                "versionState": "RELEASED",
                "UNF": "UNF:6:9Ygehodl5cg7JYtrkTA6bw==",
                "lastUpdateTime": "2022-05-17T19:14:19Z",
                "releaseTime": "2022-05-17T19:14:19Z",
                "createTime": "2022-01-03T23:24:51Z",
                "license": {
                    "name": "CC0 1.0",
                    "uri": "http://creativecommons.org/publicdomain/zero/1.0"
                },
                "fileAccessRequest": false,
                "metadataBlocks": {
                    "citation": {
                        "displayName": "Citation Metadata",
                        "name": "citation",
                        "fields": [
                            {
                                "typeName": "title",
                                "multiple": false,
                                "typeClass": "primitive",
                                "value": "US Metropolitan Daily Cases with Basemap"
                            },
                            {
                                "typeName": "author",
                                "multiple": true,
                                "typeClass": "compound",
                                "value": [
                                    {
                                        "authorName": {
                                            "typeName": "authorName",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "China Data Lab"
                                        },
                                        "authorAffiliation": {
                                            "typeName": "authorAffiliation",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "China Data Lab"
                                        }
                                    }
                                ]
                            },
                            {
                                "typeName": "datasetContact",
                                "multiple": true,
                                "typeClass": "compound",
                                "value": [
                                    {
                                        "datasetContactName": {
                                            "typeName": "datasetContactName",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "Hanchen Yu"
                                        },
                                        "datasetContactAffiliation": {
                                            "typeName": "datasetContactAffiliation",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "China Data Lab"
                                        },
                                        "datasetContactEmail": {
                                            "typeName": "datasetContactEmail",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "hanchenyu@fas.harvard.edu"
                                        }
                                    }
                                ]
                            },
                            {
                                "typeName": "dsDescription",
                                "multiple": true,
                                "typeClass": "compound",
                                "value": [
                                    {
                                        "dsDescriptionValue": {
                                            "typeName": "dsDescriptionValue",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "Updated to May 17, 2022. Metropolitan level daily cases. There are 926 metropolitans except for the areas in Perto Rico."
                                        }
                                    }
                                ]
                            },
                            {
                                "typeName": "subject",
                                "multiple": true,
                                "typeClass": "controlledVocabulary",
                                "value": [
                                    "Earth and Environmental Sciences",
                                    "Social Sciences"
                                ]
                            },
                            {
                                "typeName": "publication",
                                "multiple": true,
                                "typeClass": "compound",
                                "value": [
                                    {
                                        "publicationCitation": {
                                            "typeName": "publicationCitation",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "Hu, T., Guan, W., Zhu, X., Shao, Y., ... & Bao, S. (2020). Building an Open Resources Repository for COVID-19 Research, Data and Information Management, 4(3), 130-147. doi: https://doi.org/10.2478/dim-2020-0012"
                                        }
                                    }
                                ]
                            },
                            {
                                "typeName": "contributor",
                                "multiple": true,
                                "typeClass": "compound",
                                "value": [
                                    {
                                        "contributorType": {
                                            "typeName": "contributorType",
                                            "multiple": false,
                                            "typeClass": "controlledVocabulary",
                                            "value": "Data Manager"
                                        },
                                        "contributorName": {
                                            "typeName": "contributorName",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "Hu, Tao"
                                        }
                                    },
                                    {
                                        "contributorType": {
                                            "typeName": "contributorType",
                                            "multiple": false,
                                            "typeClass": "controlledVocabulary",
                                            "value": "Data Collector"
                                        },
                                        "contributorName": {
                                            "typeName": "contributorName",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "Hu, Tao"
                                        }
                                    }
                                ]
                            },
                            {
                                "typeName": "grantNumber",
                                "multiple": true,
                                "typeClass": "compound",
                                "value": [
                                    {
                                        "grantNumberAgency": {
                                            "typeName": "grantNumberAgency",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "NSF"
                                        },
                                        "grantNumberValue": {
                                            "typeName": "grantNumberValue",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "2027540"
                                        }
                                    },
                                    {
                                        "grantNumberAgency": {
                                            "typeName": "grantNumberAgency",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "NSF"
                                        },
                                        "grantNumberValue": {
                                            "typeName": "grantNumberValue",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "1841403"
                                        }
                                    }
                                ]
                            },
                            {
                                "typeName": "depositor",
                                "multiple": false,
                                "typeClass": "primitive",
                                "value": "China, Data Lab"
                            },
                            {
                                "typeName": "dateOfDeposit",
                                "multiple": false,
                                "typeClass": "primitive",
                                "value": "2020-04-29"
                            }
                        ]
                    },
                    "geospatial": {
                        "displayName": "Geospatial Metadata",
                        "name": "geospatial",
                        "fields": []
                    }
                },
                "files": [
                    {
                        "label": "us_metro_confirmed_cases_cdl.tab",
                        "restricted": false,
                        "version": 3,
                        "datasetVersionId": 300092,
                        "dataFile": {
                            "id": 6297263,
                            "persistentId": "",
                            "pidURL": "",
                            "filename": "us_metro_confirmed_cases_cdl.tab",
                            "contentType": "text/tab-separated-values",
                            "filesize": 3953412,
                            "storageIdentifier": "s3://dvn-cloud:180d366a3da-80ca5c2acd1b",
                            "originalFileFormat": "text/csv",
                            "originalFormatLabel": "Comma Separated Values",
                            "originalFileSize": 3961820,
                            "originalFileName": "us_metro_confirmed_cases_cdl.csv",
                            "UNF": "UNF:6:w715RbMgdXAjmDiwdGNv+g==",
                            "rootDataFileId": -1,
                            "md5": "ef0d67774caa8f1bcd7bcce4e8d62396",
                            "checksum": {
                                "type": "MD5",
                                "value": "ef0d67774caa8f1bcd7bcce4e8d62396"
                            },
                            "creationDate": "2022-05-17"
                        }
                    }
                ]
            }
        }
    }"""

    dataset_json_different_keys_response = r"""{
        "status": "OK",
        "data": {
            "id": 3820814,
            "identifier": "DVN/5B8YM8",
            "persistentUrl": "https://doi.org/10.7910/DVN/5B8YM8",
            "protocol": "doi",
            "authority": "10.7910",
            "publisher": "Harvard Dataverse",
            "publicationDate": "2020-04-29",
            "storageIdentifier": "s3://10.7910/DVN/5B8YM8",
            "metadataLanguage": "undefined",
            "latest_version": {
                "id": 300092,
                "datasetId": 3820814,
                "datasetPersistentId": "doi:10.7910/DVN/5B8YM8",
                "storageIdentifier": "s3://10.7910/DVN/5B8YM8",
                "versionNumber": 24,
                "versionMinorNumber": 0,
                "versionState": "RELEASED",
                "UNF": "UNF:6:9Ygehodl5cg7JYtrkTA6bw==",
                "lastUpdateTime": "2022-05-17T19:14:19Z",
                "releaseTime": "2022-05-17T19:14:19Z",
                "createTime": "2022-01-03T23:24:51Z",
                "license": {
                    "name": "CC0 1.0",
                    "uri": "http://creativecommons.org/publicdomain/zero/1.0"
                },
                "fileAccessRequest": false,
                "metadata_blocks": {
                    "citation": {
                        "displayName": "Citation Metadata",
                        "name": "citation",
                        "fields": [
                            {
                                "typeName": "title",
                                "multiple": false,
                                "typeClass": "primitive",
                                "value": "US Metropolitan Daily Cases with Basemap"
                            },
                            {
                                "typeName": "author",
                                "multiple": true,
                                "typeClass": "compound",
                                "value": [
                                    {
                                        "authorName": {
                                            "typeName": "authorName",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "China Data Lab"
                                        },
                                        "authorAffiliation": {
                                            "typeName": "authorAffiliation",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "China Data Lab"
                                        }
                                    }
                                ]
                            },
                            {
                                "typeName": "datasetContact",
                                "multiple": true,
                                "typeClass": "compound",
                                "value": [
                                    {
                                        "datasetContactName": {
                                            "typeName": "datasetContactName",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "Hanchen Yu"
                                        },
                                        "datasetContactAffiliation": {
                                            "typeName": "datasetContactAffiliation",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "China Data Lab"
                                        },
                                        "datasetContactEmail": {
                                            "typeName": "datasetContactEmail",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "hanchenyu@fas.harvard.edu"
                                        }
                                    }
                                ]
                            },
                            {
                                "typeName": "dsDescription",
                                "multiple": true,
                                "typeClass": "compound",
                                "value": [
                                    {
                                        "dsDescriptionValue": {
                                            "typeName": "dsDescriptionValue",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "Updated to May 17, 2022. Metropolitan level daily cases. There are 926 metropolitans except for the areas in Perto Rico."
                                        }
                                    }
                                ]
                            },
                            {
                                "typeName": "subject",
                                "multiple": true,
                                "typeClass": "controlledVocabulary",
                                "value": [
                                    "Earth and Environmental Sciences",
                                    "Social Sciences"
                                ]
                            },
                            {
                                "typeName": "publication",
                                "multiple": true,
                                "typeClass": "compound",
                                "value": [
                                    {
                                        "publicationCitation": {
                                            "typeName": "publicationCitation",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "Hu, T., Guan, W., Zhu, X., Shao, Y., ... & Bao, S. (2020). Building an Open Resources Repository for COVID-19 Research, Data and Information Management, 4(3), 130-147. doi: https://doi.org/10.2478/dim-2020-0012"
                                        }
                                    }
                                ]
                            },
                            {
                                "typeName": "contributor",
                                "multiple": true,
                                "typeClass": "compound",
                                "value": [
                                    {
                                        "contributorType": {
                                            "typeName": "contributorType",
                                            "multiple": false,
                                            "typeClass": "controlledVocabulary",
                                            "value": "Data Manager"
                                        },
                                        "contributorName": {
                                            "typeName": "contributorName",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "Hu, Tao"
                                        }
                                    },
                                    {
                                        "contributorType": {
                                            "typeName": "contributorType",
                                            "multiple": false,
                                            "typeClass": "controlledVocabulary",
                                            "value": "Data Collector"
                                        },
                                        "contributorName": {
                                            "typeName": "contributorName",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "Hu, Tao"
                                        }
                                    }
                                ]
                            },
                            {
                                "typeName": "grantNumber",
                                "multiple": true,
                                "typeClass": "compound",
                                "value": [
                                    {
                                        "grantNumberAgency": {
                                            "typeName": "grantNumberAgency",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "NSF"
                                        },
                                        "grantNumberValue": {
                                            "typeName": "grantNumberValue",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "2027540"
                                        }
                                    },
                                    {
                                        "grantNumberAgency": {
                                            "typeName": "grantNumberAgency",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "NSF"
                                        },
                                        "grantNumberValue": {
                                            "typeName": "grantNumberValue",
                                            "multiple": false,
                                            "typeClass": "primitive",
                                            "value": "1841403"
                                        }
                                    }
                                ]
                            },
                            {
                                "typeName": "depositor",
                                "multiple": false,
                                "typeClass": "primitive",
                                "value": "China, Data Lab"
                            },
                            {
                                "typeName": "dateOfDeposit",
                                "multiple": false,
                                "typeClass": "primitive",
                                "value": "2020-04-29"
                            }
                        ]
                    },
                    "geospatial": {
                        "displayName": "Geospatial Metadata",
                        "name": "geospatial",
                        "fields": []
                    }
                },
                "files": [
                    {
                        "label": "us_metro_confirmed_cases_cdl.tab",
                        "restricted": false,
                        "version": 3,
                        "datasetVersionId": 300092,
                        "dataFile": {
                            "id": 6297263,
                            "persistentId": "",
                            "pidURL": "",
                            "filename": "us_metro_confirmed_cases_cdl.tab",
                            "contentType": "text/tab-separated-values",
                            "filesize": 3953412,
                            "storageIdentifier": "s3://dvn-cloud:180d366a3da-80ca5c2acd1b",
                            "originalFileFormat": "text/csv",
                            "originalFormatLabel": "Comma Separated Values",
                            "originalFileSize": 3961820,
                            "originalFileName": "us_metro_confirmed_cases_cdl.csv",
                            "UNF": "UNF:6:w715RbMgdXAjmDiwdGNv+g==",
                            "rootDataFileId": -1,
                            "md5": "ef0d67774caa8f1bcd7bcce4e8d62396",
                            "checksum": {
                                "type": "MD5",
                                "value": "ef0d67774caa8f1bcd7bcce4e8d62396"
                            },
                            "creationDate": "2022-05-17"
                        }
                    }
                ]
            }
        }
    }"""

    file_ddi_response = """<?xml version='1.0' encoding='UTF-8'?>
    <codeBook xmlns="http://www.icpsr.umich.edu/DDI" version="2.0">
        <stdyDscr>
            <citation>
                <titlStmt>
                    <titl>US Metropolitan Daily Cases with Basemap</titl>
                    <IDNo agency="doi">10.7910/DVN/5B8YM8</IDNo>
                </titlStmt>
                <rspStmt>
                    <AuthEnty>China Data Lab</AuthEnty>
                </rspStmt>
                <biblCit>China Data Lab, 2020, "US Metropolitan Daily Cases with Basemap", https://doi.org/10.7910/DVN/5B8YM8, Harvard Dataverse, V24, UNF:6:9Ygehodl5cg7JYtrkTA6bw== [fileUNF]</biblCit>
            </citation>
        </stdyDscr>
        <fileDscr ID="f6297263">
            <fileTxt>
                <fileName>us_metro_confirmed_cases_cdl.tab</fileName>
                <dimensns>
                    <caseQnty>942</caseQnty>
                    <varQnty>852</varQnty>
                </dimensns>
                <fileType>text/tab-separated-values</fileType>
            </fileTxt>
            <notes level="file" type="VDC:UNF" subject="Universal Numeric Fingerprint">UNF:6:w715RbMgdXAjmDiwdGNv+g==</notes>
        </fileDscr>
        <dataDscr>
            <var ID="v28336577" name="POP90" intrvl="discrete">
                <location fileid="f6297263"/>
                <labl level="variable">POP90</labl>
                <sumStat type="mode">.</sumStat>
                <sumStat type="min">10089.0</sumStat>
                <sumStat type="vald">942.0</sumStat>
                <sumStat type="invd">0.0</sumStat>
                <sumStat type="max">1.6835336E7</sumStat>
                <sumStat type="stdev">863685.1810810249</sumStat>
                <sumStat type="mean">245006.8174097664</sumStat>
                <sumStat type="medn">62229.0</sumStat>
                <varFormat type="numeric"/>
                <notes subject="Universal Numeric Fingerprint" level="variable" type="VDC:UNF">UNF:6:rrZvx0sccW3/T39TpNZkww==</notes>
            </var>
            <var ID="v28336083" name="POP80" intrvl="discrete">
                <location fileid="f6297263"/>
                <labl level="variable">POP80</labl>
                <sumStat type="medn">58890.0</sumStat>
                <sumStat type="stdev">793757.747968027</sumStat>
                <sumStat type="mode">.</sumStat>
                <sumStat type="vald">942.0</sumStat>
                <sumStat type="invd">0.0</sumStat>
                <sumStat type="mean">219736.50849256906</sumStat>
                <sumStat type="min">1486.0</sumStat>
                <sumStat type="max">1.6313732E7</sumStat>
                <varFormat type="numeric"/>
                <notes subject="Universal Numeric Fingerprint" level="variable" type="VDC:UNF">UNF:6:/gE+Jgr2RolrwkeE+O0log==</notes>
            </var>
            <var ID="v28336475" name="POP70" intrvl="discrete">
                <location fileid="f6297263"/>
                <labl level="variable">POP70</labl>
                <sumStat type="vald">942.0</sumStat>
                <sumStat type="invd">0.0</sumStat>
                <sumStat type="mode">.</sumStat>
                <sumStat type="max">1.7009092E7</sumStat>
                <sumStat type="mean">196906.501061571</sumStat>
                <sumStat type="min">0.0</sumStat>
                <sumStat type="medn">50533.5</sumStat>
                <sumStat type="stdev">777648.8670546024</sumStat>
                <varFormat type="numeric"/>
                <notes subject="Universal Numeric Fingerprint" level="variable" type="VDC:UNF">UNF:6:fhyMqy/xsabC5iK9w885Hw==</notes>
            </var>
            <var ID="v28336321" name="POP10" intrvl="discrete">
                <location fileid="f6297263"/>
                <labl level="variable">POP10</labl>
                <sumStat type="mean">307071.45966029697</sumStat>
                <sumStat type="stdev">1034155.0228777333</sumStat>
                <sumStat type="min">12093.0</sumStat>
                <sumStat type="max">1.8897109E7</sumStat>
                <sumStat type="invd">0.0</sumStat>
                <sumStat type="medn">74765.5</sumStat>
                <sumStat type="vald">942.0</sumStat>
                <sumStat type="mode">.</sumStat>
                <varFormat type="numeric"/>
                <notes subject="Universal Numeric Fingerprint" level="variable" type="VDC:UNF">UNF:6:4uEu5qWUDCdmXqi8flG1MQ==</notes>
            </var>
            <var ID="v28336509" name="POP00" intrvl="discrete">
                <location fileid="f6297263"/>
                <labl level="variable">POP00</labl>
                <sumStat type="mode">.</sumStat>
                <sumStat type="mean">278420.1868365179</sumStat>
                <sumStat type="stdev">964415.2918384229</sumStat>
                <sumStat type="max">1.832299E7</sumStat>
                <sumStat type="min">13004.0</sumStat>
                <sumStat type="medn">69472.5</sumStat>
                <sumStat type="invd">0.0</sumStat>
                <sumStat type="vald">942.0</sumStat>
                <varFormat type="numeric"/>
                <notes subject="Universal Numeric Fingerprint" level="variable" type="VDC:UNF">UNF:6:tyCEHtT+8GdCOTpJpYvAJg==</notes>
            </var>
            <var ID="v28336299" name="Metropolitan" intrvl="discrete">
                <location fileid="f6297263"/>
                <labl level="variable">Metropolitan</labl>
                <varFormat type="character"/>
                <notes subject="Universal Numeric Fingerprint" level="variable" type="VDC:UNF">UNF:6:mphqnaLCTtmU+tpSoBgFHw==</notes>
            </var>
            <var ID="v28336477" name="Metro_ID" intrvl="discrete">
                <location fileid="f6297263"/>
                <labl level="variable">Metro_ID</labl>
                <sumStat type="max">49780.0</sumStat>
                <sumStat type="mode">.</sumStat>
                <sumStat type="stdev">11370.289276051668</sumStat>
                <sumStat type="invd">0.0</sumStat>
                <sumStat type="min">10020.0</sumStat>
                <sumStat type="medn">29800.0</sumStat>
                <sumStat type="mean">29761.56050955417</sumStat>
                <sumStat type="vald">942.0</sumStat>
                <varFormat type="numeric"/>
                <notes subject="Universal Numeric Fingerprint" level="variable" type="VDC:UNF">UNF:6:m28+WicbC3+/UcbML38hgQ==</notes>
            </var>
            <var ID="v28336208" name="2022-04-11" intrvl="discrete">
                <location fileid="f6297263"/>
                <labl level="variable">2022-04-11</labl>
                <sumStat type="stdev">270083.02026017866</sumStat>
                <sumStat type="min">1757.0</sumStat>
                <sumStat type="mode">.</sumStat>
                <sumStat type="mean">77844.43312101916</sumStat>
                <sumStat type="invd">0.0</sumStat>
                <sumStat type="vald">942.0</sumStat>
                <sumStat type="max">5205145.0</sumStat>
                <sumStat type="medn">19037.0</sumStat>
                <varFormat type="numeric"/>
                <notes subject="Universal Numeric Fingerprint" level="variable" type="VDC:UNF">UNF:6:FmvYl83snrKvDdkjUsZegg==</notes>
            </var>
            <var ID="v28336064" name="2022-04-10" intrvl="discrete">
                <location fileid="f6297263"/>
                <labl level="variable">2022-04-10</labl>
                <sumStat type="mean">77797.73460721863</sumStat>
                <sumStat type="stdev">269746.2378437367</sumStat>
                <sumStat type="invd">0.0</sumStat>
                <sumStat type="min">1757.0</sumStat>
                <sumStat type="mode">.</sumStat>
                <sumStat type="medn">19006.5</sumStat>
                <sumStat type="max">5191684.0</sumStat>
                <sumStat type="vald">942.0</sumStat>
                <varFormat type="numeric"/>
                <notes subject="Universal Numeric Fingerprint" level="variable" type="VDC:UNF">UNF:6:6d+rPXgszAck4Ad3bLkVlQ==</notes>
            </var>
        </dataDscr>
    </codeBook>"""

    file_single_variable_ddi_response = """<?xml version='1.0' encoding='UTF-8'?>
    <codeBook xmlns="http://www.icpsr.umich.edu/DDI" version="2.0">
        <stdyDscr>
            <citation>
                <titlStmt>
                    <titl>US Metropolitan Daily Cases with Basemap</titl>
                    <IDNo agency="doi">10.7910/DVN/5B8YM8</IDNo>
                </titlStmt>
                <rspStmt>
                    <AuthEnty>China Data Lab</AuthEnty>
                </rspStmt>
                <biblCit>China Data Lab, 2020, "US Metropolitan Daily Cases with Basemap", https://doi.org/10.7910/DVN/5B8YM8, Harvard Dataverse, V24, UNF:6:9Ygehodl5cg7JYtrkTA6bw== [fileUNF]</biblCit>
            </citation>
        </stdyDscr>
        <fileDscr ID="f6297263">
            <fileTxt>
                <fileName>us_metro_confirmed_cases_cdl.tab</fileName>
                <dimensns>
                    <caseQnty>942</caseQnty>
                    <varQnty>852</varQnty>
                </dimensns>
                <fileType>text/tab-separated-values</fileType>
            </fileTxt>
            <notes level="file" type="VDC:UNF" subject="Universal Numeric Fingerprint">UNF:6:w715RbMgdXAjmDiwdGNv+g==</notes>
        </fileDscr>
        <dataDscr>
            <var ID="v28336577" name="POP90" intrvl="discrete">
                <location fileid="f6297263"/>
                <labl level="variable">POP90</labl>
                <sumStat type="mode">.</sumStat>
                <sumStat type="min">10089.0</sumStat>
                <sumStat type="vald">942.0</sumStat>
                <sumStat type="invd">0.0</sumStat>
                <sumStat type="max">1.6835336E7</sumStat>
                <sumStat type="stdev">863685.1810810249</sumStat>
                <sumStat type="mean">245006.8174097664</sumStat>
                <sumStat type="medn">62229.0</sumStat>
                <varFormat type="numeric"/>
                <notes subject="Universal Numeric Fingerprint" level="variable" type="VDC:UNF">UNF:6:rrZvx0sccW3/T39TpNZkww==</notes>
            </var>
        </dataDscr>
    </codeBook>"""

    field_mappings = {
        "tags": [],
        "authz": "",
        "sites": "",
        "summary": "path:dsDescriptionValue",
        "study_description_summary": "path:dsDescriptionValue",
        "study_url": "path:url",
        "location": "",
        "subjects": "path:subject",
        "__manifest": [],
        "study_name": "path:title",
        "study_name_title": "path:title",
        "study_type": "",
        "institutions": "path:datasetContactAffiliation",
        "year_awarded": "",
        "investigators": "path:datasetContactName",
        "investigators_name": "path:datasetContactName",
        "project_title": "",
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
        "data_availability": "path:data_availability",
    }

    expected_response = {
        "doi:10.7910/DVN/5B8YM8": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "tags": [],
                "authz": "",
                "sites": "",
                "summary": "Updated to May 17, 2022. Metropolitan level daily cases. There are 926 metropolitans except for the areas in Perto Rico.",
                "study_description_summary": "Updated to May 17, 2022. Metropolitan level daily cases. There are 926 metropolitans except for the areas in Perto Rico.",
                "study_url": "https://doi.org/10.7910/DVN/5B8YM8",
                "location": "",
                "subjects": "Earth and Environmental Sciences, Social Sciences",
                "__manifest": [],
                "study_name": "US Metropolitan Daily Cases with Basemap",
                "study_name_title": "US Metropolitan Daily Cases with Basemap",
                "study_type": "",
                "institutions": "China Data Lab",
                "year_awarded": "",
                "investigators": "Hanchen Yu",
                "investigators_name": "Hanchen Yu",
                "project_title": "",
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
                "data_availability": "available",
                "data_dictionary": {
                    "us_metro_confirmed_cases_cdl.tab": [
                        {
                            "name": "POP90",
                            "label": "POP90",
                            "interval": "discrete",
                            "type": "numeric",
                        },
                        {
                            "name": "POP80",
                            "label": "POP80",
                            "interval": "discrete",
                            "type": "numeric",
                        },
                        {
                            "name": "POP70",
                            "label": "POP70",
                            "interval": "discrete",
                            "type": "numeric",
                        },
                        {
                            "name": "POP10",
                            "label": "POP10",
                            "interval": "discrete",
                            "type": "numeric",
                        },
                        {
                            "name": "POP00",
                            "label": "POP00",
                            "interval": "discrete",
                            "type": "numeric",
                        },
                        {
                            "name": "Metropolitan",
                            "label": "Metropolitan",
                            "interval": "discrete",
                            "type": "character",
                        },
                        {
                            "name": "Metro_ID",
                            "label": "Metro_ID",
                            "interval": "discrete",
                            "type": "numeric",
                        },
                        {
                            "name": "2022-04-11",
                            "label": "2022-04-11",
                            "interval": "discrete",
                            "type": "numeric",
                        },
                        {
                            "name": "2022-04-10",
                            "label": "2022-04-10",
                            "interval": "discrete",
                            "type": "numeric",
                        },
                    ]
                },
            },
        }
    }

    expected_single_variable_response = {
        "doi:10.7910/DVN/5B8YM8": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "tags": [],
                "authz": "",
                "sites": "",
                "summary": "Updated to May 17, 2022. Metropolitan level daily cases. There are 926 metropolitans except for the areas in Perto Rico.",
                "study_description_summary": "Updated to May 17, 2022. Metropolitan level daily cases. There are 926 metropolitans except for the areas in Perto Rico.",
                "study_url": "https://doi.org/10.7910/DVN/5B8YM8",
                "location": "",
                "subjects": "Earth and Environmental Sciences, Social Sciences",
                "__manifest": [],
                "study_name": "US Metropolitan Daily Cases with Basemap",
                "study_name_title": "US Metropolitan Daily Cases with Basemap",
                "study_type": "",
                "institutions": "China Data Lab",
                "year_awarded": "",
                "investigators": "Hanchen Yu",
                "investigators_name": "Hanchen Yu",
                "project_title": "",
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
                "data_availability": "available",
                "data_dictionary": {
                    "us_metro_confirmed_cases_cdl.tab": [
                        {
                            "name": "POP90",
                            "label": "POP90",
                            "interval": "discrete",
                            "type": "numeric",
                        }
                    ]
                },
            },
        }
    }

    # failed calls
    respx.get(
        "http://test/ok/datasets/:persistentId/?persistentId=doi:10.7910/DVN/5B8YM8"
    ).mock(return_value=httpx.Response(status_code=200, content=dataset_json_response))

    respx.get("http://test/ok/access/datafile/6297263/metadata/ddi").mock(
        return_value=httpx.Response(status_code=200, content=file_ddi_response)
    )

    assert get_metadata("havard_dataverse", "http://test/ok", filters=None) == {}

    assert (
        get_metadata(
            "harvard_dataverse",
            None,
            filters={"persistent_ids": ["doi:10.7910/DVN/5B8YM8"]},
        )
        == {}
    )

    assert (
        get_metadata(
            "harvard_dataverse",
            "http://test/ok",
            filters=None,
        )
        == {}
    )

    # valid call
    assert (
        get_metadata(
            "harvard_dataverse",
            "http://test/ok",
            filters={"persistent_ids": ["doi:10.7910/DVN/5B8YM8"]},
            mappings=field_mappings,
        )
        == expected_response
    )

    # valid single variable call
    respx.get(
        "http://test/single_variable/datasets/:persistentId/?persistentId=doi:10.7910/DVN/5B8YM8"
    ).mock(return_value=httpx.Response(status_code=200, content=dataset_json_response))

    respx.get("http://test/single_variable/access/datafile/6297263/metadata/ddi").mock(
        return_value=httpx.Response(
            status_code=200, content=file_single_variable_ddi_response
        )
    )

    assert (
        get_metadata(
            "harvard_dataverse",
            "http://test/single_variable",
            filters={"persistent_ids": ["doi:10.7910/DVN/5B8YM8"]},
            mappings=field_mappings,
        )
        == expected_single_variable_response
    )

    # invalid responses
    respx.get(
        "http://test/invalid_dataset_response/datasets/:persistentId/?persistentId=doi:10.7910/DVN/5B8YM8"
    ).mock(return_value=httpx.Response(status_code=200, json={"status": "ok"}))

    assert (
        get_metadata(
            "harvard_dataverse",
            "http://test/invalid_dataset_response",
            filters={"persistent_ids": ["doi:10.7910/DVN/5B8YM8"]},
            mappings=field_mappings,
        )
        == {}
    )

    respx.get(
        "http://test/err404/datasets/:persistentId/?persistentId=doi:10.7910/DVN/5B8YM8"
    ).mock(return_value=httpx.Response(status_code=404, json={}))

    assert (
        get_metadata(
            "harvard_dataverse",
            "http://test/err404",
            filters={"persistent_ids": ["doi:10.7910/DVN/5B8YM8"]},
            mappings=field_mappings,
        )
        == {}
    )

    # Incorrect keys expected in adapter class
    respx.get(
        "http://test/different_keys/datasets/:persistentId/?persistentId=doi:10.7910/DVN/5B8YM8"
    ).mock(
        return_value=httpx.Response(
            status_code=200, json=dataset_json_different_keys_response
        )
    )

    assert (
        get_metadata(
            "harvard_dataverse",
            "http://test/different_keys",
            filters={"persistent_ids": ["doi:10.7910/DVN/5B8YM8"]},
            mappings=field_mappings,
        )
        == {}
    )

    try:
        from mds.agg_mds.adapters import HarvardDataverse

        HarvardDataverse.getRemoteDataAsJson.retry.wait = wait_none()

        respx.get(
            "http://test/timeouterror/datasets/:persistentId/?persistentId=doi:10.7910/DVN/5B8YM8"
        ).mock(side_effect=httpx.TimeoutException)

        get_metadata(
            "harvard_dataverse",
            "http://test/timeouterror",
            filters={"persistent_ids": ["doi:10.7910/DVN/5B8YM8"]},
            mappings=field_mappings,
        )
    except Exception as exc:
        assert isinstance(exc, RetryError) == True


def test_missing_adapter():
    try:
        get_metadata("notAKnownAdapter", "http://test/ok/", None, None)
    except Exception as err:
        assert isinstance(err, ValueError) == True


def test_json_path_expression():
    sample1 = {
        "study1": {
            "study_description_summary": "This is a summary",
            "id": "2334.5.555ad",
            "contributors": ["Bilbo Baggins"],
            "datasets": ["results1.csv", "results2.csv", "results3.csv"],
        },
        "study3": {
            "study_description_summary": "This is another summary",
            "id": "333.33222.ad",
            "datasets": ["results4.csv", "results5.csv", "results6.csv"],
        },
    }

    assert (
        get_json_path_value("study1.study_description_summary", sample1)
        == "This is a summary"
    )

    # test non existent path
    assert get_json_path_value("study2.study_description_summary", sample1) == ""

    # test bad path
    assert get_json_path_value(".contributors", sample1) == ""

    # test single array
    assert get_json_path_value("study1.contributors", sample1) == ["Bilbo Baggins"]

    # test array whose length is greater than 1
    assert get_json_path_value("*.datasets", sample1) == [
        ["results1.csv", "results2.csv", "results3.csv"],
        ["results4.csv", "results5.csv", "results6.csv"],
    ]
