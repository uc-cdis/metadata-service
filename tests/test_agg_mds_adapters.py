import respx
import json
from mds.agg_mds.adapters import get_metadata


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

    respx.get(
        "http://test/ok?verb=GetRecord&metadataPrefix=oai_dc&identifier=6425",
        status_code=200,
        content=xml_response,
    )

    assert get_metadata("icpsr", "http://test/ok", filters=None) == {}

    assert get_metadata("icpsr", "http://test/ok", filters={"study_ids": [6425]}) == {
        "10.3886/ICPSR06425.v1": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "authz": "",
                "contributor": "Inter-university Consortium for Political and Social Research [distributor]",
                "coverage": "1990",
                "creator": "United States. Bureau of the Census",
                "date": "03-16-1995",
                "description": "These data describe the geographic relationships of the 103rd congressional districts to selected governmental and statistical geographic entities for the entire United States, American Samoa, Guam, Puerto Rico, and the Virgin Islands. Each record represents a census geographic tabulation unit (GTUB), a unique combination of geographic codes expressing specific geographic relationships. This file provides the following information: state, congressional district, county and county subdivision, place, American Indian/Alaska Native area, urbanized area, urban/rural descriptor, and Metropolitan Statistical Area/Primary Metropolitan Statistical Area (MSA/PMSA).",
                "identifier": "10.3886/ICPSR06425.v1",
                "source": "equivalency files and listings submitted by the appropriate agency or official within each state, usually from the Secretary of State's Office",
                "tags": [],
                "title": "103rd Congressional District Geographic Entity File, 1990: [United States]",
                "type": "administrative records data",
            },
        }
    }


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
                    "PrimaryOutcomeTimeFrame":"the duration of hospital stay, an expected average of 2 weeks"
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
                    "OtherOutcomeTimeFrame":"the duration of hospital stay, an expected average of 2 weeks"
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
    respx.get(
        "http://test/ok?expr=heart+attack&fmt=json&min_rnk=1&max_rnk=1",
        status_code=200,
        content=json.loads(json_response),
        content_type="text/plain;charset=UTF-8",
    )

    assert get_metadata("clinicaltrials", "http://test/ok", filters=None) == {}

    assert get_metadata(
        "clinicaltrials",
        "http://test/ok",
        filters={"term": "heart attack", "maxItems": 1},
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
                        "PrimaryOutcomeTimeFrame": "the duration of hospital stay, an expected average of 2 weeks",
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
                        "OtherOutcomeTimeFrame": "the duration of hospital stay, an expected average of 2 weeks",
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
                "authz": "",
                "tags": [],
            },
        }
    }
