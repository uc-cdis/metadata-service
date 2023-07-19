import respx
import httpx
import json

from mds.agg_mds.adapters import get_metadata
from tenacity import RetryError, wait_none


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
                            "md5": "ef0d6777",
                            "checksum": {
                                "type": "MD5",
                                "value": "ef0d67774"
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
                            "md5": "ef0d6777",
                            "checksum": {
                                "type": "MD5",
                                "value": "ef0d6777"
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
        "investigators": "path:datasetContactName",
        "investigators_name": "path:datasetContactName",
        "advSearchFilters": [],
        "data_availability": "path:data_availability",
        "study_metadata.minimal_info.alternative_study_name": "path:title",
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
                "investigators": "Hanchen Yu",
                "investigators_name": "Hanchen Yu",
                "advSearchFilters": [],
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
                "study_metadata": {
                    "minimal_info": {
                        "alternative_study_name": "US Metropolitan Daily Cases with Basemap"
                    },
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
                "investigators": "Hanchen Yu",
                "investigators_name": "Hanchen Yu",
                "advSearchFilters": [],
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
                "study_metadata": {
                    "minimal_info": {
                        "alternative_study_name": "US Metropolitan Daily Cases with Basemap"
                    },
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

    respx.get(
        "http://test/ok/datasets/:persistentId/?persistentId=doi:10.7910/DVN/5B8YM8"
        "http://test/ok/datasets/:persistentId/?persistentId=doi:10.7910/DVN/5B8YM8"
    ).mock(
        return_value=httpx.Response(
            status_code=200, json=json.loads(dataset_json_response)
        )
    )

    respx.get("http://test/ok/access/datafile/6297263/metadata/ddi").mock(
        return_value=httpx.Response(status_code=200, text=file_ddi_response)
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
        assert isinstance(exc, RetryError) is True
