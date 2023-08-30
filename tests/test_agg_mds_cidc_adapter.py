import respx
import httpx

from mds.agg_mds.adapters import get_metadata


@respx.mock
def test_get_metadata_cidc():
    json_response = r"""
    {
      "code": 200,
      "collections": [
        {
          "cancer_type": "Prostate Cancer",
          "collection_id": "tcga_prad",
          "date_updated": "2022-10-10",
          "description": "<div><strong>Note:&nbsp;This collection has special restrictions on its usage. See <a href=\"https: //wiki.cancerimagingarchive.net/x/c4hF\" target=\"_blank\">Data Usage Policies and Restrictions</a>.</strong></p>\n<div>\n\t&nbsp;</p>\n<div>\n\t<span>The <a href=\"http: //imaging.cancer.gov/\" target=\"_blank\"><u>Cancer Imaging Program (CIP)</u></a></span><span>&thinsp;</span><span> is working directly with primary investigators from institutes participating in TCGA to obtain and load images relating to the genomic, clinical, and pathological data being stored within the <a href=\"http: //tcga-data.nci.nih.gov/\" target=\"_blank\">TCGA Data Portal</a>.&nbsp;Currently this image collection of prostate adenocarcinoma (PRAD) patients can be matched by each unique case identifier with the extensive gene and expression data of the same case from The Cancer Genome Atlas Data Portal to research the link between clinical phenome and tissue genome.&nbsp;<br />\n\t</span></p>\n<div>\n\t&nbsp;</p>\n<div>\n\t<span>Please see the <span><a href=\"https: //wiki.cancerimagingarchive.net/x/tgpp\" target=\"_blank\">TCGA-PRAD</a></span> wiki page to learn more about the images and to obtain any supporting metadata for this collection.</span></p>",
          "doi": "10.7937/k9/tcia.2016.yxoglm4y",
          "image_types": "CT, MR, PT, SM",
          "location": "Prostate",
          "species": "Human",
          "subject_count": 500,
          "supporting_data": "Clinical, Genomics"
        },
        {
          "cancer_type": "Bladder Endothelial Carcinoma",
          "collection_id": "tcga_blca",
          "date_updated": "2022-10-10",
          "description": "<p>\n\tThe Cancer Genome Atlas-Bladder Endothelial Carcinoma (TCGA-BLCA) data collection is part of a larger effort to enhance the TCGA http://cancergenome.nih.gov/ data set with characterized radiological images. The Cancer Imaging Program (CIP), with the cooperation of several of the TCGA tissue-contributing institutions, has archived a large portion of the radiological images of the genetically-analyzed BLCA cases.</p>\n<p>\n\tPlease see the <a href=\"https: //wiki.cancerimagingarchive.net/display/Public/TCGA-BLCA\" target=\"_blank\">TCGA-BLCA</a> wiki page to learn more about the images and to obtain any supporting metadata for this collection.</p>\n",
          "doi": "10.7937/k9/tcia.2016.8lng8xdr",
          "image_types": "CR, CT, DX, MR, PT, SM",
          "location": "Bladder",
          "species": "Human",
          "subject_count": 412,
          "supporting_data": "Clinical, Genomics"
        },
        {
          "cancer_type": "Uterine Corpus Endometrial Carcinoma",
          "collection_id": "tcga_ucec",
          "date_updated": "2022-10-10",
          "description": "<p>\n\tThe Cancer Genome Atlas-Uterine Corpus Endometrial Carcinoma (TCGA-UCEC) data collection is part of a larger effort to enhance the TCGA http://cancergenome.nih.gov/ data set with characterized radiological images. The Cancer Imaging Program (CIP) with the cooperation of several of the TCGA tissue-contributing institutions are working to archive a large portion of the radiological images of the genetically-analyzed UCEC cases.</p>\n<p>\n\tPlease see the <a href=\"https: //wiki.cancerimagingarchive.net/display/Public/TCGA-UCEC\" target=\"_blank\">TCGA-UCEC</a> wiki page to learn more about the images and to obtain any supporting metadata for this collection.</p>\n",
          "doi": "10.7937/k9/tcia.2016.gkj0zwac",
          "image_types": "CR, CT, MR, PT, SM",
          "location": "Uterus",
          "species": "Human",
          "subject_count": 560,
          "supporting_data": "Clinical, Genomics"
        },
        {
          "cancer_type": "Head and Neck Squamous Cell Carcinoma",
          "collection_id": "tcga_hnsc",
          "date_updated": "2022-10-10",
          "description": "<div>\n\t<span>The <a href=\"http: //imaging.cancer.gov/\" target=\"_blank\"><u>Cancer Imaging Program (CIP)</u></a></span><span>&thinsp;</span><span> is working directly with primary investigators from institutes participating in TCGA to obtain and load images relating to the genomic, clinical, and pathological data being stored within the <a href=\"http: //tcga-data.nci.nih.gov/\" target=\"_blank\">TCGA Data Portal</a>.&nbsp;Currently this large PET/CT&nbsp;multi-sequence image collection of </span><span>head and neck squamous cell carcinoma (HNSC) patients can be matched by each unique case identifier with the extensive gene and expression data of the same case from The Cancer Genome Atlas Data Portal to research the link between clinical phenome and tissue genome.&nbsp;</span></p>\n<div>\n\t&nbsp;</p>\n<div>\n\t<span>Please see the <a href=\"https: //wiki.cancerimagingarchive.net/display/Public/TCGA-HNSC\" target=\"_blank\"><span>TCGA -HNSC</span></a> wiki page to learn more about the images and to obtain any supporting metadata for this collection.</span></p>\n",
          "doi": "10.7937/k9/tcia.2016.lxkq47ms",
          "image_types": "SM",
          "location": "Head-Neck",
          "species": "Human",
          "subject_count": 528,
          "supporting_data": "Clinical, Genomics"
        },
        {
          "cancer_type": "Lung Squamous Cell Carcinoma",
          "collection_id": "tcga_lusc",
          "date_updated": "2022-10-10",
          "description": "<p>\n\tThe Cancer Genome Atlas-Lung Squamous Cell Carcinoma (TCGA-LUSC) data collection is part of a larger effort to enhance the TCGA http://cancergenome.nih.gov/ data set with characterized radiological images. The Cancer Imaging Program (CIP) with the cooperation of several of the TCGA tissue-contributing institutions are working to archive a large portion of the radiological images of the LUSC cases.</p>\n<p>\n\tPlease see the <a href=\"https: //wiki.cancerimagingarchive.net/display/Public/TCGA-LUSC\" target=\"_blank\">TCGA-LUSC</a> wiki page to learn more about the images and to obtain any supporting metadata for this collection.</p>\n",
          "doi": "10.7937/k9/tcia.2016.tygkkfmq",
          "image_types": "CT, NM, PT, SM",
          "location": "Lung",
          "species": "Human",
          "subject_count": 504,
          "supporting_data": "Clinical, Genomics"
        }
      ]
    }
    """
    field_mappings = {
        "commons": "CRDC Cancer Imaging Data Commons",
        "_unique_id": "path:collection_id",
        "study_title": "path:collection_id",
        "accession_number": "path:collection_id",
        "short_name": {"path": "collection_id", "filters": ["uppercase"]},
        "full_name": {"path": "collection_id", "filters": ["uppercase"]},
        "dbgap_accession_number": {"path": "collection_id", "filters": ["uppercase"]},
        "description": {
            "path": "description",
            "filters": ["strip_html", "prepare_cidc_description"],
        },
        "image_types": "path:image_types",
        "subjects_count": "path:subject_count",
        "doi": {"path": "doi", "filters": ["uppercase"]},
        "species": "path:species",
        "disease_type": "path:cancer_type",
        "data_type": "path:supporting_data",
        "primary_site": "path:location",
        "tags": [],
        "study_metadata.minimal_info.alternative_study_description": {
            "path": "description",
            "filters": ["strip_html", "prepare_cidc_description"],
        },
    }

    respx.get("http://test/ok").mock(side_effect=httpx.TimeoutException)
    assert (
        get_metadata("cidc", "http://test/ok", filters=None, mappings=field_mappings)
        == {}
    )

    respx.get("http://test/ok").mock(side_effect=httpx.HTTPError)
    assert (
        get_metadata("cidc", "http://test/ok", filters=None, mappings=field_mappings)
        == {}
    )

    respx.get("http://test/ok").mock(side_effect=Exception)
    assert (
        get_metadata("cidc", "http://test/ok", filters=None, mappings=field_mappings)
        == {}
    )

    respx.get("http://test/ok").mock(
        return_value=httpx.Response(status_code=200, content=json_response)
    )
    assert get_metadata("cidc", None, filters=None, mappings=field_mappings) == {}

    assert get_metadata(
        "cidc",
        "http://test/ok",
        filters=None,
        mappings=field_mappings,
    ) == {
        "tcga_prad": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "commons": "CRDC Cancer Imaging Data Commons",
                "_unique_id": "tcga_prad",
                "study_title": "tcga_prad",
                "accession_number": "tcga_prad",
                "short_name": "TCGA_PRAD",
                "full_name": "TCGA_PRAD",
                "dbgap_accession_number": "TCGA_PRAD",
                "description": "Note: This collection has special restrictions on its usage. See Data Usage Policies and Restrictions. The Cancer Imaging Program (CIP)  is working directly with primary investigators from institutes participating in TCGA to obtain and load images relating to the genomic, clinical, and pathological data being stored within the TCGA Data Portal. Currently this image collection of prostate adenocarcinoma (PRAD) patients can be matched by each unique case identifier with the extensive gene and expression data of the same case from The Cancer Genome Atlas Data Portal to research the link between clinical phenome and tissue genome.  Please see the TCGA-PRAD wiki page to learn more about the images and to obtain any supporting metadata for this collection.",
                "image_types": "CT, MR, PT, SM",
                "subjects_count": 500,
                "doi": "10.7937/K9/TCIA.2016.YXOGLM4Y",
                "species": "Human",
                "disease_type": "Prostate Cancer",
                "data_type": "Clinical, Genomics",
                "primary_site": "Prostate",
                "tags": [
                    {"name": "Prostate Cancer", "category": "disease_type"},
                    {"name": "Clinical, Genomics", "category": "data_type"},
                    {"name": "Prostate", "category": "primary_site"},
                ],
                "study_metadata": {
                    "minimal_info": {
                        "alternative_study_description": "Note: This collection has special restrictions on its usage. See Data Usage Policies and Restrictions. The Cancer Imaging Program (CIP)  is working directly with primary investigators from institutes participating in TCGA to obtain and load images relating to the genomic, clinical, and pathological data being stored within the TCGA Data Portal. Currently this image collection of prostate adenocarcinoma (PRAD) patients can be matched by each unique case identifier with the extensive gene and expression data of the same case from The Cancer Genome Atlas Data Portal to research the link between clinical phenome and tissue genome.  Please see the TCGA-PRAD wiki page to learn more about the images and to obtain any supporting metadata for this collection."
                    }
                },
            },
        },
        "tcga_blca": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "commons": "CRDC Cancer Imaging Data Commons",
                "_unique_id": "tcga_blca",
                "study_title": "tcga_blca",
                "accession_number": "tcga_blca",
                "short_name": "TCGA_BLCA",
                "full_name": "TCGA_BLCA",
                "dbgap_accession_number": "TCGA_BLCA",
                "description": "The Cancer Genome Atlas-Bladder Endothelial Carcinoma (TCGA-BLCA) data collection is part of a larger effort to enhance the TCGA http://cancergenome.nih.gov/ data set with characterized radiological images. The Cancer Imaging Program (CIP), with the cooperation of several of the TCGA tissue-contributing institutions, has archived a large portion of the radiological images of the genetically-analyzed BLCA cases.Please see the TCGA-BLCA wiki page to learn more about the images and to obtain any supporting metadata for this collection.",
                "image_types": "CR, CT, DX, MR, PT, SM",
                "subjects_count": 412,
                "doi": "10.7937/K9/TCIA.2016.8LNG8XDR",
                "species": "Human",
                "disease_type": "Bladder Endothelial Carcinoma",
                "data_type": "Clinical, Genomics",
                "primary_site": "Bladder",
                "tags": [
                    {
                        "name": "Bladder Endothelial Carcinoma",
                        "category": "disease_type",
                    },
                    {"name": "Clinical, Genomics", "category": "data_type"},
                    {"name": "Bladder", "category": "primary_site"},
                ],
                "study_metadata": {
                    "minimal_info": {
                        "alternative_study_description": "The Cancer Genome Atlas-Bladder Endothelial Carcinoma (TCGA-BLCA) data collection is part of a larger effort to enhance the TCGA http://cancergenome.nih.gov/ data set with characterized radiological images. The Cancer Imaging Program (CIP), with the cooperation of several of the TCGA tissue-contributing institutions, has archived a large portion of the radiological images of the genetically-analyzed BLCA cases.Please see the TCGA-BLCA wiki page to learn more about the images and to obtain any supporting metadata for this collection."
                    }
                },
            },
        },
        "tcga_ucec": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "commons": "CRDC Cancer Imaging Data Commons",
                "_unique_id": "tcga_ucec",
                "study_title": "tcga_ucec",
                "accession_number": "tcga_ucec",
                "short_name": "TCGA_UCEC",
                "full_name": "TCGA_UCEC",
                "dbgap_accession_number": "TCGA_UCEC",
                "description": "The Cancer Genome Atlas-Uterine Corpus Endometrial Carcinoma (TCGA-UCEC) data collection is part of a larger effort to enhance the TCGA http://cancergenome.nih.gov/ data set with characterized radiological images. The Cancer Imaging Program (CIP) with the cooperation of several of the TCGA tissue-contributing institutions are working to archive a large portion of the radiological images of the genetically-analyzed UCEC cases.Please see the TCGA-UCEC wiki page to learn more about the images and to obtain any supporting metadata for this collection.",
                "image_types": "CR, CT, MR, PT, SM",
                "subjects_count": 560,
                "doi": "10.7937/K9/TCIA.2016.GKJ0ZWAC",
                "species": "Human",
                "disease_type": "Uterine Corpus Endometrial Carcinoma",
                "data_type": "Clinical, Genomics",
                "primary_site": "Uterus",
                "tags": [
                    {
                        "name": "Uterine Corpus Endometrial Carcinoma",
                        "category": "disease_type",
                    },
                    {"name": "Clinical, Genomics", "category": "data_type"},
                    {"name": "Uterus", "category": "primary_site"},
                ],
                "study_metadata": {
                    "minimal_info": {
                        "alternative_study_description": "The Cancer Genome Atlas-Uterine Corpus Endometrial Carcinoma (TCGA-UCEC) data collection is part of a larger effort to enhance the TCGA http://cancergenome.nih.gov/ data set with characterized radiological images. The Cancer Imaging Program (CIP) with the cooperation of several of the TCGA tissue-contributing institutions are working to archive a large portion of the radiological images of the genetically-analyzed UCEC cases.Please see the TCGA-UCEC wiki page to learn more about the images and to obtain any supporting metadata for this collection."
                    }
                },
            },
        },
        "tcga_hnsc": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "commons": "CRDC Cancer Imaging Data Commons",
                "_unique_id": "tcga_hnsc",
                "study_title": "tcga_hnsc",
                "accession_number": "tcga_hnsc",
                "short_name": "TCGA_HNSC",
                "full_name": "TCGA_HNSC",
                "dbgap_accession_number": "TCGA_HNSC",
                "description": "The Cancer Imaging Program (CIP)  is working directly with primary investigators from institutes participating in TCGA to obtain and load images relating to the genomic, clinical, and pathological data being stored within the TCGA Data Portal. Currently this large PET/CT multi-sequence image collection of head and neck squamous cell carcinoma (HNSC) patients can be matched by each unique case identifier with the extensive gene and expression data of the same case from The Cancer Genome Atlas Data Portal to research the link between clinical phenome and tissue genome.  Please see the TCGA -HNSC wiki page to learn more about the images and to obtain any supporting metadata for this collection.",
                "image_types": "SM",
                "subjects_count": 528,
                "doi": "10.7937/K9/TCIA.2016.LXKQ47MS",
                "species": "Human",
                "disease_type": "Head and Neck Squamous Cell Carcinoma",
                "data_type": "Clinical, Genomics",
                "primary_site": "Head-Neck",
                "tags": [
                    {
                        "name": "Head and Neck Squamous Cell Carcinoma",
                        "category": "disease_type",
                    },
                    {"name": "Clinical, Genomics", "category": "data_type"},
                    {"name": "Head-Neck", "category": "primary_site"},
                ],
                "study_metadata": {
                    "minimal_info": {
                        "alternative_study_description": "The Cancer Imaging Program (CIP)  is working directly with primary investigators from institutes participating in TCGA to obtain and load images relating to the genomic, clinical, and pathological data being stored within the TCGA Data Portal. Currently this large PET/CT multi-sequence image collection of head and neck squamous cell carcinoma (HNSC) patients can be matched by each unique case identifier with the extensive gene and expression data of the same case from The Cancer Genome Atlas Data Portal to research the link between clinical phenome and tissue genome.  Please see the TCGA -HNSC wiki page to learn more about the images and to obtain any supporting metadata for this collection."
                    }
                },
            },
        },
        "tcga_lusc": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "commons": "CRDC Cancer Imaging Data Commons",
                "_unique_id": "tcga_lusc",
                "study_title": "tcga_lusc",
                "accession_number": "tcga_lusc",
                "short_name": "TCGA_LUSC",
                "full_name": "TCGA_LUSC",
                "dbgap_accession_number": "TCGA_LUSC",
                "description": "The Cancer Genome Atlas-Lung Squamous Cell Carcinoma (TCGA-LUSC) data collection is part of a larger effort to enhance the TCGA http://cancergenome.nih.gov/ data set with characterized radiological images. The Cancer Imaging Program (CIP) with the cooperation of several of the TCGA tissue-contributing institutions are working to archive a large portion of the radiological images of the LUSC cases.Please see the TCGA-LUSC wiki page to learn more about the images and to obtain any supporting metadata for this collection.",
                "image_types": "CT, NM, PT, SM",
                "subjects_count": 504,
                "doi": "10.7937/K9/TCIA.2016.TYGKKFMQ",
                "species": "Human",
                "disease_type": "Lung Squamous Cell Carcinoma",
                "data_type": "Clinical, Genomics",
                "primary_site": "Lung",
                "tags": [
                    {
                        "name": "Lung Squamous Cell Carcinoma",
                        "category": "disease_type",
                    },
                    {"name": "Clinical, Genomics", "category": "data_type"},
                    {"name": "Lung", "category": "primary_site"},
                ],
                "study_metadata": {
                    "minimal_info": {
                        "alternative_study_description": "The Cancer Genome Atlas-Lung Squamous Cell Carcinoma (TCGA-LUSC) data collection is part of a larger effort to enhance the TCGA http://cancergenome.nih.gov/ data set with characterized radiological images. The Cancer Imaging Program (CIP) with the cooperation of several of the TCGA tissue-contributing institutions are working to archive a large portion of the radiological images of the LUSC cases.Please see the TCGA-LUSC wiki page to learn more about the images and to obtain any supporting metadata for this collection."
                    }
                },
            },
        },
    }
    assert get_metadata(
        "cidc",
        "http://test/ok",
        filters=None,
        mappings=field_mappings,
        keepOriginalFields=True,
    ) == {
        "tcga_prad": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "cancer_type": "Prostate Cancer",
                "collection_id": "tcga_prad",
                "date_updated": "2022-10-10",
                "description": "Note: This collection has special restrictions on its usage. See Data Usage Policies and Restrictions. The Cancer Imaging Program (CIP)  is working directly with primary investigators from institutes participating in TCGA to obtain and load images relating to the genomic, clinical, and pathological data being stored within the TCGA Data Portal. Currently this image collection of prostate adenocarcinoma (PRAD) patients can be matched by each unique case identifier with the extensive gene and expression data of the same case from The Cancer Genome Atlas Data Portal to research the link between clinical phenome and tissue genome.  Please see the TCGA-PRAD wiki page to learn more about the images and to obtain any supporting metadata for this collection.",
                "doi": "10.7937/K9/TCIA.2016.YXOGLM4Y",
                "image_types": "CT, MR, PT, SM",
                "location": "Prostate",
                "species": "Human",
                "subject_count": 500,
                "supporting_data": "Clinical, Genomics",
                "commons": "CRDC Cancer Imaging Data Commons",
                "_unique_id": "tcga_prad",
                "study_title": "tcga_prad",
                "accession_number": "tcga_prad",
                "short_name": "TCGA_PRAD",
                "full_name": "TCGA_PRAD",
                "dbgap_accession_number": "TCGA_PRAD",
                "subjects_count": 500,
                "disease_type": "Prostate Cancer",
                "data_type": "Clinical, Genomics",
                "primary_site": "Prostate",
                "tags": [
                    {"name": "Prostate Cancer", "category": "disease_type"},
                    {"name": "Clinical, Genomics", "category": "data_type"},
                    {"name": "Prostate", "category": "primary_site"},
                ],
                "study_metadata": {
                    "minimal_info": {
                        "alternative_study_description": "Note: This collection has special restrictions on its usage. See Data Usage Policies and Restrictions. The Cancer Imaging Program (CIP)  is working directly with primary investigators from institutes participating in TCGA to obtain and load images relating to the genomic, clinical, and pathological data being stored within the TCGA Data Portal. Currently this image collection of prostate adenocarcinoma (PRAD) patients can be matched by each unique case identifier with the extensive gene and expression data of the same case from The Cancer Genome Atlas Data Portal to research the link between clinical phenome and tissue genome.  Please see the TCGA-PRAD wiki page to learn more about the images and to obtain any supporting metadata for this collection."
                    }
                },
            },
        },
        "tcga_blca": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "cancer_type": "Bladder Endothelial Carcinoma",
                "collection_id": "tcga_blca",
                "date_updated": "2022-10-10",
                "description": "The Cancer Genome Atlas-Bladder Endothelial Carcinoma (TCGA-BLCA) data collection is part of a larger effort to enhance the TCGA http://cancergenome.nih.gov/ data set with characterized radiological images. The Cancer Imaging Program (CIP), with the cooperation of several of the TCGA tissue-contributing institutions, has archived a large portion of the radiological images of the genetically-analyzed BLCA cases.Please see the TCGA-BLCA wiki page to learn more about the images and to obtain any supporting metadata for this collection.",
                "doi": "10.7937/K9/TCIA.2016.8LNG8XDR",
                "image_types": "CR, CT, DX, MR, PT, SM",
                "location": "Bladder",
                "species": "Human",
                "subject_count": 412,
                "supporting_data": "Clinical, Genomics",
                "commons": "CRDC Cancer Imaging Data Commons",
                "_unique_id": "tcga_blca",
                "study_title": "tcga_blca",
                "accession_number": "tcga_blca",
                "short_name": "TCGA_BLCA",
                "full_name": "TCGA_BLCA",
                "dbgap_accession_number": "TCGA_BLCA",
                "subjects_count": 412,
                "disease_type": "Bladder Endothelial Carcinoma",
                "data_type": "Clinical, Genomics",
                "primary_site": "Bladder",
                "tags": [
                    {
                        "name": "Bladder Endothelial Carcinoma",
                        "category": "disease_type",
                    },
                    {"name": "Clinical, Genomics", "category": "data_type"},
                    {"name": "Bladder", "category": "primary_site"},
                ],
                "study_metadata": {
                    "minimal_info": {
                        "alternative_study_description": "The Cancer Genome Atlas-Bladder Endothelial Carcinoma (TCGA-BLCA) data collection is part of a larger effort to enhance the TCGA http://cancergenome.nih.gov/ data set with characterized radiological images. The Cancer Imaging Program (CIP), with the cooperation of several of the TCGA tissue-contributing institutions, has archived a large portion of the radiological images of the genetically-analyzed BLCA cases.Please see the TCGA-BLCA wiki page to learn more about the images and to obtain any supporting metadata for this collection."
                    }
                },
            },
        },
        "tcga_ucec": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "cancer_type": "Uterine Corpus Endometrial Carcinoma",
                "collection_id": "tcga_ucec",
                "date_updated": "2022-10-10",
                "description": "The Cancer Genome Atlas-Uterine Corpus Endometrial Carcinoma (TCGA-UCEC) data collection is part of a larger effort to enhance the TCGA http://cancergenome.nih.gov/ data set with characterized radiological images. The Cancer Imaging Program (CIP) with the cooperation of several of the TCGA tissue-contributing institutions are working to archive a large portion of the radiological images of the genetically-analyzed UCEC cases.Please see the TCGA-UCEC wiki page to learn more about the images and to obtain any supporting metadata for this collection.",
                "doi": "10.7937/K9/TCIA.2016.GKJ0ZWAC",
                "image_types": "CR, CT, MR, PT, SM",
                "location": "Uterus",
                "species": "Human",
                "subject_count": 560,
                "supporting_data": "Clinical, Genomics",
                "commons": "CRDC Cancer Imaging Data Commons",
                "_unique_id": "tcga_ucec",
                "study_title": "tcga_ucec",
                "accession_number": "tcga_ucec",
                "short_name": "TCGA_UCEC",
                "full_name": "TCGA_UCEC",
                "dbgap_accession_number": "TCGA_UCEC",
                "subjects_count": 560,
                "disease_type": "Uterine Corpus Endometrial Carcinoma",
                "data_type": "Clinical, Genomics",
                "primary_site": "Uterus",
                "tags": [
                    {
                        "name": "Uterine Corpus Endometrial Carcinoma",
                        "category": "disease_type",
                    },
                    {"name": "Clinical, Genomics", "category": "data_type"},
                    {"name": "Uterus", "category": "primary_site"},
                ],
                "study_metadata": {
                    "minimal_info": {
                        "alternative_study_description": "The Cancer Genome Atlas-Uterine Corpus Endometrial Carcinoma (TCGA-UCEC) data collection is part of a larger effort to enhance the TCGA http://cancergenome.nih.gov/ data set with characterized radiological images. The Cancer Imaging Program (CIP) with the cooperation of several of the TCGA tissue-contributing institutions are working to archive a large portion of the radiological images of the genetically-analyzed UCEC cases.Please see the TCGA-UCEC wiki page to learn more about the images and to obtain any supporting metadata for this collection."
                    }
                },
            },
        },
        "tcga_hnsc": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "cancer_type": "Head and Neck Squamous Cell Carcinoma",
                "collection_id": "tcga_hnsc",
                "date_updated": "2022-10-10",
                "description": "The Cancer Imaging Program (CIP)  is working directly with primary investigators from institutes participating in TCGA to obtain and load images relating to the genomic, clinical, and pathological data being stored within the TCGA Data Portal. Currently this large PET/CT multi-sequence image collection of head and neck squamous cell carcinoma (HNSC) patients can be matched by each unique case identifier with the extensive gene and expression data of the same case from The Cancer Genome Atlas Data Portal to research the link between clinical phenome and tissue genome.  Please see the TCGA -HNSC wiki page to learn more about the images and to obtain any supporting metadata for this collection.",
                "doi": "10.7937/K9/TCIA.2016.LXKQ47MS",
                "image_types": "SM",
                "location": "Head-Neck",
                "species": "Human",
                "subject_count": 528,
                "supporting_data": "Clinical, Genomics",
                "commons": "CRDC Cancer Imaging Data Commons",
                "_unique_id": "tcga_hnsc",
                "study_title": "tcga_hnsc",
                "accession_number": "tcga_hnsc",
                "short_name": "TCGA_HNSC",
                "full_name": "TCGA_HNSC",
                "dbgap_accession_number": "TCGA_HNSC",
                "subjects_count": 528,
                "disease_type": "Head and Neck Squamous Cell Carcinoma",
                "data_type": "Clinical, Genomics",
                "primary_site": "Head-Neck",
                "tags": [
                    {
                        "name": "Head and Neck Squamous Cell Carcinoma",
                        "category": "disease_type",
                    },
                    {"name": "Clinical, Genomics", "category": "data_type"},
                    {"name": "Head-Neck", "category": "primary_site"},
                ],
                "study_metadata": {
                    "minimal_info": {
                        "alternative_study_description": "The Cancer Imaging Program (CIP)  is working directly with primary investigators from institutes participating in TCGA to obtain and load images relating to the genomic, clinical, and pathological data being stored within the TCGA Data Portal. Currently this large PET/CT multi-sequence image collection of head and neck squamous cell carcinoma (HNSC) patients can be matched by each unique case identifier with the extensive gene and expression data of the same case from The Cancer Genome Atlas Data Portal to research the link between clinical phenome and tissue genome.  Please see the TCGA -HNSC wiki page to learn more about the images and to obtain any supporting metadata for this collection."
                    }
                },
            },
        },
        "tcga_lusc": {
            "_guid_type": "discovery_metadata",
            "gen3_discovery": {
                "cancer_type": "Lung Squamous Cell Carcinoma",
                "collection_id": "tcga_lusc",
                "date_updated": "2022-10-10",
                "description": "The Cancer Genome Atlas-Lung Squamous Cell Carcinoma (TCGA-LUSC) data collection is part of a larger effort to enhance the TCGA http://cancergenome.nih.gov/ data set with characterized radiological images. The Cancer Imaging Program (CIP) with the cooperation of several of the TCGA tissue-contributing institutions are working to archive a large portion of the radiological images of the LUSC cases.Please see the TCGA-LUSC wiki page to learn more about the images and to obtain any supporting metadata for this collection.",
                "doi": "10.7937/K9/TCIA.2016.TYGKKFMQ",
                "image_types": "CT, NM, PT, SM",
                "location": "Lung",
                "species": "Human",
                "subject_count": 504,
                "supporting_data": "Clinical, Genomics",
                "commons": "CRDC Cancer Imaging Data Commons",
                "_unique_id": "tcga_lusc",
                "study_title": "tcga_lusc",
                "accession_number": "tcga_lusc",
                "short_name": "TCGA_LUSC",
                "full_name": "TCGA_LUSC",
                "dbgap_accession_number": "TCGA_LUSC",
                "subjects_count": 504,
                "disease_type": "Lung Squamous Cell Carcinoma",
                "data_type": "Clinical, Genomics",
                "primary_site": "Lung",
                "tags": [
                    {
                        "name": "Lung Squamous Cell Carcinoma",
                        "category": "disease_type",
                    },
                    {"name": "Clinical, Genomics", "category": "data_type"},
                    {"name": "Lung", "category": "primary_site"},
                ],
                "study_metadata": {
                    "minimal_info": {
                        "alternative_study_description": "The Cancer Genome Atlas-Lung Squamous Cell Carcinoma (TCGA-LUSC) data collection is part of a larger effort to enhance the TCGA http://cancergenome.nih.gov/ data set with characterized radiological images. The Cancer Imaging Program (CIP) with the cooperation of several of the TCGA tissue-contributing institutions are working to archive a large portion of the radiological images of the LUSC cases.Please see the TCGA-LUSC wiki page to learn more about the images and to obtain any supporting metadata for this collection."
                    }
                },
            },
        },
    }
