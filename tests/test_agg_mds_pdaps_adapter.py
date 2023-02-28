import respx
import httpx
import json

from mds.agg_mds.adapters import get_metadata
from tenacity import RetryError, wait_none


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
