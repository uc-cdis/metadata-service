import pytest
import respx
from unittest.mock import patch
from conftest import AsyncMock
from mds.agg_mds.adapters import get_metadata


@respx.mock
def test_get_metadata():
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
