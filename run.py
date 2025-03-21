"""
Usage:
- Run app: python run.py
- Generate openapi docs: python run.py openapi
"""

import os
from pathlib import Path
import sys
import uvicorn
import yaml
import cdislogging

import mds.config
from mds.main import get_app


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

cdislogging.get_logger(None, log_level="debug" if mds.config.DEBUG else "warn")
for logger_name in ["uvicorn", "uvicorn.error", "uvicorn.access"]:
    cdislogging.get_logger(
        logger_name, log_level="debug" if mds.config.DEBUG else "info"
    )


def _get_schema_with_clean_descriptions():
    """
    By default, openapi() in FastAPI just grabs the entire docstring and because we
    use Google-style, we have Args and Returns for the function, which makes the OpenAPI
    docs messy looking (since those inputs aren't always the same as the actual RESTful API).

    So, this just recreates a new schema with definitions based off only the first part
    of the docstring. It does so by splitting on the first well-defined part of
    the Google-style docstring, the string "Args", and returning only everything before that.
    """
    raw_schema = get_app().openapi()
    output_schema = {}
    output_schema["openapi"] = raw_schema.get("openapi", {})
    output_schema["info"] = raw_schema.get("info", {})
    output_schema["components"] = raw_schema.get("components", {})
    output_schema["paths"] = {}
    for path, path_info in raw_schema.get("paths").items():
        for http_operation, http_operation_info in path_info.items():
            stripped_description = (
                http_operation_info.get("description", "").split("Args")[0].strip()
            )
            http_operation_info["description"] = stripped_description

        output_schema["paths"].update({path: path_info})
    return output_schema


if __name__ == "__main__":
    if sys.argv[-1] == "openapi":
        schema = _get_schema_with_clean_descriptions()
        path = Path(CURRENT_DIR, "docs/openapi.yaml")
        yaml.Dumper.ignore_aliases = lambda *args: True
        with open(path, "w+") as f:
            yaml.dump(schema, f, default_flow_style=False)
        print(f"Saved docs at {path}")
    else:
        uvicorn.run("mds.asgi:app", reload=True, log_config=None)
