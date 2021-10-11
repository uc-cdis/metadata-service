import json

from fastapi import HTTPException, Query, APIRouter
from sqlalchemy import cast, column, exists
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql.operators import ColumnOperators
from starlette.requests import Request
from starlette.status import HTTP_400_BAD_REQUEST, HTTP_404_NOT_FOUND

from parsimonious.exceptions import IncompleteParseError, ParseError, VisitationError

from . import logger
from .models import db, Metadata
from .param_parser import filter_param_grammar, FilterVisitor

mod = APIRouter()


@mod.get("/metadata")
async def search_metadata(
    request: Request,
    data: bool = Query(
        False,
        description="Switch to returning a list of GUIDs (false), "
        "or GUIDs mapping to their metadata (true).",
    ),
    limit: int = Query(
        10, description="Maximum number of records returned. (max: 2000)"
    ),
    offset: int = Query(0, description="Return results at this given offset."),
):
    """Search the metadata.

    Without filters, this will return all data. Add filters as query strings like this:

        GET /metadata?a=1&b=2

    This will match all records that have metadata containing all of:

        {"a": 1, "b": 2}

    The values are always treated as strings for filtering. Nesting is supported:

        GET /metadata?a.b.c=3

    Matching records containing:

        {"a": {"b": {"c": 3}}}

    Providing the same key with more than one value filters records whose value of the
    given key matches any of the given values. But values of different keys must all
    match. For example:

        GET /metadata?a.b.c=3&a.b.c=33&a.b.d=4

    Matches these:

        {"a": {"b": {"c": 3, "d": 4}}}
        {"a": {"b": {"c": 33, "d": 4}}}
        {"a": {"b": {"c": "3", "d": 4, "e": 5}}}

    But won't match these:

        {"a": {"b": {"c": 3}}}
        {"a": {"b": {"c": 3, "d": 5}}}
        {"a": {"b": {"d": 5}}}
        {"a": {"b": {"c": "333", "d": 4}}}

    """
    limit = min(limit, 2000)
    queries = {}
    for key, value in request.query_params.multi_items():
        if key not in {"data", "limit", "offset"}:
            queries.setdefault(key, []).append(value)

    def add_filter(query):
        for path, values in queries.items():
            query = query.where(
                db.or_(Metadata.data[list(path.split("."))].astext == v for v in values)
            )
        return query.offset(offset).limit(limit)

    if data:
        return {
            metadata.guid: metadata.data
            for metadata in await add_filter(Metadata.query).gino.all()
        }
    else:
        return [
            row[0]
            for row in await add_filter(db.select([Metadata.guid]))
            .gino.return_model(False)
            .all()
        ]


async def search_metadata_objects(
    data: bool = True,
    page: int = 0,
    limit: int = 10,
    filter: str = "",
):
    """
    Intended as a helper for the get_objects function to query the db
    based on the filter param. Please see get_objects function for more
    documentation of filter param formatting.

    Args:
        data (bool): Switch to returning a list of GUIDs (false), or metadata
        objects (true).

        page (int): The offset for what objects are returned (zero-indexed).
        The exact offset will be equal to page*limit (e.g. with page=1,
        limit=15, 15 objects beginning at index 15 will be returned).

        limit (int): Maximum number of objects returned (max: 1024). Also used
        with page to determine page size.

        filter (str): The filter(s) that will be applied to the result.

    Returns:
        if data=True, a list of (guid, metadata object) tuples
        if data=False, a list of guids
    """
    limit = min(limit, 1024)

    def add_filter(target_key, operator_name, right_operand):
        """
        Wrapper around operators dict. Decides if a scalar or compound operator
        needs to be instantiated based on right_operand.

        Args:
        target_key (str): the json key which we are filtering based on

        operator_name (str): the name of the operator (leading colon included)

        right_operand (str, bool, int, float, dict): With a scalar filter (i.e.
        when it's not a dict), the value that the target value is compared
        against.  With a compound filter (i.e. when it's a dict), the nested
        filter that's run on the target key.

        e.g. for the following requests, the arguments to add_filter:
            * `GET /objects?filter=(_uploader_id,:eq,"57")`
                - target_key is '_uploader_id'
                - operator_name is ':eq'
                - right_operand is '57'
            * `GET /objects?filter=(count,:gte,16)`
                - target_key is 'count'
                - operator_name is ':gte'
                - right_operand is 16 (note this is a python int)
            * `GET /objects?filter=(_resource_paths,:any,(,:like,"/programs/foo/%"))`
                - target_key is '_resource_paths'
                - operator_name is ':any'
                - right_operand is {'name': '', 'op': ':like', 'val': '/programs/foo/%'}

        Returns:
            SQLAlchemy object
        """

        json_object = Metadata.data[list(target_key.split("."))]

        if type(right_operand) is dict:
            #  since right_operand isn't a primitive type, it means that this is a compound
            #  operator
            #  (e.g. GET objects?filter=(_resource_paths,:any,(,:like,"/programs/%")) )
            return operators[operator_name]["sql_clause"](
                json_object, right_operand["op"], right_operand["val"]
            )

        if (
            "type" in operators[operator_name]
            and operators[operator_name]["type"] == "text"
        ):
            json_object = json_object.astext
        else:
            #  this is necessary to differentiate between strings, booleans, numbers,
            #  etc. in JSON
            right_operand = cast(right_operand, JSONB)

        return operators[operator_name]["sql_comparator"](json_object, right_operand)

    def get_any_sql_clause(target_json_key, scalar_operator_name, scalar_right_operand):
        """
        Generate a SQLAlchemy clause that corresponds to the :any operation.

        Args:
            target_json_key (str): the json key pointing to the array which we
            are filtering based on

            scalar_operator_name (str): the name of the operator (leading colon
            included) (see operators dict for a list of valid operators)

            scalar_right_operand (str, bool, int, float): the value that each
            element in the target_json_key array is compared against

        Returns:
            SQLAlchemy clause corresponding to the :any operation.

        Note: get_any_sql_clause could possibly be used for a :has_key
        operation w/ a little more work (e.g. does teams object {} have "bears"
        key: (teams,:any,(,:eq,"bears"))) (would need to use jsonb_object_keys
        https://www.postgresql.org/docs/9.5/functions-json.html#FUNCTIONS-JSON-OP-TABLE)
        """
        if (
            "type" in operators[scalar_operator_name]
            and operators[scalar_operator_name]["type"] == "text"
        ):
            f = db.func.jsonb_array_elements_text
        else:
            f = db.func.jsonb_array_elements
            scalar_right_operand = cast(scalar_right_operand, JSONB)

        f = f(target_json_key).alias()

        return exists(
            db.select("*")
            .select_from(f)
            .where(
                operators[scalar_operator_name]["sql_comparator"](
                    column(f.name), scalar_right_operand
                )
            )
        )

    def get_all_sql_clause(target_json_key, scalar_operator_name, scalar_right_operand):
        """
        Generate a SQLAlchemy clause that corresponds to the :all operation.

        Args:
            target_json_key (str): the json key pointing to the array which we
            are filtering based on

            scalar_operator_name (str): the name of the operator (leading colon
            included) (see operators dict for a list of valid operators)

            scalar_right_operand (str, bool, int, float): the value that each
            element in the target_json_key array is compared against

        Returns:
            SQLAlchemy clause corresponding to the :all operation.
        """
        if (
            "type" in operators[scalar_operator_name]
            and operators[scalar_operator_name]["type"] == "text"
        ):
            f = db.func.jsonb_array_elements_text
        else:
            f = db.func.jsonb_array_elements
            scalar_right_operand = cast(scalar_right_operand, JSONB)

        f = f(target_json_key).alias()
        count_f = db.func.jsonb_array_length(target_json_key)

        return ColumnOperators.__eq__(
            count_f,
            db.select([db.func.count()])
            .select_from(f)
            .where(
                operators[scalar_operator_name]["sql_comparator"](
                    column(f.name), scalar_right_operand
                )
            ),
        )

    operators = {
        ":eq": {"sql_comparator": ColumnOperators.__eq__},
        ":ne": {"sql_comparator": ColumnOperators.__ne__},
        ":gt": {"sql_comparator": ColumnOperators.__gt__},
        ":gte": {"sql_comparator": ColumnOperators.__ge__},
        ":lt": {"sql_comparator": ColumnOperators.__lt__},
        ":lte": {"sql_comparator": ColumnOperators.__le__},
        ":like": {
            "type": "text",
            "sql_comparator": ColumnOperators.like,
        },
        ":all": {"sql_clause": get_all_sql_clause},
        ":any": {"sql_clause": get_any_sql_clause},
    }

    def get_sqlalchemy_clause(filter_dict):
        """
        Return a SQLAlchemy WHERE clause representing filter_dict.

        Args:
            filter_dict(dict):

            when filter_dict is a scalar:
            {
                "name": "_resource_paths.1",
                "op": ":like",
                "val": "/programs/%"
            }

            when filter_dict is a compound:
            {
                "name": "_resource_paths",
                "op": ":any",
                "val": {
                    "name": "",
                    "op": ":like",
                    "val": "/programs/%"
                }
            }

            when filter_dict is a boolean compound:
            {
                "or": [
                    {
                        "name": "_uploader_id",
                        "op": ":eq",
                        "val": "101"
                    },
                    {
                        "name": "_uploader_id",
                        "op": ":eq",
                        "val": "102"
                    }
                ]
            }

        Returns:
            A SQLAlchemy WHERE clause representing filter_dict.
        """
        if not filter_dict:
            return

        if len(filter_dict) == 1:
            boolean = list(filter_dict.keys())[0]

            boolean_operand_clauses = (
                get_sqlalchemy_clause(c) for c in filter_dict[boolean]
            )
            if boolean == "and":
                return db.and_(boolean_operand_clauses)
            elif boolean == "or":
                return db.or_(boolean_operand_clauses)
            raise

        return add_filter(filter_dict["name"], filter_dict["op"], filter_dict["val"])

    try:
        filter_tree = None
        if filter:
            filter_tree = filter_param_grammar.parse(filter)
        filter_dict = {}
        if filter_tree:
            filter_tree_visitor = FilterVisitor()
            filter_dict = filter_tree_visitor.visit(filter_tree)
    except (IncompleteParseError, ParseError) as e:
        logger.debug(e, exc_info=True)
        raise HTTPException(
            HTTP_400_BAD_REQUEST,
            f"Please check syntax for the provided `filter` URL query param, which could not be correctly parsed from index {e.pos} (zero-indexed) onwards.",
        )
    except VisitationError as e:
        logger.debug(e, exc_info=True)
        raise HTTPException(
            HTTP_400_BAD_REQUEST,
            "Please check syntax for the provided `filter` URL query param, which could not be correctly parsed.",
        )

    if data:
        query = Metadata.query
        if filter_dict:
            query = query.where(get_sqlalchemy_clause(filter_dict))
        return [
            (metadata.guid, metadata.data)
            for metadata in await query.offset(page * limit).limit(limit).gino.all()
        ]
    else:
        query = db.select([Metadata.guid])
        if filter_dict:
            query = query.where(get_sqlalchemy_clause(filter_dict))
        return [
            row[0]
            for row in await query.offset(page * limit)
            .limit(limit)
            .gino.return_model(False)
            .all()
        ]


@mod.get("/metadata/{guid:path}")
async def get_metadata(guid):
    """Get the metadata of the GUID."""
    metadata = await Metadata.get(guid)
    if metadata:
        return metadata.data
    else:
        raise HTTPException(HTTP_404_NOT_FOUND, f"Not found: {guid}")


def init_app(app):
    app.include_router(mod, tags=["Query"])
