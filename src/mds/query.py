import json

from fastapi import HTTPException, Query, APIRouter
from pydantic import Json
from sqlalchemy import cast, column, exists
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql.operators import ColumnOperators
from starlette.requests import Request
from starlette.status import HTTP_404_NOT_FOUND

from parsimonious.grammar import Grammar

from .models import db, Metadata

mod = APIRouter()


@mod.get("/metadata")
#  XXX fix tests
#  XXX maybe translate old way to use filter argument here?
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

    #  def add_filter(query):
    def add_filters():
        filter_param = "(and"
        for path, values in queries.items():
            filter_param += ",(or"
            for v in values:
                filter_param += f",({path},:like,{v})"
                #  query = query.where(
                #  db.or_(Metadata.data[list(path.split("."))].astext == v for v in values)
                #  db.or_(Metadata.data[list(path.split("."))].astext == v)
                #  )
            filter_param += ")"
        filter_param += ")"
        return filter_param
        #  return query.offset(offset).limit(limit)

    #  if data:
    #  return {
    #  metadata.guid: metadata.data
    #  for metadata in await add_filter(Metadata.query).gino.all()
    #  }
    #  else:
    #  return [
    #  row[0]
    #  for row in await add_filter(db.select([Metadata.guid]))
    #  .gino.return_model(False)
    #  .all()
    #  ]

    return await search_metadata_helper(
        #  request.query_params, data=data, limit=limit, offset=offset
        data=data,
        limit=limit,
        offset=offset,
        filter=add_filters(),
    )


#  XXX rename/update docstring
async def search_metadata_helper(
    data: bool = Query(
        False,
        description="Switch to returning a list of GUIDs (false), "
        "or GUIDs mapping to their metadata (true).",
    ),
    limit: int = Query(
        10, description="Maximum number of records returned. (max: 2000)"
    ),
    offset: int = Query(0, description="Return results at this given offset."),
    #  XXX description
    #  XXX how to name this variable something other than filter
    #  filter: Json = Query(Json(), description="The filters!"),
    filter: str = Query("", description="The filters!"),
):
    """
    XXX comments
    """
    #  import pdb; pdb.set_trace()

    limit = min(limit, 2000)
    #  XXX using __op__?
    #  XXX aggregate functions (e.g size, max, etc.)
    filter_operators = {
        ":all": None,
        ":any": None,
        ":eq": ColumnOperators.__eq__,
        ":ne": ColumnOperators.__ne__,
        ":gt": ColumnOperators.__gt__,
        ":gte": ColumnOperators.__ge__,
        ":lt": ColumnOperators.__lt__,
        ":lte": ColumnOperators.__le__,
        #  XXX check how mongoDB filters for null (does it use it, if an object doesn't have a given key, is that considered null, etc.)
        #  XXX in is probably unnecessary (i.e. instead of (score, :in, [1, 2]) do (OR (score, :eq, 1), (score, :eq, 2)))
        #  ":in": ColumnOperators.in_,
        #  XXX what does SQL IS mean?
        ":is": ColumnOperators.is_,
        ":is_not": ColumnOperators.isnot,
        ":like": ColumnOperators.like,
    }
    textual_operators = [":like"]
    other_not_to_be_cast = [":like"]

    def fun_parser(s):
        #  grammar = Grammar(
        #  """
        #  bold_text  = bold_open text bold_close
        #  text       = ~"[A-Z 0-9]*"i
        #  bold_open  = "(("
        #  bold_close = "))"
        #  """)

        # '(and,(or,(fun_fact,:like,rhinos_rock),(fun_fact,:like,cats_rock)),(or,(_uploader_id,:like,57)))'
        #  (_uploader_id,:eq,“57”)
        #  (or,(_uploader_id,:like,57))

        #  compound_filter = open boolean separator scalar_filter close
        #  boolean         = "and"
        #  json_value      = "57" / "rhinosrock" / "catsrock"
        #  json_value      = ~"[A-Z 0-9_]*"i
        grammar = Grammar(
            """
        boolean_filter = scalar_filter / (open boolean (separator (scalar_filter / boolean_filter))+ close)
        boolean         = "and" / "or"
        scalar_filter   = open json_key separator operator separator (json_value / scalar_filter) close
        json_key        = ~"[A-Z 0-9_]*"i
        #  json_value      = "57" / "rhinosrock" / "catsrock"
        operator        = ":eq" / ":any"
        open            = "("
        close           = ")"
        separator       = ","

        json_value = true_false_null / number / doubleString
        #  json_value = true_false_null / number
        true_false_null = "true" / "false" / "null"
        number = (int frac exp) / (int exp) / (int frac) / int
        int = "-"? ((digit1to9 digits) / digit)
        frac = "." digits
        exp = e digits
        digits = digit+
        e = "e+" / "e-" / "e" / "E+" / "E-" / "E"
        digit1to9 = ~"[1-9]"
        digit = ~"[0-9]"

        #  string = ~"\"[^\"]*\""
        #  string = ~"[\".*\"]"
        #  string = ~"\".*\""
        #  doubleString   = ~'"([^"]|(\"))*"'
        doubleString   = ~'"([^"])*"'
        """
        )
        #  o = grammar.parse('(fun_fact,:eq,"rhinos_rock")')
        #  o = grammar.parse('(and,(or,(fun_fact,:eq,"rhinosrock"),(funfact,:eq,"catsrock")),(or,(uploaderid,:eq,57)))')
        #  import pdb; pdb.set_trace()

        #  json_value = "\"57\""
        #  print(grammar.parse('((bold stuff))'))
        #  print(grammar.parse('(_uploader_id,:eq,57)'))
        #  print(grammar.parse('(_uploader_id,:eq,true)'))
        #  print(grammar.parse('(_uploader_id,:eq,false)'))
        #  print(grammar.parse('(_uploader_id,:eq,null)'))
        #  print(grammar.parse('(fun_fact,:eq,"rhinos_rock")'))
        #  print(grammar.parse('(uploader,:eq,"57")'))
        #  print(grammar.parse('(uploader,:eq,"57")'))
        #  print(grammar.parse('(paths,:any,(,:eq,57))'))
        #  print(grammar.parse('(and,(uploader,:eq,57))'))
        #  print(grammar.parse('(or,(uploader,:eq,57))'))
        #  print(grammar.parse('(and,(or,(funfact,:eq,rhinosrock),(funfact,:eq,catsrock)),(or,(uploaderid,:eq,57)))'))
        print(
            grammar.parse(
                '(and,(or,(fun_fact,:eq,"rhinosrock"),(funfact,:eq,"catsrock")),(or,(uploaderid,:eq,57)))'
            )
        )

        #  print(grammar.parse('(paths,:eq,(,:eq,57))'))
        #  print(grammar.parse('(and,(uploader,:eq,57))'))
        #  print(grammar.parse('(,:eq,57)'))

    fun_parser(filter)

    #  XXX comments
    def parantheses_to_json(s):
        def scalar_to_json(s):
            if not s:
                return {}
            #  XXX what if only one is a parantheses?
            if s[0] != "(" and s[-1] != ")":
                try:
                    return json.loads(s)
                except:
                    return s

            f = s[1:-1].split(",", 2)
            f = {"name": f[0], "op": f[1], "val": parantheses_to_json(f[2])}
            return f

        #  XXX DRY with boolean_operators = ["or", "and"]
        #  scalar_operator_pattern = r'\({json_key_pattern}*,{operator_pattern},{json_value_pattern}\)'
        #  compund_operator_pattern = r'\({json_key_pattern}*,{operator_pattern},({scalar_value_pattern}|{json_value_pattern})\)'
        #  #  filter_pattern = r'\({{and|or}{,filter_pattern}+}|{compund_operator}\)'
        #  boolean_filters_pattern = r'\({{and|or}{,compund_operator}+}\)'
        #  filter_pattern = r''
        #  r‘\((and|or)(,\([.+]]\))\)’
        #  if s.startswith('(or'):
        #  return {"or": [parantheses_to_json(s[3:-1])]}
        #  if s.startswith('(and'):
        #  return {"and": parantheses_to_json(s[4:-1])}

        return scalar_to_json(s)

    filter = [parantheses_to_json(filter)]

    #  XXX should maybe default query
    #  XXX comments
    def add_filter(query):

        #  XXX make this an optional url query param
        for _filter in filter:
            if not _filter:
                continue

            json_object = Metadata.data[list(_filter["name"].split("."))]
            operator = filter_operators[_filter["op"]]
            other = cast(_filter["val"], JSONB)
            #  XXX this could be used for has_key (e.g. for does have bears key: (teams, :any, (,:eq, "bears")))
            if _filter["op"] == ":any":

                #  XXX handle duplicates
                #  XXX any and has have to be used with other scalar operation
                #  (i.e. (_resource_paths,:any,(,:like,"/programs/*")). whether
                #  there is a key for the scalar comparator might determine
                #  whether to use jsonb_array_elements

                other = cast(_filter["val"]["val"], JSONB)
                f = db.func.jsonb_array_elements
                if _filter["val"]["op"] in textual_operators:
                    f = db.func.jsonb_array_elements_text

                if _filter["val"]["op"] in other_not_to_be_cast:
                    other = _filter["val"]["val"]

                f = f(Metadata.data[list(_filter["name"].split("."))]).alias()
                operator = filter_operators[_filter["val"]["op"]]

                e = exists(
                    #  XXX select 1
                    db.select("*")
                    .select_from(f)
                    .where(operator(column(f.name), other))
                )

                query = query.where(e)

                return query.offset(offset).limit(limit)

            elif _filter["op"] == ":all":
                count_f = db.func.jsonb_array_length(json_object)
                f = db.func.jsonb_array_elements

                if _filter["val"]["op"] in textual_operators:
                    f = db.func.jsonb_array_elements_text
                f = f(json_object).alias()

                operator = filter_operators[_filter["val"]["op"]]

                if _filter["val"]["op"] in other_not_to_be_cast:
                    other = _filter["val"]["val"]

                sq = (
                    db.select([db.func.count()])
                    .select_from(f)
                    .where(operator(column(f.name), other))
                )
                query = query.where(count_f == sq)

                return query.offset(offset).limit(limit)

            #  elif _filter["op"] == "is":

            if _filter["op"] in textual_operators:
                json_object = json_object.astext
                other = _filter["val"]

            query = query.where(operator(json_object, other))
            #  query = query.where(db.or_(operator(json_object, other), ColumnOperators.__eq__(json_object, cast("cats_rock", JSONB))))
            #  query = query.where(db.and_(operator(json_object, other), ColumnOperators.__eq__(json_object, cast("cats_rock", JSONB))))
            #  query = query.where(
            #  db.and_(
            #  db.or_(
            #  operator(json_object, other),
            #  operator(json_object, "cats_rock"),
            #  ),
            #  db.or_(
            #  operator(json_object, "%"),
            #  )
            #  )
            #  )

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
