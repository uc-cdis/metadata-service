import json

from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor

#  json_value and below was taken from
#  https://github.com/erikrose/parsimonious/pull/23/files
filter_param_grammar = Grammar(
    """
filter           = scalar_filter / compound_filter / boolean_filter
boolean_filter   = open boolean boolean_operands close
boolean          = "and" / "or"
boolean_operands = boolean_operand+
boolean_operand = separator specific_filter
specific_filter = scalar_filter / compound_filter / boolean_filter
scalar_filter   = open json_key separator scalar_operator separator json_value close
compound_filter   = open json_key separator compound_operator separator scalar_filter close

json_key        = ~"[A-Z 0-9_.]*"i
scalar_operator = ":eq" / ":ne" / ":gte" / ":gt" / ":lte" / ":lt" / ":like"
compound_operator = ":all" / ":any"
open            = "("
close           = ")"
separator       = ","

json_value = double_string / true_false_null / number

double_string   = ~'"([^"])*"'
true_false_null = "true" / "false" / "null"
number = (int frac exp) / (int exp) / (int frac) / int

int = "-"? ((digit1to9 digits) / digit)
frac = "." digits
exp = e digits
digits = digit+
e = "e+" / "e-" / "e" / "E+" / "E-" / "E"
digit1to9 = ~"[1-9]"
digit = ~"[0-9]"
"""
)


class FilterVisitor(NodeVisitor):
    def visit_filter(self, node, visited_children):
        return visited_children[0]

    def visit_boolean_filter(self, node, visited_children):
        _, boolean, boolean_operands, _ = visited_children
        return {boolean: boolean_operands}

    def visit_boolean(self, node, visited_children):
        return node.text

    def visit_boolean_operand(self, node, visited_children):
        _, specific_filter = visited_children
        return specific_filter

    def visit_specific_filter(self, node, visited_children):
        return visited_children[0]

    def visit_scalar_filter(self, node, visited_children):
        (
            _,
            json_key,
            _,
            operator,
            _,
            json_value,
            _,
        ) = visited_children

        return {
            "name": json_key,
            "op": operator,
            "val": json_value,
        }

    def visit_compound_filter(self, node, visited_children):
        (
            _,
            json_key,
            _,
            operator,
            _,
            scalar_filter,
            _,
        ) = visited_children

        return {
            "name": json_key,
            "op": operator,
            "val": scalar_filter,
        }

    def visit_json_key(self, node, visited_children):
        return node.text

    def visit_scalar_operator(self, node, visited_children):
        return node.text

    def visit_compound_operator(self, node, visited_children):
        return node.text

    def visit_json_value(self, node, visited_children):
        return json.loads(node.text)

    def generic_visit(self, node, visited_children):
        return visited_children or node
