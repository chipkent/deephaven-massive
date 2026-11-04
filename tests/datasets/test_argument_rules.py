"""Tests of the shared argument rules that the dataset reference prints on each page."""

from deephaven_massive._dataset_list import dataset_calls
from deephaven_massive._datasets.argument_rules import ARGUMENT_RULES


def test_every_rule_belongs_to_an_argument_several_datasets_take() -> None:
    # A rule for a name no method takes, or only one, would be printed nowhere or repeat a
    # docstring line; a misspelled key would leave its argument without a rule.
    counts: dict[str, int] = {}
    for call in dataset_calls().values():
        for name in call.arguments:
            counts[name] = counts.get(name, 0) + 1
    assert {name: counts.get(name, 0) for name in ARGUMENT_RULES if counts.get(name, 0) < 2} == {}


def test_the_most_shared_arguments_have_rules() -> None:
    assert {
        "symbols",
        "start",
        "end",
        "sort",
        "order",
        "limit",
        "filters",
        "adjusted",
        "timespan",
        "multiplier",
    } <= ARGUMENT_RULES.keys()


def test_every_rule_fits_one_table_cell() -> None:
    # Each rule is one Markdown cell: sentences, with no line break or column separator.
    bad = {
        name: rule
        for name, rule in ARGUMENT_RULES.items()
        if "\n" in rule or "|" in rule or not rule.endswith(".")
    }
    assert bad == {}
