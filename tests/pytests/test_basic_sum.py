"""Basic summation Pytest example"""
from spark_example_proj.basic_calculations.basic_summation import increment_by_two


def test_increment_by_two_when_input_is_4_then_output_6():
    assert increment_by_two(4) == 6


