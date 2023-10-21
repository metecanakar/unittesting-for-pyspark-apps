"""Basic summation unittest example"""
import unittest

from spark_example_proj.basic_calculations.basic_summation import increment_by_two


class TestBasicSum(unittest.TestCase):
    def test_increment_by_two_when_input_is_4_then_output_6(self):
        self.assertEqual(6, increment_by_two(4))


if __name__ == "__main__":
    unittest.main()
