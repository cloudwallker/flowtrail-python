import unittest

from flowtrail.models import render


class TemplateBudgetTests(unittest.TestCase):
    def test_unicode_output_budget_counts_bytes_not_characters(self):
        self.assertEqual(render("a${inputs.word}", {"word": "中"}, {}, max_bytes=4), "a中")
        with self.assertRaisesRegex(ValueError, "byte limit"):
            render("a${inputs.word}", {"word": "中"}, {}, max_bytes=3)

    def test_repeated_placeholder_expansion_is_bounded(self):
        with self.assertRaisesRegex(ValueError, "byte limit"):
            render("${inputs.payload}" * 200, {"payload": "x" * 32768}, {}, max_bytes=262144)

    def test_literal_and_inserted_values_share_one_budget(self):
        with self.assertRaisesRegex(ValueError, "byte limit"):
            render("${inputs.x}tail", {"x": "1234"}, {}, max_bytes=7)


if __name__ == "__main__":
    unittest.main()
