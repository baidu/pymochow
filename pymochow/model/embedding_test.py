"""Unit tests for embedding params and text ANN search serialization."""
import unittest

from pymochow.model.schema import EmbeddingParams, Schema
from pymochow.model.table import BatchTextAnnSearch, TextAnnSearch, FLATSearchParams


class EmbeddingModelTest(unittest.TestCase):
    """Test cases for embedding schema and text search dict serialization."""

    def test_schema_and_text_search_dict(self):
        """Verify schema and text search objects serialize to expected dicts.

        Args:
            self: The test case instance.

        Returns:
            None.

        Raises:
            AssertionError: If any serialized dict does not match the expected value.
        """
        schema = Schema([], [], [EmbeddingParams("text", "text_vector", "qwen3-embedding-0.6b")])
        self.assertEqual(schema.to_dict()["embeddingParams"][0]["modelName"], "qwen3-embedding-0.6b")
        search = TextAnnSearch("text", "hello", FLATSearchParams(limit=5), "text_vector")
        self.assertEqual(search.to_dict()["text"], "hello")
        batch = BatchTextAnnSearch("text", ["hello", "world"], FLATSearchParams(limit=5))
        self.assertEqual(batch.to_dict()["texts"], ["hello", "world"])
