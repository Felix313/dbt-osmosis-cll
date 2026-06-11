# pyright: reportUnknownVariableType=false, reportPrivateImportUsage=false, reportUnknownMemberType=false
"""Pattern-based test suggestion for dbt models.

This module provides functionality to:
1. Analyze existing test patterns in a dbt project
2. Learn team conventions from existing tests
3. Suggest appropriate tests for models based on those patterns
"""

from __future__ import annotations

import typing as t
from collections import Counter, defaultdict
from dataclasses import dataclass, field

from dbt_osmosis_cll.osmosis_propagation.introspection import PropertyAccessor
from dbt_osmosis_cll.osmosis_propagation.settings import YamlRefactorContext

__all__ = [
    "AITestSuggester",
    "TestPatternExtractor",
    "TestSuggester",
    "TestSuggestion",
    "suggest_tests_for_model",
    "suggest_tests_for_project",
]


@dataclass
class TestSuggestion:
    """A single test suggestion for a column or model."""

    __test__ = False

    test_type: str  # e.g., "unique", "not_null", "relationships", "accepted_values"
    column_name: str | None = None  # None for model-level tests
    reason: str = ""
    config: dict[str, t.Any] = field(default_factory=dict)
    confidence: float = 1.0  # 0.0 to 1.0

    def to_yaml_dict(self) -> dict[str, t.Any]:
        """Convert to YAML-serializable dict."""
        return {self.test_type: self.config or {}}

    def __repr__(self) -> str:
        if self.column_name:
            return f"TestSuggestion({self.column_name}: {self.test_type})"
        return f"TestSuggestion(model: {self.test_type})"


@dataclass
class ModelTestAnalysis:
    """Analysis of test patterns for a single model."""

    model_name: str
    columns: list[str]
    existing_tests: dict[str, list[TestSuggestion]]
    suggested_tests: dict[str, list[TestSuggestion]] = field(default_factory=dict)

    def get_test_summary(self) -> dict[str, t.Any]:
        """Get a summary of tests for this model."""
        return {
            "model_name": self.model_name,
            "total_columns": len(self.columns),
            "columns_with_tests": len(self.existing_tests),
            "total_existing_tests": sum(len(tests) for tests in self.existing_tests.values()),
            "total_suggested_tests": sum(len(tests) for tests in self.suggested_tests.values()),
        }


def _iter_node_columns(node: t.Any) -> t.Iterator[tuple[str, t.Any]]:
    """Yield node columns regardless of whether dbt exposes a dict or list shape."""

    columns = getattr(node, "columns", {})
    if isinstance(columns, dict):
        yield from columns.items()
        return

    for column in columns or []:
        yield getattr(column, "name", ""), column


def _get_manifest_test_name(test_metadata: t.Any) -> str:
    """Return a stable dbt test name, including namespace when present."""

    name = getattr(test_metadata, "name", "")
    namespace = getattr(test_metadata, "namespace", None)
    return f"{namespace}.{name}" if namespace else name


def _get_manifest_test_config(test_node: t.Any) -> dict[str, t.Any]:
    """Strip manifest-only plumbing fields from a generic test definition."""

    test_metadata = getattr(test_node, "test_metadata", None)
    kwargs = dict(getattr(test_metadata, "kwargs", {}) or {})
    kwargs.pop("column_name", None)
    kwargs.pop("model", None)
    return kwargs


def _get_existing_tests_for_node(
    manifest: t.Any,
    node: t.Any,
) -> dict[str, list[TestSuggestion]]:
    """Collect generic tests attached to a model from real dbt manifest test nodes."""

    from dbt.artifacts.resources.types import NodeType

    unique_id = getattr(node, "unique_id", None)
    if unique_id is None or not hasattr(manifest, "nodes"):
        return {}

    attached_tests: defaultdict[str, list[TestSuggestion]] = defaultdict(list)
    for manifest_node in manifest.nodes.values():
        if getattr(manifest_node, "resource_type", None) != NodeType.Test:
            continue

        attached_node = getattr(manifest_node, "attached_node", None)
        if attached_node is not None:
            if attached_node != unique_id:
                continue
        else:
            depends_on = getattr(getattr(manifest_node, "depends_on", None), "nodes", []) or []
            if unique_id not in depends_on:
                continue

        column_name = getattr(manifest_node, "column_name", None)
        test_metadata = getattr(manifest_node, "test_metadata", None)
        test_name = _get_manifest_test_name(test_metadata)
        if not column_name or not test_name:
            continue

        attached_tests[column_name].append(
            TestSuggestion(
                test_type=test_name,
                column_name=column_name,
                config=_get_manifest_test_config(manifest_node),
                confidence=1.0,
            )
        )

    return dict(attached_tests)


class TestPatternExtractor:
    """Extracts and learns test patterns from a dbt project.

    This class analyzes existing tests to understand:
    - Which test types are commonly used
    - Column naming patterns that trigger specific tests
    - Test configuration patterns
    """

    __test__ = False

    def __init__(self, context: YamlRefactorContext) -> None:
        """Initialize the extractor with a dbt project context.

        Args:
            context: The YamlRefactorContext containing project information
        """
        self.context = context
        self.accessor = PropertyAccessor(context=context)

        # Track patterns across the project
        self.column_pattern_tests: defaultdict[str, Counter[str]] = defaultdict(Counter)
        self.data_type_tests: defaultdict[str, Counter[str]] = defaultdict(Counter)
        self.test_frequency: Counter[str] = Counter()
        self.relationship_patterns: list[dict[str, t.Any]] = []

        # Common patterns learned from the project
        self.learned_patterns: dict[str, t.Any] = {}

    def extract_patterns(self) -> None:
        """Extract test patterns from all nodes in the project."""
        from dbt.artifacts.resources.types import NodeType

        manifest = self.context.project.manifest
        for node in manifest.nodes.values():
            # Only process model nodes
            if getattr(node, "resource_type", None) != NodeType.Model:
                continue
            self._analyze_node(node)

        self._learn_patterns()

    def _analyze_node(self, node: t.Any) -> None:
        """Analyze a single node's test patterns."""
        if not hasattr(node, "columns"):
            return

        existing_tests = _get_existing_tests_for_node(self.context.project.manifest, node)
        for col_name, column in _iter_node_columns(node):
            tests = existing_tests.get(col_name, [])

            if not tests:
                continue

            # Extract column naming pattern (e.g., "*_id", "*_date", "status")
            base_pattern = self._get_column_pattern(col_name)

            # Get data type if available
            data_type = getattr(column, "data_type", None)

            for test in tests:
                test_payload = test.to_yaml_dict() if test.config else test.test_type
                self._analyze_test(test_payload, col_name, base_pattern, data_type)

    def _get_column_pattern(self, column_name: str) -> str:
        """Extract a naming pattern from a column name.

        Examples:
            "order_id" -> "*_id"
            "order_date" -> "*_date"
            "status" -> "status"
            "is_active" -> "is_*"
        """
        # Common suffixes
        for suffix in [
            "_id",
            "_id",
            "_date",
            "_time",
            "_at",
            "_ts",
            "_amount",
            "_count",
            "_flag",
            "_bool",
        ]:
            if column_name.endswith(suffix):
                return f"*{suffix}"

        # Common prefixes
        for prefix in ["is_", "has_", "can_", "should_"]:
            if column_name.startswith(prefix):
                return f"{prefix}*"

        # Return the name itself for specific columns like "status", "type"
        return column_name

    def _analyze_test(
        self, test: t.Any, col_name: str, pattern: str, data_type: str | None
    ) -> None:
        """Analyze a single test definition."""
        if isinstance(test, str):
            # Simple test like "unique" or "not_null"
            self.column_pattern_tests[pattern][test] += 1
            self.test_frequency[test] += 1
            if data_type:
                self.data_type_tests[data_type][test] += 1

        elif isinstance(test, dict):
            # Complex test with config
            for test_name, config in test.items():
                self.column_pattern_tests[pattern][test_name] += 1
                self.test_frequency[test_name] += 1
                if data_type:
                    self.data_type_tests[data_type][test_name] += 1

                # Track relationship patterns
                if test_name == "relationships":
                    self.relationship_patterns.append({
                        "column_pattern": pattern,
                        "config": config,
                    })

    def _learn_patterns(self) -> None:
        """Learn patterns from extracted data."""
        self.learned_patterns = {
            "id_column_tests": self._get_top_tests_for_pattern("*_id"),
            "date_column_tests": self._get_top_tests_for_pattern("*_date"),
            "amount_column_tests": self._get_top_tests_for_pattern("*_amount"),
            "status_column_tests": self._get_top_tests_for_pattern("status"),
            "is_column_tests": self._get_top_tests_for_pattern("is_*"),
            "common_tests": dict(self.test_frequency.most_common(10)),
            "data_type_tests": {
                dt: dict(tests.most_common(3)) for dt, tests in self.data_type_tests.items()
            },
        }

    def _get_top_tests_for_pattern(self, pattern: str, top_n: int = 5) -> list[str]:
        """Get the most common tests for a column pattern."""
        if pattern not in self.column_pattern_tests:
            return []
        return [test for test, _ in self.column_pattern_tests[pattern].most_common(top_n)]

    def get_suggestions_for_column(
        self, column_name: str, data_type: str | None = None
    ) -> list[TestSuggestion]:
        """Get test suggestions for a column based on learned patterns.

        Args:
            column_name: The name of the column
            data_type: Optional data type of the column

        Returns:
            List of TestSuggestion objects
        """
        suggestions: list[TestSuggestion] = []
        pattern = self._get_column_pattern(column_name)

        # Get pattern-based suggestions
        pattern_tests: list[str] = []
        if pattern == "*_id":
            pattern_tests = list(self.learned_patterns.get("id_column_tests", []))
        elif pattern == "*_date":
            pattern_tests = list(self.learned_patterns.get("date_column_tests", []))
        elif pattern == "*_amount":
            pattern_tests = list(self.learned_patterns.get("amount_column_tests", []))
        elif pattern == "status":
            pattern_tests = list(self.learned_patterns.get("status_column_tests", []))
        elif pattern.startswith("is_"):
            pattern_tests = list(self.learned_patterns.get("is_column_tests", []))

        # Get data type based suggestions
        if data_type:
            dt_tests = self.learned_patterns.get("data_type_tests", {}).get(data_type, [])
            pattern_tests.extend(dt_tests)

        # Create suggestions
        for test_type in pattern_tests:
            # Skip if it's already in suggestions
            if any(s.test_type == test_type for s in suggestions):
                continue

            suggestion = TestSuggestion(
                test_type=test_type,
                column_name=column_name,
                reason=f"Commonly used for columns matching pattern '{pattern}'",
                confidence=0.7,
            )
            suggestions.append(suggestion)

        return suggestions


class TestSuggester:
    """Pattern-based test suggestion using learned project conventions."""

    __test__ = False

    def __init__(
        self,
        context: YamlRefactorContext,
        pattern_extractor: TestPatternExtractor | None = None,
    ) -> None:
        """Initialize the test suggester.

        Args:
            context: The YamlRefactorContext containing project information
            pattern_extractor: Optional TestPatternExtractor with learned patterns
        """
        self.context = context
        self.pattern_extractor = pattern_extractor
        self.accessor = PropertyAccessor(context=context)

    def suggest_tests_for_node(self, node: t.Any) -> ModelTestAnalysis:
        """Generate test suggestions for a single node.

        Args:
            node: The dbt model node

        Returns:
            ModelTestAnalysis with existing and suggested tests
        """
        model_name = getattr(node, "name", "unknown")
        columns = [column_name for column_name, _ in _iter_node_columns(node)]
        existing_tests = _get_existing_tests_for_node(self.context.project.manifest, node)

        analysis = ModelTestAnalysis(
            model_name=model_name,
            columns=columns,
            existing_tests=existing_tests,
        )

        analysis.suggested_tests = self._pattern_suggest_tests(node)
        return analysis

    def _pattern_suggest_tests(self, node: t.Any) -> dict[str, list[TestSuggestion]]:
        """Generate test suggestions based on learned patterns."""
        suggestions: dict[str, list[TestSuggestion]] = defaultdict(list)

        if not self.pattern_extractor:
            return suggestions

        existing_tests = _get_existing_tests_for_node(self.context.project.manifest, node)
        for col_name, column in _iter_node_columns(node):
            data_type = getattr(column, "data_type", None)

            col_suggestions = self.pattern_extractor.get_suggestions_for_column(col_name, data_type)

            # Filter out already existing tests
            existing = {test.test_type for test in existing_tests.get(col_name, [])}
            for suggestion in col_suggestions:
                if suggestion.test_type not in existing:
                    suggestions[col_name].append(suggestion)

        return dict(suggestions)

# Backwards-compatible alias from when this class had an LLM-backed mode.
AITestSuggester = TestSuggester


def suggest_tests_for_model(
    context: YamlRefactorContext,
    node: t.Any,
) -> ModelTestAnalysis:
    """Suggest tests for a single model.

    This is a convenience function that creates the necessary components
    and returns test suggestions for a model.

    Args:
        context: The YamlRefactorContext containing project information
        node: The dbt model node to analyze

    Returns:
        ModelTestAnalysis with existing and suggested tests
    """
    extractor = TestPatternExtractor(context)
    extractor.extract_patterns()

    suggester = TestSuggester(context, extractor)
    return suggester.suggest_tests_for_node(node)


def suggest_tests_for_project(
    context: YamlRefactorContext,
) -> dict[str, ModelTestAnalysis]:
    """Suggest tests for all models in a project.

    This is a convenience function that analyzes all models in a project
    and returns test suggestions for each.

    Args:
        context: The YamlRefactorContext containing project information

    Returns:
        Dictionary mapping model names to ModelTestAnalysis objects
    """
    # Extract patterns once for the entire project
    extractor = TestPatternExtractor(context)
    extractor.extract_patterns()

    suggester = TestSuggester(context, extractor)

    from dbt.artifacts.resources.types import NodeType

    results: dict[str, ModelTestAnalysis] = {}
    manifest = context.project.manifest
    for node in manifest.nodes.values():
        # Only process model nodes
        if getattr(node, "resource_type", None) != NodeType.Model:
            continue
        model_name = getattr(node, "name", "unknown")
        analysis = suggester.suggest_tests_for_node(node)
        results[model_name] = analysis

    return results
