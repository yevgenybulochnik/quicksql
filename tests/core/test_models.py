"""Tests for the models module."""

import pytest
from quicksql.core.models import (
    ConnectionResolver,
    CellConfig,
    InputConfig,
    ConfigSchemaRegistry,
)
from quicksql.core.errors import QsqlConfigError, CellValidationError


class TestConnectionResolver:
    """Tests for ConnectionResolver."""

    def test_resolve_duckdb_ddb_extension(self):
        """Test resolving .ddb extension to duckdb."""
        assert ConnectionResolver.resolve("./test.ddb") == "duckdb"

    def test_resolve_duckdb_duckdb_extension(self):
        """Test resolving .duckdb extension to duckdb."""
        assert ConnectionResolver.resolve("./test.duckdb") == "duckdb"

    def test_resolve_duckdb_memory(self):
        """Test resolving :memory: to duckdb."""
        assert ConnectionResolver.resolve(":memory:") == "duckdb"

    def test_resolve_duckdb_case_insensitive(self):
        """Test that extension matching is case insensitive."""
        assert ConnectionResolver.resolve("./TEST.DDB") == "duckdb"
        assert ConnectionResolver.resolve("./Test.DuckDB") == "duckdb"

    def test_resolve_unknown_raises(self):
        """Test that unknown extensions raise ValueError."""
        with pytest.raises(ValueError, match="Cannot infer backend"):
            ConnectionResolver.resolve("./data.xyz")

    def test_resolve_with_path(self):
        """Test resolving paths with directories."""
        assert ConnectionResolver.resolve("/path/to/database.ddb") == "duckdb"
        assert ConnectionResolver.resolve("../relative/path.duckdb") == "duckdb"


class TestCellConfig:
    """Tests for CellConfig Pydantic model."""

    def test_parse_simple_string_input(self):
        """Test parsing simple string input (inferred backend)."""
        config = CellConfig.model_validate({"input": "./test.ddb"})
        assert config.input is not None
        assert config.input.backend_name == "duckdb"
        assert config.input.connection_string == "./test.ddb"

    def test_parse_explicit_dict_input(self):
        """Test parsing explicit dict input."""
        config = CellConfig.model_validate({"input": {"duckdb": "./data.db"}})
        assert config.input is not None
        assert config.input.backend_name == "duckdb"
        assert config.input.connection_string == "./data.db"

    def test_parse_no_input(self):
        """Test parsing config without input."""
        config = CellConfig.model_validate({})
        assert config.input is None

    def test_parse_memory_database(self):
        """Test parsing :memory: connection string."""
        config = CellConfig.model_validate({"input": ":memory:"})
        assert config.input is not None
        assert config.input.backend_name == "duckdb"
        assert config.input.connection_string == ":memory:"

    def test_invalid_input_type_raises(self):
        """Test that invalid input types raise validation error."""
        with pytest.raises(ValueError, match="Invalid input format"):
            CellConfig.model_validate({"input": 123})

    def test_multiple_backends_in_explicit_raises(self):
        """Test that multiple backends in explicit format raises error."""
        with pytest.raises(ValueError, match="exactly one backend"):
            CellConfig.model_validate(
                {"input": {"duckdb": "./test.ddb", "postgres": "postgres://"}}
            )

    def test_unknown_backend_in_string_raises(self):
        """Test that unknown backend inference raises error."""
        with pytest.raises(ValueError, match="Cannot infer backend"):
            CellConfig.model_validate({"input": "./unknown.xyz"})


class TestInputConfig:
    """Tests for InputConfig model."""

    def test_create_input_config(self):
        """Test creating InputConfig directly."""
        config = InputConfig(backend_name="duckdb", connection_string="./test.ddb")
        assert config.backend_name == "duckdb"
        assert config.connection_string == "./test.ddb"


class TestConfigSchemaRegistry:
    """Tests for ConfigSchemaRegistry."""

    def test_get_registered_schema(self):
        """Test getting a registered schema."""
        schema = ConfigSchemaRegistry.get_schema("duckdb")
        assert schema is not None

    def test_get_unregistered_schema_raises(self):
        """Test that getting unregistered schema raises error."""
        with pytest.raises(ValueError, match="No config schema registered"):
            ConfigSchemaRegistry.get_schema("nonexistent")

    def test_get_all_schemas(self):
        """Test getting all registered schemas."""
        schemas = ConfigSchemaRegistry.get_all_schemas()
        assert "duckdb" in schemas


class TestQsqlConfigError:
    """Tests for QsqlConfigError."""

    def test_error_formatting(self, tmp_path):
        """Test error message formatting."""
        error = QsqlConfigError(
            file_path=tmp_path / "test.sql",
            errors=[
                CellValidationError(
                    cell_name="query_1",
                    cell_start_line=4,
                    field_path="input",
                    message="Cannot infer backend",
                    invalid_value="./unknown.xyz",
                ),
                CellValidationError(
                    cell_name="query_2",
                    cell_start_line=10,
                    field_path="input.duckdb",
                    message="Path does not exist",
                    invalid_value="/missing/path.ddb",
                ),
            ],
        )

        error_str = str(error)
        assert "query_1" in error_str
        assert "line 5" in error_str  # cell_start_line + 1
        assert "query_2" in error_str
        assert "line 11" in error_str
        assert "Cannot infer backend" in error_str

    def test_has_errors(self, tmp_path):
        """Test has_errors method."""
        error = QsqlConfigError(file_path=tmp_path / "test.sql")
        assert not error.has_errors()

        error.add_error(
            CellValidationError(
                cell_name="query_1",
                cell_start_line=0,
                field_path="input",
                message="test",
            )
        )
        assert error.has_errors()

    def test_add_error(self, tmp_path):
        """Test add_error method."""
        error = QsqlConfigError(file_path=tmp_path / "test.sql")
        assert len(error.errors) == 0

        error.add_error(
            CellValidationError(
                cell_name="query_1",
                cell_start_line=0,
                field_path="input",
                message="test error",
            )
        )
        assert len(error.errors) == 1
        assert error.errors[0].message == "test error"
