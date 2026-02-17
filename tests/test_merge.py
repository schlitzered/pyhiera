import os
import shutil
import tempfile
import unittest
from typing import Optional

from pydantic import BaseModel

from pyhiera import PyHieraAsync
from pyhiera import PyHieraSync
from pyhiera import PyHieraBackendYamlAsync
from pyhiera import PyHieraBackendYamlSync
from pyhiera import PyHieraKeyBase
from pyhiera import PyHieraModelDataBase
from pyhiera.errors import PyHieraBackendError
from pyhiera.errors import PyHieraError


class PyHieraKeyDataDictModel(PyHieraModelDataBase):
    """Dynamic dict model for testing merge functionality"""
    data: dict


class PyHieraKeyDict(PyHieraKeyBase):
    def __init__(self):
        super().__init__()
        self._description = "dynamic dict"
        self._model = PyHieraKeyDataDictModel


class PyHieraKeyDataComplexLevelB(BaseModel):
    blarg: Optional[str] = None
    other: Optional[str] = None
    blub: Optional[set[str]] = None


class PyHieraKeyDataComplexLevel(BaseModel):
    a: Optional[str] = None
    b: Optional[PyHieraKeyDataComplexLevelB] = None


class PyHieraKeyDataComplex(PyHieraModelDataBase):
    data: PyHieraKeyDataComplexLevel


class PyHieraKeyComplex(PyHieraKeyBase):
    def __init__(self):
        super().__init__()
        self._description = "complex data"
        self._model = PyHieraKeyDataComplex


class TestPyHieraSyncMerge(unittest.TestCase):
    """Test cases for PyHieraSync merge functionality"""

    def setUp(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.hierarchy = [
            "environment/{environment}.yaml",
            "common.yaml",
        ]

        self.backend = PyHieraBackendYamlSync(
            identifier="test_backend",
            priority=1,
            config={"path": self.test_dir},
            hierarchy=self.hierarchy,
        )

        self.pyhiera = PyHieraSync()
        self.pyhiera.key_model_add(key="DynamicDict", model=PyHieraKeyDict)
        self.pyhiera.key_model_add(key="Complex", model=PyHieraKeyComplex)
        self.pyhiera.backend_add(self.backend)

    def tearDown(self):
        """Clean up test fixtures"""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_key_models_property(self):
        """Test accessing key_models property"""
        models = self.pyhiera.key_models
        self.assertIsInstance(models, dict)
        self.assertIn("DynamicDict", models)
        self.assertIn("Complex", models)

    def test_keys_property(self):
        """Test accessing keys property"""
        self.pyhiera.key_add(key="test_key", hiera_key="SimpleString")
        keys = self.pyhiera.keys
        self.assertIsInstance(keys, dict)
        self.assertIn("test_key", keys)

    def test_key_model_delete(self):
        """Test deleting a key model"""
        self.pyhiera.key_model_add(key="TestModel", model=PyHieraKeyDict)
        self.assertIn("TestModel", self.pyhiera.key_models)

        self.pyhiera.key_model_delete(key="TestModel")
        self.assertNotIn("TestModel", self.pyhiera.key_models)

    def test_key_model_delete_error(self):
        """Test deleting non-existent key model raises error"""
        with self.assertRaises(PyHieraError) as context:
            self.pyhiera.key_model_delete(key="NonExistentModel")
        self.assertIn("Key model NonExistentModel not found", str(context.exception))

    def test_key_model_get_error(self):
        """Test getting non-existent key model raises error"""
        pyhiera = PyHieraSync()
        with self.assertRaises(PyHieraError) as context:
            pyhiera.key_add(key="test", hiera_key="NonExistentModel")
        self.assertIn("Invalid key model NonExistentModel", str(context.exception))

    def test_key_delete(self):
        """Test deleting a key"""
        self.pyhiera.key_add(key="test_key", hiera_key="SimpleString")
        self.assertIn("test_key", self.pyhiera.keys)

        self.pyhiera.key_delete(key="test_key")
        self.assertNotIn("test_key", self.pyhiera.keys)

    def test_key_delete_error(self):
        """Test deleting non-existent key raises error"""
        with self.assertRaises(PyHieraError) as context:
            self.pyhiera.key_delete(key="NonExistentKey")
        self.assertIn("Key NonExistentKey not found", str(context.exception))

    def test_backend_delete(self):
        """Test deleting a backend"""
        self.pyhiera.backend_delete(identifier="test_backend")
        with self.assertRaises(PyHieraError):
            # Try to use deleted backend
            self.pyhiera.key_add(key="config", hiera_key="DynamicDict")
            self.pyhiera.key_data_add(
                backend_identifier="test_backend",
                key="config",
                data={"a": 1},
                level="common.yaml",
                facts={},
            )

    def test_backend_delete_error(self):
        """Test deleting non-existent backend raises error"""
        with self.assertRaises(PyHieraError) as context:
            self.pyhiera.backend_delete(identifier="NonExistentBackend")
        self.assertIn("Backend with identifier NonExistentBackend not found", str(context.exception))

    def test_backend_add_duplicate_identifier_error(self):
        """Test adding backend with duplicate identifier raises error"""
        backend2 = PyHieraBackendYamlSync(
            identifier="test_backend",  # Same identifier
            priority=2,
            config={"path": self.test_dir},
            hierarchy=self.hierarchy,
        )
        with self.assertRaises(PyHieraError) as context:
            self.pyhiera.backend_add(backend2)
        self.assertIn("already exists", str(context.exception))

    def test_backend_add_duplicate_priority_error(self):
        """Test adding backend with duplicate priority raises error"""
        backend2 = PyHieraBackendYamlSync(
            identifier="test_backend_2",
            priority=1,  # Same priority as existing backend
            config={"path": self.test_dir},
            hierarchy=self.hierarchy,
        )
        with self.assertRaises(PyHieraError) as context:
            self.pyhiera.backend_add(backend2)
        self.assertIn("cannot use priority", str(context.exception))

    def test_key_data_validate_error(self):
        """Test key_data_validate with invalid data raises error"""
        self.pyhiera.key_add(key="int_key", hiera_key="SimpleInt")
        with self.assertRaises(PyHieraError) as context:
            self.pyhiera.key_data_validate(key="int_key", data="not_an_int")
        self.assertIn("Invalid data for key int_key", str(context.exception))

    def test_key_data_validate_key_not_found_error(self):
        """Test key_data_validate with non-existent key raises error"""
        with self.assertRaises(PyHieraError) as context:
            self.pyhiera.key_data_validate(key="nonexistent_key", data={"a": 1})
        self.assertIn("Key nonexistent_key not found", str(context.exception))

    def test_key_data_get_non_merge(self):
        """Test key_data_get (non-merge) method"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"timeout": 30},
            level="common.yaml",
            facts={},
        )

        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"timeout": 60},
            level="environment/{environment}.yaml",
            facts={"environment": "prod"},
        )

        # Get without merge - should return first match
        result = self.pyhiera.key_data_get(
            key="config",
            facts={"environment": "prod"},
            include_sources=True,
        )

        self.assertEqual(result.data["timeout"], 60)  # First match (most specific)
        self.assertIsNotNone(result.sources)

    def test_key_data_get_non_merge_without_sources(self):
        """Test key_data_get without sources"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"value": 123},
            level="environment/{environment}.yaml",
            facts={"environment": "test"},
        )

        result = self.pyhiera.key_data_get(
            key="config",
            facts={"environment": "test"},
            include_sources=False,
        )

        self.assertEqual(result.data["value"], 123)
        self.assertIsNone(result.sources)

    def test_key_data_get_not_found_error(self):
        """Test key_data_get raises error when no data found"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        with self.assertRaises(PyHieraBackendError) as context:
            self.pyhiera.key_data_get(
                key="config",
                facts={"environment": "nonexistent"},
                include_sources=False,
            )

        self.assertIn("No data found", str(context.exception))

    def test_key_data_get_invalid_key_error(self):
        """Test key_data_get with invalid key raises error"""
        with self.assertRaises(PyHieraError) as context:
            self.pyhiera.key_data_get(
                key="nonexistent_key",
                facts={},
                include_sources=False,
            )

        self.assertIn("Key nonexistent_key not found", str(context.exception))

    def test_merge_dict_data_across_hierarchy(self):
        """Test Bug 1: Merge dict data across multiple hierarchy levels"""
        # Add key
        self.pyhiera.key_add(key="config_data", hiera_key="DynamicDict")

        # Add data at common level
        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config_data",
            data={"timeout": 30, "retries": 3, "cache": True},
            level="common.yaml",
            facts={},
        )

        # Add data at environment level (prod)
        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config_data",
            data={"timeout": 60, "ssl": True},
            level="environment/{environment}.yaml",
            facts={"environment": "prod"},
        )

        # Test merge
        result = self.pyhiera.key_data_get_merge(
            key="config_data",
            facts={"environment": "prod"},
            include_sources=False,
        )

        # Verify merged data
        self.assertIsInstance(result.data, dict)
        self.assertEqual(result.data["timeout"], 60)  # Overridden by prod
        self.assertEqual(result.data["retries"], 3)  # From common
        self.assertEqual(result.data["cache"], True)  # From common
        self.assertEqual(result.data["ssl"], True)  # From prod

    def test_merge_with_include_sources_true(self):
        """Test merge with include_sources=True returns sources"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        # Add data at both levels
        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"a": 1, "b": 2},
            level="common.yaml",
            facts={},
        )

        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"a": 10},
            level="environment/{environment}.yaml",
            facts={"environment": "dev"},
        )

        # Test with sources
        result = self.pyhiera.key_data_get_merge(
            key="config",
            facts={"environment": "dev"},
            include_sources=True,
        )

        # Verify data
        self.assertEqual(result.data["a"], 10)
        self.assertEqual(result.data["b"], 2)

        # Verify sources are included
        self.assertIsNotNone(result.sources)
        self.assertEqual(len(result.sources), 2)
        self.assertEqual(result.sources[0].level, "environment/dev.yaml")
        self.assertEqual(result.sources[1].level, "common.yaml")

    def test_merge_with_include_sources_false(self):
        """Test merge with include_sources=False does not return sources"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"x": 100},
            level="common.yaml",
            facts={},
        )

        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"y": 200},
            level="environment/{environment}.yaml",
            facts={"environment": "test"},
        )

        result = self.pyhiera.key_data_get_merge(
            key="config",
            facts={"environment": "test"},
            include_sources=False,
        )

        # Verify data
        self.assertEqual(result.data["x"], 100)
        self.assertEqual(result.data["y"], 200)

        # Verify sources are not included
        self.assertIsNone(result.sources)

    def test_merge_non_dict_data_raises_error(self):
        """Test that merging non-dict data raises an error"""
        self.pyhiera.key_add(key="simple_string", hiera_key="SimpleString")

        # Add string data (not dict)
        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="simple_string",
            data="test_value",
            level="common.yaml",
            facts={},
        )

        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="simple_string",
            data="test_value2",
            level="environment/{environment}.yaml",
            facts={"environment": "test"},
        )

        # Test that merge raises error for non-dict data
        with self.assertRaises(PyHieraBackendError) as context:
            self.pyhiera.key_data_get_merge(
                key="simple_string",
                facts={"environment": "test"},
                include_sources=False,
            )

        self.assertIn("expected dict", str(context.exception))

    def test_merge_complex_nested_dicts(self):
        """Test merge with complex nested dictionary structures"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        # Add complex nested data at common level
        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={
                "database": {
                    "host": "localhost",
                    "port": 5432,
                    "settings": {"timeout": 30, "pool_size": 10},
                },
                "cache": {"enabled": True},
            },
            level="common.yaml",
            facts={},
        )

        # Add overlapping nested data at environment level
        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={
                "database": {
                    "host": "prod-db.example.com",
                    "settings": {"timeout": 60, "ssl": True},
                },
                "logging": {"level": "INFO"},
            },
            level="environment/{environment}.yaml",
            facts={"environment": "prod"},
        )

        # Test merge
        result = self.pyhiera.key_data_get_merge(
            key="config",
            facts={"environment": "prod"},
            include_sources=False,
        )

        # Verify deep merge worked correctly
        self.assertEqual(result.data["database"]["host"], "prod-db.example.com")  # Overridden
        self.assertEqual(result.data["database"]["port"], 5432)  # From common
        self.assertEqual(result.data["database"]["settings"]["timeout"], 60)  # Overridden
        self.assertEqual(result.data["database"]["settings"]["pool_size"], 10)  # From common
        self.assertEqual(result.data["database"]["settings"]["ssl"], True)  # From prod
        self.assertEqual(result.data["cache"]["enabled"], True)  # From common
        self.assertEqual(result.data["logging"]["level"], "INFO")  # From prod

    def test_merge_empty_dict(self):
        """Test merge handles empty dicts correctly"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={},
            level="common.yaml",
            facts={},
        )

        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"a": 1},
            level="environment/{environment}.yaml",
            facts={"environment": "test"},
        )

        result = self.pyhiera.key_data_get_merge(
            key="config",
            facts={"environment": "test"},
            include_sources=False,
        )

        self.assertEqual(result.data["a"], 1)

    def test_merge_list_values(self):
        """Test merge with list values - lists should be extended"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"servers": ["server1", "server2"], "ports": [80, 443]},
            level="common.yaml",
            facts={},
        )

        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"servers": ["server3"], "timeout": 30},
            level="environment/{environment}.yaml",
            facts={"environment": "prod"},
        )

        result = self.pyhiera.key_data_get_merge(
            key="config",
            facts={"environment": "prod"},
            include_sources=False,
        )

        # Lists should be extended (merged)
        self.assertEqual(result.data["servers"], ["server1", "server2", "server3"])
        self.assertEqual(result.data["ports"], [80, 443])
        self.assertEqual(result.data["timeout"], 30)

    def test_merge_set_values(self):
        """Test merge with set values - sets should be updated"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"tags": {"production", "web"}, "features": {"feature1"}},
            level="common.yaml",
            facts={},
        )

        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"tags": {"critical"}, "enabled": True},
            level="environment/{environment}.yaml",
            facts={"environment": "prod"},
        )

        result = self.pyhiera.key_data_get_merge(
            key="config",
            facts={"environment": "prod"},
            include_sources=False,
        )

        # Sets should be merged (updated)
        self.assertEqual(result.data["tags"], {"production", "web", "critical"})
        self.assertEqual(result.data["features"], {"feature1"})
        self.assertEqual(result.data["enabled"], True)

    def test_merge_no_data_found_error(self):
        """Test merge raises error when no data is found"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        # Don't add any data, try to merge
        with self.assertRaises(PyHieraBackendError) as context:
            self.pyhiera.key_data_get_merge(
                key="config",
                facts={"environment": "nonexistent"},
                include_sources=False,
            )

        self.assertIn("No data found", str(context.exception))

    def test_merge_invalid_key_error(self):
        """Test merge raises error for non-existent key"""
        with self.assertRaises(PyHieraError) as context:
            self.pyhiera.key_data_get_merge(
                key="nonexistent_key",
                facts={},
                include_sources=False,
            )

        self.assertIn("Key nonexistent_key not found", str(context.exception))

    def test_merge_multiple_levels_deep_hierarchy(self):
        """Test merge across 3+ hierarchy levels"""
        # Add a more complex hierarchy
        self.test_dir_multi = tempfile.mkdtemp()
        hierarchy_multi = [
            "host/{hostname}.yaml",
            "environment/{environment}.yaml",
            "common.yaml",
        ]

        backend_multi = PyHieraBackendYamlSync(
            identifier="test_backend_multi",
            priority=1,
            config={"path": self.test_dir_multi},
            hierarchy=hierarchy_multi,
        )

        pyhiera_multi = PyHieraSync()
        pyhiera_multi.key_model_add(key="DynamicDict", model=PyHieraKeyDict)
        pyhiera_multi.backend_add(backend_multi)
        pyhiera_multi.key_add(key="config", hiera_key="DynamicDict")

        # Add data at all three levels
        pyhiera_multi.key_data_add(
            backend_identifier="test_backend_multi",
            key="config",
            data={"timeout": 10, "retries": 3, "log_level": "INFO"},
            level="common.yaml",
            facts={},
        )

        pyhiera_multi.key_data_add(
            backend_identifier="test_backend_multi",
            key="config",
            data={"timeout": 30, "ssl": True},
            level="environment/{environment}.yaml",
            facts={"environment": "prod"},
        )

        pyhiera_multi.key_data_add(
            backend_identifier="test_backend_multi",
            key="config",
            data={"timeout": 60, "priority": "high"},
            level="host/{hostname}.yaml",
            facts={"hostname": "web01"},
        )

        result = pyhiera_multi.key_data_get_merge(
            key="config",
            facts={"environment": "prod", "hostname": "web01"},
            include_sources=False,
        )

        # Host level should override environment, environment should override common
        self.assertEqual(result.data["timeout"], 60)  # From host
        self.assertEqual(result.data["retries"], 3)  # From common
        self.assertEqual(result.data["log_level"], "INFO")  # From common
        self.assertEqual(result.data["ssl"], True)  # From environment
        self.assertEqual(result.data["priority"], "high")  # From host

        # Cleanup
        if os.path.exists(self.test_dir_multi):
            shutil.rmtree(self.test_dir_multi)

    def test_merge_scalar_value_override(self):
        """Test that scalar values (str, int, bool, float) are overridden, not merged"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={
                "string_val": "common",
                "int_val": 100,
                "bool_val": False,
                "float_val": 1.5,
            },
            level="common.yaml",
            facts={},
        )

        self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={
                "string_val": "prod",
                "int_val": 200,
                "bool_val": True,
                "float_val": 2.7,
            },
            level="environment/{environment}.yaml",
            facts={"environment": "prod"},
        )

        result = self.pyhiera.key_data_get_merge(
            key="config",
            facts={"environment": "prod"},
            include_sources=False,
        )

        # All scalar values should be overridden by more specific level
        self.assertEqual(result.data["string_val"], "prod")
        self.assertEqual(result.data["int_val"], 200)
        self.assertEqual(result.data["bool_val"], True)
        self.assertEqual(result.data["float_val"], 2.7)


class TestPyHieraAsyncMerge(unittest.IsolatedAsyncioTestCase):
    """Test cases for PyHieraAsync merge functionality"""

    async def asyncSetUp(self):
        """Set up test fixtures"""
        self.test_dir = tempfile.mkdtemp()
        self.hierarchy = [
            "environment/{environment}.yaml",
            "common.yaml",
        ]

        self.backend = PyHieraBackendYamlAsync(
            identifier="test_backend",
            priority=1,
            config={"path": self.test_dir},
            hierarchy=self.hierarchy,
        )

        self.pyhiera = PyHieraAsync()
        self.pyhiera.key_model_add(key="DynamicDict", model=PyHieraKeyDict)
        self.pyhiera.key_model_add(key="Complex", model=PyHieraKeyComplex)
        self.pyhiera.backend_add(self.backend)

    async def asyncTearDown(self):
        """Clean up test fixtures"""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    async def test_key_data_get_non_merge(self):
        """Test key_data_get (non-merge) method (async)"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"timeout": 30},
            level="common.yaml",
            facts={},
        )

        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"timeout": 60},
            level="environment/{environment}.yaml",
            facts={"environment": "prod"},
        )

        # Get without merge - should return first match
        result = await self.pyhiera.key_data_get(
            key="config",
            facts={"environment": "prod"},
            include_sources=True,
        )

        self.assertEqual(result.data["timeout"], 60)  # First match (most specific)
        self.assertIsNotNone(result.sources)

    async def test_key_data_get_non_merge_without_sources(self):
        """Test key_data_get without sources (async)"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"value": 123},
            level="environment/{environment}.yaml",
            facts={"environment": "test"},
        )

        result = await self.pyhiera.key_data_get(
            key="config",
            facts={"environment": "test"},
            include_sources=False,
        )

        self.assertEqual(result.data["value"], 123)
        self.assertIsNone(result.sources)

    async def test_key_data_get_not_found_error(self):
        """Test key_data_get raises error when no data found (async)"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        with self.assertRaises(PyHieraBackendError) as context:
            await self.pyhiera.key_data_get(
                key="config",
                facts={"environment": "nonexistent"},
                include_sources=False,
            )

        self.assertIn("No data found", str(context.exception))

    async def test_key_data_get_invalid_key_error(self):
        """Test key_data_get with invalid key raises error (async)"""
        with self.assertRaises(PyHieraError) as context:
            await self.pyhiera.key_data_get(
                key="nonexistent_key",
                facts={},
                include_sources=False,
            )

        self.assertIn("Key nonexistent_key not found", str(context.exception))

    async def test_merge_dict_data_across_hierarchy(self):
        """Test Bug 1: Merge dict data across multiple hierarchy levels (async)"""
        # Add key
        self.pyhiera.key_add(key="config_data", hiera_key="DynamicDict")

        # Add data at common level
        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config_data",
            data={"timeout": 30, "retries": 3, "cache": True},
            level="common.yaml",
            facts={},
        )

        # Add data at environment level (prod)
        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config_data",
            data={"timeout": 60, "ssl": True},
            level="environment/{environment}.yaml",
            facts={"environment": "prod"},
        )

        # Test merge
        result = await self.pyhiera.key_data_get_merge(
            key="config_data",
            facts={"environment": "prod"},
            include_sources=False,
        )

        # Verify merged data
        self.assertIsInstance(result.data, dict)
        self.assertEqual(result.data["timeout"], 60)  # Overridden by prod
        self.assertEqual(result.data["retries"], 3)  # From common
        self.assertEqual(result.data["cache"], True)  # From common
        self.assertEqual(result.data["ssl"], True)  # From prod

    async def test_merge_with_include_sources_true(self):
        """Test merge with include_sources=True returns sources (async)"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        # Add data at both levels
        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"a": 1, "b": 2},
            level="common.yaml",
            facts={},
        )

        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"a": 10},
            level="environment/{environment}.yaml",
            facts={"environment": "dev"},
        )

        # Test with sources
        result = await self.pyhiera.key_data_get_merge(
            key="config",
            facts={"environment": "dev"},
            include_sources=True,
        )

        # Verify data
        self.assertEqual(result.data["a"], 10)
        self.assertEqual(result.data["b"], 2)

        # Verify sources are included
        self.assertIsNotNone(result.sources)
        self.assertEqual(len(result.sources), 2)

    async def test_merge_with_include_sources_false(self):
        """Test merge with include_sources=False does not return sources (async)"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"x": 100},
            level="common.yaml",
            facts={},
        )

        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"y": 200},
            level="environment/{environment}.yaml",
            facts={"environment": "test"},
        )

        result = await self.pyhiera.key_data_get_merge(
            key="config",
            facts={"environment": "test"},
            include_sources=False,
        )

        # Verify data
        self.assertEqual(result.data["x"], 100)
        self.assertEqual(result.data["y"], 200)

        # Verify sources are not included
        self.assertIsNone(result.sources)

    async def test_merge_non_dict_data_raises_error(self):
        """Test that merging non-dict data raises an error (async)"""
        self.pyhiera.key_add(key="simple_string", hiera_key="SimpleString")

        # Add string data (not dict)
        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="simple_string",
            data="test_value",
            level="common.yaml",
            facts={},
        )

        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="simple_string",
            data="test_value2",
            level="environment/{environment}.yaml",
            facts={"environment": "test"},
        )

        # Test that merge raises error for non-dict data
        with self.assertRaises(PyHieraBackendError) as context:
            await self.pyhiera.key_data_get_merge(
                key="simple_string",
                facts={"environment": "test"},
                include_sources=False,
            )

        self.assertIn("expected dict", str(context.exception))

    async def test_merge_complex_nested_dicts(self):
        """Test merge with complex nested dictionary structures (async)"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        # Add complex nested data at common level
        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={
                "database": {
                    "host": "localhost",
                    "port": 5432,
                    "settings": {"timeout": 30, "pool_size": 10},
                },
                "cache": {"enabled": True},
            },
            level="common.yaml",
            facts={},
        )

        # Add overlapping nested data at environment level
        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={
                "database": {
                    "host": "prod-db.example.com",
                    "settings": {"timeout": 60, "ssl": True},
                },
                "logging": {"level": "INFO"},
            },
            level="environment/{environment}.yaml",
            facts={"environment": "prod"},
        )

        # Test merge
        result = await self.pyhiera.key_data_get_merge(
            key="config",
            facts={"environment": "prod"},
            include_sources=False,
        )

        # Verify deep merge worked correctly
        self.assertEqual(result.data["database"]["host"], "prod-db.example.com")  # Overridden
        self.assertEqual(result.data["database"]["port"], 5432)  # From common
        self.assertEqual(result.data["database"]["settings"]["timeout"], 60)  # Overridden
        self.assertEqual(result.data["database"]["settings"]["pool_size"], 10)  # From common
        self.assertEqual(result.data["database"]["settings"]["ssl"], True)  # From prod
        self.assertEqual(result.data["cache"]["enabled"], True)  # From common
        self.assertEqual(result.data["logging"]["level"], "INFO")  # From prod

    async def test_merge_list_values(self):
        """Test merge with list values - lists should be extended (async)"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"servers": ["server1", "server2"], "ports": [80, 443]},
            level="common.yaml",
            facts={},
        )

        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"servers": ["server3"], "timeout": 30},
            level="environment/{environment}.yaml",
            facts={"environment": "prod"},
        )

        result = await self.pyhiera.key_data_get_merge(
            key="config",
            facts={"environment": "prod"},
            include_sources=False,
        )

        # Lists should be extended (merged)
        self.assertEqual(result.data["servers"], ["server1", "server2", "server3"])
        self.assertEqual(result.data["ports"], [80, 443])
        self.assertEqual(result.data["timeout"], 30)

    async def test_merge_set_values(self):
        """Test merge with set values - sets should be updated (async)"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"tags": {"production", "web"}, "features": {"feature1"}},
            level="common.yaml",
            facts={},
        )

        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={"tags": {"critical"}, "enabled": True},
            level="environment/{environment}.yaml",
            facts={"environment": "prod"},
        )

        result = await self.pyhiera.key_data_get_merge(
            key="config",
            facts={"environment": "prod"},
            include_sources=False,
        )

        # Sets should be merged (updated)
        self.assertEqual(result.data["tags"], {"production", "web", "critical"})
        self.assertEqual(result.data["features"], {"feature1"})
        self.assertEqual(result.data["enabled"], True)

    async def test_merge_no_data_found_error(self):
        """Test merge raises error when no data is found (async)"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        # Don't add any data, try to merge
        with self.assertRaises(PyHieraBackendError) as context:
            await self.pyhiera.key_data_get_merge(
                key="config",
                facts={"environment": "nonexistent"},
                include_sources=False,
            )

        self.assertIn("No data found", str(context.exception))

    async def test_merge_invalid_key_error(self):
        """Test merge raises error for non-existent key (async)"""
        with self.assertRaises(PyHieraError) as context:
            await self.pyhiera.key_data_get_merge(
                key="nonexistent_key",
                facts={},
                include_sources=False,
            )

        self.assertIn("Key nonexistent_key not found", str(context.exception))

    async def test_merge_multiple_levels_deep_hierarchy(self):
        """Test merge across 3+ hierarchy levels (async)"""
        # Add a more complex hierarchy
        test_dir_multi = tempfile.mkdtemp()
        hierarchy_multi = [
            "host/{hostname}.yaml",
            "environment/{environment}.yaml",
            "common.yaml",
        ]

        backend_multi = PyHieraBackendYamlAsync(
            identifier="test_backend_multi",
            priority=1,
            config={"path": test_dir_multi},
            hierarchy=hierarchy_multi,
        )

        pyhiera_multi = PyHieraAsync()
        pyhiera_multi.key_model_add(key="DynamicDict", model=PyHieraKeyDict)
        pyhiera_multi.backend_add(backend_multi)
        pyhiera_multi.key_add(key="config", hiera_key="DynamicDict")

        # Add data at all three levels
        await pyhiera_multi.key_data_add(
            backend_identifier="test_backend_multi",
            key="config",
            data={"timeout": 10, "retries": 3, "log_level": "INFO"},
            level="common.yaml",
            facts={},
        )

        await pyhiera_multi.key_data_add(
            backend_identifier="test_backend_multi",
            key="config",
            data={"timeout": 30, "ssl": True},
            level="environment/{environment}.yaml",
            facts={"environment": "prod"},
        )

        await pyhiera_multi.key_data_add(
            backend_identifier="test_backend_multi",
            key="config",
            data={"timeout": 60, "priority": "high"},
            level="host/{hostname}.yaml",
            facts={"hostname": "web01"},
        )

        result = await pyhiera_multi.key_data_get_merge(
            key="config",
            facts={"environment": "prod", "hostname": "web01"},
            include_sources=False,
        )

        # Host level should override environment, environment should override common
        self.assertEqual(result.data["timeout"], 60)  # From host
        self.assertEqual(result.data["retries"], 3)  # From common
        self.assertEqual(result.data["log_level"], "INFO")  # From common
        self.assertEqual(result.data["ssl"], True)  # From environment
        self.assertEqual(result.data["priority"], "high")  # From host

        # Cleanup
        if os.path.exists(test_dir_multi):
            shutil.rmtree(test_dir_multi)

    async def test_merge_scalar_value_override(self):
        """Test that scalar values (str, int, bool, float) are overridden, not merged (async)"""
        self.pyhiera.key_add(key="config", hiera_key="DynamicDict")

        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={
                "string_val": "common",
                "int_val": 100,
                "bool_val": False,
                "float_val": 1.5,
            },
            level="common.yaml",
            facts={},
        )

        await self.pyhiera.key_data_add(
            backend_identifier="test_backend",
            key="config",
            data={
                "string_val": "prod",
                "int_val": 200,
                "bool_val": True,
                "float_val": 2.7,
            },
            level="environment/{environment}.yaml",
            facts={"environment": "prod"},
        )

        result = await self.pyhiera.key_data_get_merge(
            key="config",
            facts={"environment": "prod"},
            include_sources=False,
        )

        # All scalar values should be overridden by more specific level
        self.assertEqual(result.data["string_val"], "prod")
        self.assertEqual(result.data["int_val"], 200)
        self.assertEqual(result.data["bool_val"], True)
        self.assertEqual(result.data["float_val"], 2.7)


if __name__ == "__main__":
    unittest.main()
