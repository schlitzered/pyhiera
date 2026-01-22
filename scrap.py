import os
from pyhiera import PyHiera
from pyhiera import PyHieraBackendYaml

# Hierarchy levels
# 1. stage/{stage}/.yaml
# 2. common.yaml
# PyHieraBackendYaml appends .yaml to the level string.
hierarchy = [
    "stage/{stage}.yaml",
    "common.yaml",
]

# Base path for data
base_path = os.path.join(os.getcwd(), "test_data")
print(f"Base path: {base_path}")

if not os.path.exists(base_path):
    os.makedirs(base_path)

# Initialize backend
backend = PyHieraBackendYaml(
    identifier="test_yaml",
    priority=1,
    config={"path": base_path},
    hierarchy=hierarchy,
)

# Insert test data
print("Inserting test data...")

backend.key_data_add(
    key="db_host",
    data="127.0.0.1",
    level="common.yaml",
    facts={},
)
backend.key_data_add(
    key="db_host",
    data="127.0.0.2",
    level="stage/{stage}.yaml",
    facts={"stage": "prod"},
)
backend.key_data_add(
    key="db_host",
    data="127.0.0.3",
    level="stage/{stage}.yaml",
    facts={"stage": "dev"},
)

backend.key_data_add(
    key="complex",
    data={"a": "common", "b": {"blarg": "1", "other": "val", "blub": ["a", "b", "c"]}},
    level="common.yaml",
    facts={},
)
backend.key_data_add(
    key="complex",
    data={"b": {"blarg": "2", "blub": ["c", "d"]}},
    level="stage/{stage}.yaml",
    facts={"stage": "dev"},
)
backend.key_data_add(
    key="complex",
    data={"a": "prod", "b": {"blarg": "3", "blub": ["c", "d", "e"]}},
    level="stage/{stage}.yaml",
    facts={"stage": "prod"},
)

# Retrieve data
print("\nRetrieving data...")


def get_and_print(key, facts):
    results = backend.key_data_get(key, facts)
    print(f"Key: {key}, Facts: {facts}")
    if results:
        for r in results:
            print(f"  Found in level '{r.level}': {r.data}")
    else:
        print("  Not found")


# Test Prod
# get_and_print("db_host", {"stage": "prod"})

# Test Dev
# get_and_print("db_host", {"stage": "dev"})


# test backend via PyHiera

pyhiera = PyHiera()
pyhiera.backend_add(backend)
pyhiera.key_add(key="db_host", hiera_key="SimpleString")
pyhiera.key_add(key="complex", hiera_key="Complex")

print("\nPyHiera key_data_get:")
print(f"db_host (stage: blarg): {pyhiera.key_data_get('db_host', {'stage': 'blarg'})}")
print(f"complex (stage: blarg): {pyhiera.key_data_get('complex', {'stage': 'blarg'})}")
print(f"db_host (stage: dev): {pyhiera.key_data_get('db_host', {'stage': 'dev'})}")
print(f"complex (stage: dev): {pyhiera.key_data_get('complex', {'stage': 'dev'})}")

print("\nPyHiera key_data_get_merge:")
print(
    f"complex (stage: dev): {pyhiera.key_data_get_merge('complex', {'stage': 'dev'})}"
)
print(
    f"complex (stage: prod): {pyhiera.key_data_get_merge('complex', {'stage': 'prod'})}"
)
print(
    f"complex (stage: assdasd): {pyhiera.key_data_get_merge('complex', {'stage': 'assdasd'})}"
)
