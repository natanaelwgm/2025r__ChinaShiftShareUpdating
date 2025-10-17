from scripts.python.lib import registry


def test_build_registry_returns_dag():
    dag = registry.build_registry()
    assert isinstance(dag.tasks, dict)
