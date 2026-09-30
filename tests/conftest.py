import pytest
from clearflow.storage import get_engine
from clearflow.generator import generate

@pytest.fixture
def engine(tmp_path):
    result = get_engine(f"sqlite:///{tmp_path / 'test.db'}")
    yield result
    result.dispose()

@pytest.fixture
def journey():
    records, _ = generate(1, seed=1, faults=False, run_id="test")
    return records
