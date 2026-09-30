from pathlib import Path
from streamlit.testing.v1 import AppTest
from clearflow.generator import generate
from clearflow.processor import ingest
from clearflow.storage import get_engine


def test_dashboard_renders_both_domains(tmp_path, monkeypatch):
    url = f"sqlite:///{tmp_path / 'dashboard.db'}"
    monkeypatch.setenv("DATABASE_URL", url)
    engine = get_engine(url)
    for row in generate(4)[0]:
        ingest(engine, row)
    path = Path(__file__).parents[1] / "src/clearflow/dashboard.py"
    app = AppTest.from_file(str(path), default_timeout=20).run()
    assert not app.exception
    assert app.metric[0].value == "2"
    app.sidebar.selectbox[0].select("healthcare").run()
    assert not app.exception
    assert app.metric[0].value == "2"
