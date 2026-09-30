import json
import os
import subprocess
import sys


def test_replay_survives_process_restart(tmp_path):
    env = dict(os.environ, DATABASE_URL=f"sqlite:///{tmp_path / 'durable.db'}")
    def run(*args):
        result = subprocess.run([sys.executable, "-m", "clearflow.cli", *args],
                                env=env, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    demo = run("demo", "--count", "20", "--output", str(tmp_path))
    before = run("stats")
    replay = run("replay", str(tmp_path / "events.jsonl"))
    after = run("stats")
    assert demo["reconciliation"]["matched"] == 20
    assert replay.get("accepted", 0) == 0
    assert before["events"] == after["events"]
    assert before["entities"] == after["entities"] == 20
    assert run("reconcile", str(tmp_path / "reference.csv"))["issues"] == []
