"""Deterministic per-domain synthetic baselines; never load untrusted pickle files."""
from functools import lru_cache
import numpy as np
from sklearn.ensemble import IsolationForest

MODEL_VERSION = "synthetic-iforest-v1"

@lru_cache(maxsize=2)
def model(domain):
    rng = np.random.default_rng(42 if domain == "banking" else 43)
    median = 80 if domain == "banking" else 1200
    amounts = rng.lognormal(np.log(median), 0.65, 1500)
    features = np.log1p(amounts).reshape(-1, 1)
    estimator = IsolationForest(n_estimators=80, contamination=0.02, random_state=42, n_jobs=1)
    estimator.fit(features)
    return estimator

def score(domain, amount_cents):
    value = [[np.log1p(amount_cents / 100)]]
    decision = float(model(domain).decision_function(value)[0])
    return -decision, int(decision < 0), MODEL_VERSION
