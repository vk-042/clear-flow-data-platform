from clearflow.anomaly import score
import pytest

@pytest.mark.parametrize("domain,normal", [("banking", 8000), ("healthcare", 120000)])
def test_extreme_amount_has_higher_score(domain, normal):
    typical, _, version = score(domain, normal)
    unusual, flag, same_version = score(domain, normal * 1000)
    assert unusual > typical
    assert flag == 1
    assert version == same_version
