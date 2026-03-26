from etf_factor.factors import factor_identity, momentum


def test_factor_identity_is_stable():
    key1, name1, params1 = factor_identity(momentum, {"window": 20})
    key2, name2, params2 = factor_identity(momentum, {"window": 20})
    assert key1 == key2
    assert name1 == name2 == "momentum"
    assert params1 == params2 == '{"window": 20}'
