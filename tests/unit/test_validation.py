# tests/test_validation.py
import pytest
from pydantic import ValidationError

from ingestion.validation import Trade


# --- Normal case: valid trade message ---
def test_trade_valid_message():
    data = {
        "e": "aggTrade",
        "E": 1789215829640,
        "s": "BNBUSDT",
        "a": 857488061,
        "p": "736.01000000",
        "q": "0.52400000",
        "f": 1584058807,
        "l": 1584058807,
        "T": 1789215829639,
        "m": False,
        "M": True,
    }
    trade = Trade.model_validate(data)

    assert trade.s == "BNBUSDT"
    assert trade.a == 857488061
    assert trade.p == "736.01000000"


# --- Edge case: missing required field ---
def test_trade_missing_field_raises():
    data = {
        "e": "aggTrade",
        "E": 1789215829640,
        "s": "BNBUSDT",
        # "a" is missing
        "p": "736.01000000",
        "q": "0.52400000",
        "f": 1584058807,
        "l": 1584058807,
        "T": 1789215829639,
        "m": False,
        "M": True,
    }
    with pytest.raises(ValidationError):
        Trade.model_validate(data)


# --- Edge case: wrong type for a field ---
def test_trade_wrong_type_raises():
    data = {
        "e": "aggTrade",
        "E": 1789215829640,
        "s": "BNBUSDT",
        "a": "not-an-int",  # should be int
        "p": "736.01000000",
        "q": "0.52400000",
        "f": 1584058807,
        "l": 1584058807,
        "T": 1789215829639,
        "m": False,
        "M": True,
    }
    with pytest.raises(ValidationError):
        Trade.model_validate(data)


# --- Edge case: extra unexpected field ---
def test_trade_extra_field_ignored_or_rejected():
    data = {
        "e": "aggTrade",
        "E": 1789215829640,
        "s": "BNBUSDT",
        "a": 857488061,
        "p": "736.01000000",
        "q": "0.52400000",
        "f": 1584058807,
        "l": 1584058807,
        "T": 1789215829639,
        "m": False,
        "M": True,
        "unexpected_field": "surprise",
    }
    # This test documents current model behavior — adjust the assertion
    # once you decide whether extra fields should be allowed or rejected.
    trade = Trade.model_validate(data)
    assert trade.s == "BNBUSDT"


# --- Edge case: boolean fields as strings (common WebSocket quirk) ---
def test_trade_boolean_as_string_raises():
    data = {
        "e": "aggTrade",
        "E": 1789215829640,
        "s": "BNBUSDT",
        "a": 857488061,
        "p": "736.01000000",
        "q": "0.52400000",
        "f": 1584058807,
        "l": 1584058807,
        "T": 1789215829639,
        "m": "false",  # string instead of bool
        "M": True,
    }
    # Pydantic v2 is strict by default for bool with str input in some configs —
    # this test documents actual behavior; adjust if Trade allows coercion.
    with pytest.raises(ValidationError):
        Trade.model_validate(data)