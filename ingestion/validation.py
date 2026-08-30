# ruff: noqa: E741

from pydantic import BaseModel


class Trade(BaseModel):
    e: str
    E: int
    s: str
    a: int
    p: str
    q: str
    f: int
    l: int
    T: int
    m: bool
    M: bool


"""  
    link to binance official docs: https://developers.binance.com/en/docs/catalog/core-trading-spot-trading/api/ws-streams/~
    example message from binance socket:
    {
        "e": "aggTrade",
        "E": 1672515782136,
        "s": "BNBBTC",
        "a": 12345,
        "p": 0.001,
        "q": 100,
        "f": 100,
        "l": 105,
        "T": 1672515782136,
        "m": true,
        "M": true
    }
"""
