"""The fund's holdings in lots, its cash, and the holding rule on sales."""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field

LOCK_DAYS = 30
RULES = ("last_buy", "per_lot")


@dataclass
class Lot:
    units: float
    bought: dt.date


@dataclass
class Book:
    """Units per fund in purchase lots, and cash.

    rule "last_buy": no unit of a fund may be sold within lock_days of any purchase of it, so a
    top-up locks the whole position. rule "per_lot": only units bought in the last lock_days are
    locked. Sales take the oldest lots first.
    """

    cash: float
    rule: str = "last_buy"
    lock_days: int = LOCK_DAYS
    lots: dict[str, list[Lot]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.rule not in RULES:
            raise ValueError(f"rule must be one of {RULES}, not {self.rule!r}.")

    def units(self, fund: str) -> float:
        return sum(lot.units for lot in self.lots.get(fund, []))

    def funds(self) -> list[str]:
        return [f for f, lots in self.lots.items() if lots]

    def sellable(self, fund: str, on: dt.date) -> float:
        """Return the units of fund that may be sold on `on`."""
        lots = self.lots.get(fund, [])
        if not lots:
            return 0.0
        free = [(on - lot.bought).days >= self.lock_days for lot in lots]
        if self.rule == "last_buy":
            return self.units(fund) if all(free) else 0.0
        return sum(lot.units for lot, ok in zip(lots, free, strict=True) if ok)

    def buy(self, fund: str, units: float, on: dt.date, price: float, cost: float) -> None:
        if units <= 0:
            raise ValueError(f"A buy needs positive units, not {units}.")
        self.lots.setdefault(fund, []).append(Lot(units, on))
        self.cash -= units * price + cost

    def sell(self, fund: str, units: float, on: dt.date, price: float, cost: float) -> None:
        """Sell units of fund from its oldest free lots. Raise when the rule forbids it."""
        if units <= 0:
            raise ValueError(f"A sale needs positive units, not {units}.")
        free = self.sellable(fund, on)
        if units > free * (1 + 1e-9):
            raise ValueError(f"{fund}: {units:.6g} units to sell on {on}, {free:.6g} are free.")
        left = units
        kept = []
        for lot in self.lots[fund]:
            if left > 0 and (on - lot.bought).days >= self.lock_days:
                take = min(lot.units, left)
                lot = Lot(lot.units - take, lot.bought)
                left -= take
            if lot.units > 1e-12:
                kept.append(lot)
        self.lots[fund] = kept
        self.cash += units * price - cost

    def value(self, prices: dict[str, float]) -> float:
        return self.cash + sum(self.units(f) * prices[f] for f in self.funds())
