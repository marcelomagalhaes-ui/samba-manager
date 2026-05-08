from dataclasses import dataclass
from datetime import date
from typing import List


@dataclass
class ESALQIndicator:
    date: date
    commodity: str
    price: float
    unit: str
    variation_pct: float
    source: str


def get_mock_esalq() -> List[ESALQIndicator]:
    today = date.today()
    commodities = [
        ("Soja",            139.50, "R$/sc 60kg",  +0.45),
        ("Milho",            63.80, "R$/sc 60kg",  -0.31),
        ("Café Arábica",   1420.00, "R$/sc 60kg",  +1.20),
        ("Açúcar Cristal",  148.20, "R$/sc 50kg",  -0.18),
        ("Etanol Hidratado",  2.65, "R$/litro",    +0.76),
        ("Algodão",         112.70, "R$/@",        +0.55),
        ("Boi Gordo",       235.40, "R$/@",        -0.42),
        ("Trigo",          1580.00, "R$/t",        +0.33),
        ("Arroz",            86.50, "R$/sc 50kg",  -0.27),
    ]
    return [
        ESALQIndicator(
            date=today,
            commodity=name,
            price=price,
            unit=unit,
            variation_pct=var,
            source="ESALQ/CEPEA",
        )
        for name, price, unit, var in commodities
    ]
