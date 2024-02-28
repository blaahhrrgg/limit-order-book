import dataclasses
import enum
import uuid

from typing import AnyStr, Dict, Optional, Union


class Direction(enum.Enum):
    Buy = enum.auto()
    Sell = enum.auto()


@dataclasses.dataclass
class LimitOrder:
    trader_id: int
    price: int
    quantity: int
    direction: Direction
    id: Optional[Union[int, AnyStr]] = None

    def __post_init__(self):
        self.id = uuid.uuid4().hex if self.id is None else self.id

    @property
    def as_dict(self) -> Dict:
        return {
            "id": self.id,
            "trader_id": self.trader_id,
            "quantity": self.quantity,
            "price": self.price,
        }

    def __eq__(self, other):
        return True if self.id == other.id else False


@dataclasses.dataclass
class MatchedOrder:
    buy_trader_id: int
    sell_trader_id: int
    price: int
    quantity: int
    id: Optional[Union[int, AnyStr]] = None

    def __post_init__(self):
        self.id = uuid.uuid4().hex if self.id is None else self.id

    @property
    def as_dict(self) -> Dict:
        return {
            "Id": self.id,
            "BuyTraderId": self.buy_trader_id,
            "SellTraderId": self.sell_trader_id,
            "Quantity": self.quantity,
            "Price": self.price,
        }

    def __eq__(self, other):
        return True if self.id == other.id else False
