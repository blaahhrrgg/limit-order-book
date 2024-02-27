import dataclasses
import enum
import uuid

from typing import AnyStr, Dict


class Direction(enum.Enum):
    Buy = enum.auto()
    Sell = enum.auto()


@dataclasses.dataclass
class LimitOrder:
    trader_id: int
    price: int
    quantity: int
    direction: Direction
    _id: uuid.uuid4 = dataclasses.field(default_factory=uuid.uuid4)

    @property
    def id(self) -> AnyStr:
        return self._id.hex

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
    _id: uuid.uuid4 = dataclasses.field(default_factory=uuid.uuid4)

    @property
    def id(self) -> AnyStr:
        return self._id.hex

    @property
    def as_dict(self) -> Dict:
        return {
            "id": self.id,
            "buy_trader_id": self.buy_trader_id,
            "sell_trader_id": self.sell_trader_id,
            "quantity": self.quantity,
            "price": self.price,
        }

    def __eq__(self, other):
        return True if self.id == other.id else False
