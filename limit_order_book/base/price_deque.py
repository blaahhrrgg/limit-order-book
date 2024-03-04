from collections import deque

from typing import Dict, Iterable


class PriceDeque(deque):
    """Wrapper around deque to facilitate extracting limit order information."""

    def __init__(self, price: float, iterable: Iterable = ()):
        super().__init__(iterable)
        self.price = price

    def as_dict(self) -> Dict:
        return {
            "Price": self.price,
            "Quantity": sum([order.quantity for order in self]),
            "NumOrders": len(self),
        }
