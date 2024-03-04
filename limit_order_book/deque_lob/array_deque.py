from typing import AnyStr

from limit_order_book.base import PriceDeque, LimitOrder
from .base_deque_limit_order_book import BaseDequeLimitOrderBook


class ArrayDequeLimitOrderBook(BaseDequeLimitOrderBook):
    """A flat linear array implementation of a limit order book.

    This implementation utilises a flat vector to look up the price deque for a
    given price level. Note that a price deque is instantiated at each price
    level and the best bid/ask prices are updated via the private attributes
    `bid_max` and `ask_min`.

    References
    ----------
    . https://web.archive.org/web/20141222151051/https://dl.dropboxusercontent.com/u/3001534/engine.c
    . https://github.com/kmanley/orderbook
    """

    def __init__(self, name: AnyStr, max_price: int) -> None:
        super().__init__(name, max_price)
        self.bid_max = 0
        self.ask_min = max_price
        self.price_queues = [
            PriceDeque(iterable=[], price=price)
            for price in range(self.max_price + 1)
        ]

    def _add_order_to_queue(self, limit_order: LimitOrder) -> None:
        """Add order to price queue

        Parameters
        ----------
        limit_order: LimitOrder
            The limit order to add
        """
        self.orders[limit_order.id] = limit_order
        self._get_price_level(limit_order.price).append(limit_order)

    def _get_price_level(self, price: int) -> PriceDeque:
        """Returns the price queue for the given price

        Parameters
        ----------
        price: int
            The price

        Returns
        -------
        PriceDeque
            The PriceQueue for the given price level
        """
        return self.price_queues[price]

    def _get_next_level(self, price: int) -> int:
        """Returns the next highest price level

        Parameters
        ----------
        price: int
            The price

        Returns
        -------
        int
            The next highest price level
        """

        price += 1

        while (
                (len(self._get_price_level(price)) == 0) and
                (price < self.max_price)
        ):
            price += 1

        return price

    def _get_prev_level(self, price: int) -> int:
        """Returns the previous highest price level

        Parameters
        ----------
        price: int
            The price

        Returns
        -------
        int
            The previous highest price level
        """
        price -= 1

        while (len(self._get_price_level(price)) == 0) and (price > 0):
            price -= 1

        return price

    def _update_best_ask_price(self, price=None):
        """Update the best ask price

        From a given reference ask price, the best ask price will be
        updated to either the given reference price if the associated price
        deque is non-empty or the previous highest value from the reference
        price.

        Parameters
        ----------
        price: Optional[int]
            If provided, this value will be used as the reference point for
            updating the best ask price otherwise the current best ask price
            will be used.
        """
        starting_price = min([price, self.best_ask]) if price else self.best_ask

        if len(self._get_price_level(starting_price)) != 0:
            self.ask_min = starting_price
        else:
            self.ask_min = self._get_prev_level(starting_price)

    def _update_best_bid_price(self, price=None):
        """Update the best bid price

        From a given reference bid price, the best bid price will be
        updated to either the given reference price if the associated price
        deque is non-empty or the next highest value from the reference price.

        Parameters
        ----------
        price: Optional[int]
            If provided, this value will be used as the reference point for
            updating the best bid price, otherwise the current best bid price
            will be used.
        """

        starting_price = max([price, self.best_bid]) if price else self.best_bid

        if len(self._get_price_level(starting_price)) != 0:
            self.bid_max = starting_price
        else:
            self.bid_max = self._get_prev_level(starting_price)

    @property
    def best_bid(self) -> int:
        """The current best bid price

        Returns
        -------
        int
            The current best bid price
        """
        return self.bid_max

    @property
    def best_ask(self) -> int:
        """The current best ask price

        Returns
        -------
        int
            The current best ask price
        """
        return self.ask_min
