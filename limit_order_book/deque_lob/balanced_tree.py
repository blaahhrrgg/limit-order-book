from bintrees import FastAVLTree
from typing import AnyStr

from limit_order_book.base import (
    PriceDeque,
    LimitOrder,
    Direction
)

from .base_deque_limit_order_book import BaseDequeLimitOrderBook


class BalancedTreeDequeLimitOrderBook(BaseDequeLimitOrderBook):
    """A balanced tree implementation of a limit order book.

    This implementation utilises a balanced tree to look up the price deque
    for a given price level. Note that empty price levels are deleted from
    the tree when the best bid/ask price is updated.

    References
    ----------
    . https://en.wikipedia.org/wiki/AVL_tree
    . https://web.archive.org/web/20110219163448/http://howtohft.wordpress.com/2011/02/15/how-to-build-a-fast-limit-order-book/
    """

    def __init__(self, name: AnyStr, max_price: int) -> None:
        super().__init__(name, max_price)
        self._bids = FastAVLTree()
        self._asks = FastAVLTree()

    def _add_order_to_queue(self, limit_order: LimitOrder) -> None:
        """Add order to price queue

        Parameters
        ----------
        limit_order: LimitOrder
            The limit order to add
        """
        self.orders[limit_order.id] = limit_order

        book = (
            self._bids if limit_order.direction == Direction.Buy else self._asks
        )

        if limit_order.price not in book:
            # Create deque with limit order
            book.insert(
                limit_order.price,
                PriceDeque(price=limit_order.price, iterable=[limit_order])
            )

        else:
            # Append limit order to existing deque
            book.get_value(limit_order.price).append(limit_order)

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
        book = self._bids if price <= self.best_bid else self._asks
        return book.get_value(price)

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
        if price == self.best_bid:
            return self.best_ask
        elif price == self._asks.max_item()[0]:
            return self.max_price
        else:
            book = self._bids if price <= self.best_bid else self._asks
            return book.succ_item(price)[0]

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
        if price == self.best_ask:
            return self.best_bid
        elif price == self._bids.min_item()[0]:
            return 0
        else:
            book = self._bids if price <= self.best_bid else self._asks
            return book.prev_item(price)[0]

    def _update_best_ask_price(self, price=None):
        """Update the best ask price

        For the given reference ask price, check if the price deque is empty.
        If so, delete the node from the balanced tree.

        Parameters
        ----------
        price: Optional[int]
            If provided, this value will be used as the reference point for
            updating the best ask price, otherwise the current best ask price
            will be used.
        """

        """Update the best ask price."""
        level = self.best_ask if price is None else price
        if len(self._get_price_level(level)) == 0:
            self._asks.remove(level)

    def _update_best_bid_price(self, price=None):
        """Update the best bid price

        For the given reference bid price, check if the price deque is empty.
        If so, delete the node from the balanced tree.

        Parameters
        ----------
        price: Optional[int]
            If provided, this value will be used as the reference point for
            updating the best bid price, otherwise the current best bid price
            will be used.
        """

        """Update the best bid price."""
        level = self.best_bid if price is None else price
        if len(self._get_price_level(level)) == 0:
            self._bids.remove(level)

    @property
    def best_bid(self) -> int:
        """The current best bid price

        Returns
        -------
        int
            The current best bid price
        """
        if self._bids.is_empty():
            return 0
        else:
            return self._bids.max_item()[0]

    @property
    def best_ask(self) -> int:
        """The current best ask price

        Returns
        -------
        int
            The current best ask price
        """
        if self._asks.is_empty():
            return self.max_price
        else:
            return self._asks.min_item()[0]
