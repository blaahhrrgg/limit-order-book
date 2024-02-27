from collections import deque
from bintrees import FastAVLTree
import pandas
from typing import AnyStr, Optional

from .limit_order_book import BaseLimitOrderBook
from .order import (
    LimitOrder,
    MatchedOrder,
    Direction
)


class BalancedTreeDequeLimitOrderBook(BaseLimitOrderBook):
    """A balanced tree implementation of a limit order book.

    This implementation of a limit order book relies on i) a balanced binary
    tree to lookup a double ended queue for each bid and ask price level and
    ii) a dictionary to lookup limit orders from a given order identifier.
    """

    def __init__(self, name: AnyStr, max_price: int) -> None:
        super().__init__(name, max_price)
        self.orders = dict()
        self._bids = FastAVLTree()
        self._asks = FastAVLTree()

    def _add_order_to_queue(self, limit_order: LimitOrder) -> None:
        self.orders[limit_order.id] = limit_order

        book = (
            self._bids if limit_order.direction == Direction.Buy else self._asks
        )

        if limit_order.price not in book:
            # Create deque with limit order
            book.insert(limit_order.price, deque([limit_order]))
        else:
            # Append limit order to existing deque
            book.get_value(limit_order.price).append(limit_order)

    def add(
            self, trader_id: int, direction: Direction, quantity: int,
            price: int
    ) -> Optional[LimitOrder]:
        """Add an order to the limit order book

        Parameters
        ----------
        trader_id: int
            The identifier of the trader who sent the order
        direction: Direction
            The direction of the order
        quantity: int
            The quantity to buy or sell in the order
        price: int
            The price of the limit order

        Returns
        -------
        LimitOrder
            A limit order object with an identifier of the limit order.
        """

        if direction == Direction.Buy:
            # Look for outstanding sell orders that cross with the buy order
            while (price >= self.best_ask) and (not self._asks.is_empty()):

                # Iterate through limit orders at current ask min
                entries = self._asks.get_value(self.best_ask)

                while entries:
                    entry = entries[0]

                    if entry.quantity < quantity:
                        # Current limit order is larger than best ask order
                        quantity -= entry.quantity

                        # Remove existing order from book
                        entries.popleft()

                        self.execute(
                            MatchedOrder(
                                buy_trader_id=trader_id,
                                sell_trader_id=entry.trader_id,
                                quantity=entry.quantity,
                                price=price,
                            )
                        )

                    else:
                        self.execute(
                            MatchedOrder(
                                buy_trader_id=trader_id,
                                sell_trader_id=entry.trader_id,
                                quantity=quantity,
                                price=price,
                            )
                        )

                        # Existing limit order is larger than current order
                        if entry.quantity > quantity:
                            # Amend existing order in order book
                            entry.quantity -= quantity
                        else:
                            # Remove existing order from order book
                            entries.popleft()

                        # Order completely matched, no limit order to return
                        return None

                # Exhausted all orders at the current price level, delete empty
                # deque to update best ask
                self._asks.remove(self.best_ask)

            # If we get here, then there is some quantity we cannot fill,
            # so we enqueue the order in the limit order book
            limit_order = LimitOrder(
                trader_id=trader_id,
                direction=direction,
                quantity=quantity,
                price=price,
            )

            self._add_order_to_queue(limit_order)

            return limit_order

        else:
            # Sell order
            while (price <= self.best_bid) and (not self._bids.is_empty()):

                # Look for existing sell orders to cross with the buy order
                entries = self._bids.get_value(self.best_bid)

                while entries:
                    entry = entries[0]

                    if entry.quantity < quantity:
                        # Current limit order is larger than best bid order
                        quantity -= entry.quantity

                        # Remove existing order from book
                        entries.popleft()

                        self.execute(
                            MatchedOrder(
                                buy_trader_id=entry.trader_id,
                                sell_trader_id=trader_id,
                                quantity=entry.quantity,
                                price=entry.price,
                            )
                        )

                    else:
                        # Existing limit order is larger than current order

                        if entry.quantity > quantity:
                            # Amend existing order in order book
                            entry.quantity -= quantity
                        else:
                            # Remove existing order from order book
                            entries.popleft()

                        self.execute(
                            MatchedOrder(
                                buy_trader_id=entry.trader_id,
                                sell_trader_id=trader_id,
                                quantity=entry.quantity,
                                price=entry.price,
                            )
                        )

                        # Order completely matched, no limit order to return
                        return None

                # Exhausted all orders at the current price level, delete empty
                # deque to update best bid
                self._bids.remove(self.best_bid)

            # If we get here, then there is some quantity we cannot fill,
            # so we enqueue the order in the limit order book
            limit_order = LimitOrder(
                trader_id=trader_id,
                direction=direction,
                quantity=quantity,
                price=price,
            )

            self._add_order_to_queue(limit_order)

            return limit_order

    def cancel(self, order_id: AnyStr) -> None:
        """Cancel limit order with the given order identifier

        Parameters
        ----------
        order_id: AnyStr
            Deletes the limit order with the order identifier from the book
        """
        # Delete order
        limit_order = self.orders[order_id]

        # Remove limit order from book
        book = (
            self._bids if limit_order.direction == Direction.Buy else self._asks
        )

        book.get_value(limit_order.price).remove(limit_order)

        # Remove order from order cache
        del self.orders[order_id]

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
            return self.max_price + 1
        else:
            return self._asks.min_item()[0]

    def get_top_bids_as_dataframe(self, levels=10) -> pandas.DataFrame:
        """Get a DataFrame summary of top bids in the order book.

        Parameters
        ----------
        levels: int
            The number of price levels to include in the table.

        Returns
        -------
        pandas.DataFrame
            A pandas DataFrame summary of bids at the top of the book.
        """
        current_level = self.best_bid

        data = []

        while current_level > self.best_bid - levels:

            data.append(
                {
                    "Price": current_level,
                    "Quantity": sum([
                        order.quantity
                        for order in self._bids.get_value(current_level)
                    ]),
                    "NumOrders": len(self._bids.get_value(current_level)),
                }
            )

            if current_level == self._bids.min_item()[0]:
                # If reached the lowest level, break
                break
            else:
                # Move to next level
                current_level = self._bids.prev_item(current_level)[0]

        return pandas.DataFrame(data)

    def get_top_asks_as_dataframe(self, levels=10) -> pandas.DataFrame:
        """Get a DataFrame summary of top asks in the order book.

        Parameters
        ----------
        levels: int
            The number of price levels to include in the table.

        Returns
        -------
        pandas.DataFrame
            A pandas.DataFrame summary of asks at the top of the book.
        """

        current_level = self.best_ask

        data = []

        while current_level < self.best_ask + levels:

            data.append(
                {
                    "Price": current_level,
                    "Quantity": sum([
                        order.quantity
                        for order in self._asks.get_value(current_level)
                    ]),
                    "NumOrders": len(self._asks.get_value(current_level)),
                }
            )

            if current_level == self._asks.max_item()[0]:
                # If reached the highest level, break
                break
            else:
                # Move to next level
                current_level = self._asks.succ_item(current_level)[0]

        return pandas.DataFrame(data)
