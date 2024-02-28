from bintrees import FastAVLTree
import pandas
from typing import AnyStr

from .base import (
    BaseLimitOrderBook,
    PriceDeque,
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
            book.insert(limit_order.price, PriceDeque(
                [limit_order], price=limit_order.price))
        else:
            # Append limit order to existing deque
            book.get_value(limit_order.price).append(limit_order)

    def add(self, limit_order: LimitOrder) -> None:
        """Add an order to the limit order book

        Parameters
        ----------
        limit_order: LimitOrder
            The limit order to add to the book
        """

        if limit_order.direction == Direction.Buy:
            # Look for outstanding sell orders that cross with the buy order
            while (
                (limit_order.price >= self.best_ask) and
                (not self._asks.is_empty())
            ):

                # Iterate through limit orders at current ask min
                entries = self._asks.get_value(self.best_ask)

                while entries:
                    entry = entries[0]

                    if entry.quantity < limit_order.quantity:
                        # Current limit order is larger than best ask order
                        limit_order.quantity -= entry.quantity

                        # Remove existing order from book
                        entries.popleft()

                        self.execute(
                            MatchedOrder(
                                buy_trader_id=limit_order.trader_id,
                                sell_trader_id=entry.trader_id,
                                quantity=entry.quantity,
                                price=entry.price,
                            )
                        )

                    else:
                        # Existing limit order is larger than current order
                        if entry.quantity > limit_order.quantity:
                            # Amend existing order in order book
                            entry.quantity -= limit_order.quantity
                        else:
                            # Remove existing order from order book
                            entries.popleft()

                        self.execute(
                            MatchedOrder(
                                buy_trader_id=limit_order.trader_id,
                                sell_trader_id=entry.trader_id,
                                quantity=limit_order.quantity,
                                price=limit_order.price,
                            )
                        )

                        # Order completely matched, done
                        return None

                # Exhausted all orders at the current price level, delete empty
                # deque to update best ask
                self._asks.remove(self.best_ask)

            # If we get here, then there is some quantity we cannot fill,
            # so we enqueue the order in the limit order book
            self._add_order_to_queue(limit_order)

            # Done
            return None

        else:
            # Sell order
            while (
                (limit_order.price <= self.best_bid) and
                (not self._bids.is_empty())
            ):

                # Look for existing sell orders to cross with the buy order
                entries = self._bids.get_value(self.best_bid)

                while entries:
                    entry = entries[0]

                    if entry.quantity < limit_order.quantity:
                        # Current limit order is larger than best bid order
                        limit_order.quantity -= entry.quantity

                        # Remove existing order from book
                        entries.popleft()

                        self.execute(
                            MatchedOrder(
                                buy_trader_id=entry.trader_id,
                                sell_trader_id=limit_order.trader_id,
                                quantity=entry.quantity,
                                price=entry.price,
                            )
                        )

                    else:
                        # Existing limit order is larger than current order
                        if entry.quantity > limit_order.quantity:
                            # Amend existing order in order book
                            entry.quantity -= limit_order.quantity
                        else:
                            # Remove existing order from order book
                            entries.popleft()

                        self.execute(
                            MatchedOrder(
                                buy_trader_id=entry.trader_id,
                                sell_trader_id=limit_order.trader_id,
                                quantity=limit_order.quantity,
                                price=limit_order.price,
                            )
                        )

                        # Order completely matched, done
                        return None

                # Exhausted all orders at the current price level, delete empty
                # deque to update best bid
                self._bids.remove(self.best_bid)

            # If we get here, then there is some quantity we cannot fill,
            # so we enqueue the order in the limit order book
            self._add_order_to_queue(limit_order)

            # Done
            return None

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

        while len(data) < levels:
            data.append(self._bids.get_value(current_level).as_dict())

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

        while len(data) < levels:

            data.append(self._asks.get_value(current_level).as_dict())

            if current_level == self._asks.max_item()[0]:
                # If reached the highest level, break
                break
            else:
                # Move to next level
                current_level = self._asks.succ_item(current_level)[0]

        return pandas.DataFrame(data)
