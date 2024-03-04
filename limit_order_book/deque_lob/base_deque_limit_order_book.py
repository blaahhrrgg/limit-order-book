import pandas
from typing import AnyStr, Optional

from limit_order_book.base import (
    BaseLimitOrderBook,
    PriceDeque,
    LimitOrder,
    MatchedOrder,
    Direction
)


class BaseDequeLimitOrderBook(BaseLimitOrderBook):
    """Base class for deque limit order book implementations.

    This flavour of implementation utilises a double-ended queue at each price
    level to keep track of limit orders in chronological order. Another data
    structure is used to enable efficient lookup of the double-ended queues.

    See Also
    --------
    .. ArrayDequeLimitOrderBook
    .. HashDequeLimitOrderBook
    .. BalancedTreeDequeLimitOrderBook
    """

    def __init__(self, name: AnyStr, max_price: int) -> None:
        super().__init__(name, max_price)
        self.orders = dict()

    def _add_order_to_queue(self, limit_order: LimitOrder) -> None:
        """Add order to queue"""
        raise NotImplementedError

    def _get_price_level(self, price: int) -> PriceDeque:
        """Returns PriceDeque for the given price."""
        raise NotImplementedError

    def _get_next_level(self, price: int) -> int:
        """Returns the next price level."""
        raise NotImplementedError

    def _get_prev_level(self, price: int) -> int:
        """Returns the previous price level."""
        raise NotImplementedError

    def _update_best_ask_price(self, price: Optional[int] = None) -> None:
        """Update the best ask price."""
        raise NotImplementedError

    def _update_best_bid_price(self, price: Optional[int] = None) -> None:
        """Update the best bid price."""
        raise NotImplementedError

    @property
    def best_bid(self) -> int:
        """Return the current best bid price."""
        raise NotImplementedError

    @property
    def best_ask(self) -> int:
        """Return the current best ask price."""
        raise NotImplementedError

    def add(self, limit_order: LimitOrder) -> None:
        """Add an order to the limit order book

        Parameters
        ----------
        limit_order: LimitOrder
            The limit order to add to the book
        """

        if limit_order.direction == Direction.Buy:
            # Look for outstanding sell orders that cross with the buy order

            while limit_order.price >= self.best_ask:

                # Iterate through limit orders at current level
                entries = self._get_price_level(self.best_ask)

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
                                price=entry.price,
                            )
                        )

                        # Order completely matched, done
                        return None

                # Exhausted all orders at the current price level, move to
                # the next price level
                self._update_best_ask_price()

            # If we get here, then there is some quantity we cannot fill,
            # so we enqueue the order in the limit order book
            self._add_order_to_queue(limit_order)

            # Update bid max
            self._update_best_bid_price(limit_order.price)

            # Done
            return None

        else:
            # Sell order
            while limit_order.price <= self.best_bid:

                # Look for existing sell orders to cross with the buy order
                entries = self._get_price_level(self.best_bid)

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
                                price=entry.price,
                            )
                        )

                        # Order completely matched, done
                        return None

                # Exhausted all orders at the current price level, move
                # to the next price level
                self._update_best_bid_price()

            # If we get here, then there is some quantity we cannot fill,
            # so we enqueue the order in the limit order book
            self._add_order_to_queue(limit_order)

            # Update ask min
            self._update_best_ask_price(limit_order.price)

            # Done
            return None

    def cancel(self, order_id: AnyStr) -> None:
        """Cancel limit order with the given order identifier

        Parameters
        ----------
        order_id: AnyStr
            Deletes the limit order with the order identifier from the book
        """

        # Fetch order
        limit_order = self.orders[order_id]

        # Remove limit order from book
        self._get_price_level(limit_order.price).remove(limit_order)

        # Remove order from order cache
        del self.orders[order_id]

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

        while len(data) < levels and current_level > 0:

            level = self._get_price_level(current_level)

            # If non-empty, append
            if len(level) != 0:
                data.append(level.as_dict())

            # Go to previous level
            current_level = self._get_prev_level(current_level)

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

        while len(data) < levels and current_level < self.max_price:

            level = self._get_price_level(current_level)

            if len(level) != 0:
                data.append(level.as_dict())

            # Go to next level
            current_level = self._get_next_level(current_level)

        return pandas.DataFrame(data)
