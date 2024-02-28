import pandas
from typing import AnyStr

from .base import (
    BaseLimitOrderBook,
    PriceDeque,
    LimitOrder,
    MatchedOrder,
    Direction
)


class HashDequeLimitOrderBook(BaseLimitOrderBook):
    """A hash-map implementation of a limit order book.

    This implementation of a limit order book relies on i) a hash map (
    dictionary) of double-ended queue at each price and ii) a dictionary to
    lookup limit orders from a given order identifier.
    """

    def __init__(self, name: AnyStr, max_price: int) -> None:
        super().__init__(name, max_price)
        self.bid_max = 0
        self.ask_min = max_price + 1
        self.orders = dict()
        self.price_queues = dict()

    def _add_order_to_queue(self, limit_order: LimitOrder) -> None:
        self.orders[limit_order.id] = limit_order
        # self.price_queues[limit_order.price].append(limit_order)

        self.price_queues.setdefault(limit_order.price, PriceDeque(
            price=limit_order.price)).append(limit_order)

    def add(self, limit_order: LimitOrder) -> None:
        """Add an order to the limit order book

        Parameters
        ----------
        limit_order: LimitOrder
            The limit order to add to the book
        """

        if limit_order.direction == Direction.Buy:
            # Look for outstanding sell orders that cross with the buy order
            while limit_order.price >= self.ask_min:
                # Iterate through limit orders at current ask min
                entries = self.price_queues.get(
                    self.ask_min, PriceDeque(price=self.ask_min)
                )

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

                # Exhausted all orders at the current price level, move to
                # the next price level
                self.ask_min += 1

            # If we get here, then there is some quantity we cannot fill,
            # so we enqueue the order in the limit order book
            self._add_order_to_queue(limit_order)

            # Update bid max
            if self.bid_max < limit_order.price:
                self.bid_max = limit_order.price

            # Done
            return None

        else:
            # Sell order
            while limit_order.price <= self.bid_max:

                # Look for existing sell orders to cross with the buy order
                entries = self.price_queues.get(
                    self.bid_max, PriceDeque(price=self.bid_max)
                )

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

                # Exhausted all orders at the current price level, move
                # to the next price level
                self.bid_max -= 1

            # If we get here, then there is some quantity we cannot fill,
            # so we enqueue the order in the limit order book
            self._add_order_to_queue(limit_order)

            # Update ask min
            if self.ask_min > limit_order.price:
                self.ask_min = limit_order.price

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
        self.price_queues[limit_order.price].remove(limit_order)

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

            # If non-empty, append
            if current_level in self.price_queues.keys():
                data.append(self.price_queues[current_level].as_dict())

            # Go to next level
            current_level -= 1

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

            if current_level in self.price_queues.keys():
                data.append(self.price_queues[current_level].as_dict())

            # Go to next level
            current_level += 1

        return pandas.DataFrame(data)
