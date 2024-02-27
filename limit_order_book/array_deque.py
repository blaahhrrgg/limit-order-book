from collections import deque
import pandas
from typing import AnyStr, Optional

from .limit_order_book import BaseLimitOrderBook
from .order import (
    LimitOrder,
    MatchedOrder,
    Direction
)


class ArrayDequeLimitOrderBook(BaseLimitOrderBook):
    """A flat linear array implementation of a limit order book.

    This implementation of a limit order book relies on i) an array of
    double-ended queue at each price and ii) a dictionary to lookup limit
    orders from a given order identifier and is inspired by the Voyager
    submission to the QuantCup, see below references.

    The attributes `bid_max` and `ask_min` maintain the starting point for any
    search to match orders. The value of `bid_max` corresponds to the maximum
    price in which there is a buy order. Analogously, the value of `ask_min`
    corresponds to the minimum price at which there is a sell order.

    When a buy order arrives, a search for any existing sell orders that
    cross with the new order is conducted. The search is started from `ask_min`
    and is incremented upwards until either (a) the buy order is fully
    matched, or (b), a price point is reached that no longer crosses with the
    incoming order. For case (b), a new limit order is added to the book. An
    incoming sell order is handled analogously.

    When a limit order is cancelled, the order is looked-up using the order
    dictionary and then deleted from the deque of orders at the given price
    level.

    This is inspired by the Voyager submission to the QuantCup, see below
    references for more details.

    References
    ----------
    . https://web.archive.org/web/20141222151051/https://dl.dropboxusercontent.com/u/3001534/engine.c
    . https://github.com/kmanley/orderbook
    """

    def __init__(self, name: AnyStr, max_price: int) -> None:
        super().__init__(name, max_price)
        self.bid_max = 0
        self.ask_min = max_price + 1
        self.orders = dict()
        self.price_queues = [deque() for _ in range(self.max_price)]

    def _add_order_to_queue(self, limit_order: LimitOrder) -> None:
        self.orders[limit_order.id] = limit_order
        self.price_queues[limit_order.price].append(limit_order)

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
            while price >= self.ask_min:
                # Iterate through limit orders at current ask min
                entries = self.price_queues[self.ask_min]

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

                # Exhausted all orders at the current price level, move to
                # the next price level
                self.ask_min += 1

            # If we get here, then there is some quantity we cannot fill,
            # so we enqueue the order in the limit order book
            limit_order = LimitOrder(
                trader_id=trader_id,
                direction=direction,
                quantity=quantity,
                price=price,
            )

            self._add_order_to_queue(limit_order)

            # Update bid max
            if self.bid_max < price:
                self.bid_max = price

            return limit_order

        else:
            # Sell order
            while price <= self.bid_max:
                # Look for existing sell orders to cross with the buy order
                entries = self.price_queues[self.bid_max]

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

                # Exhausted all orders at the current price level, move
                # to the next price level
                self.bid_max -= 1

            # If we get here, then there is some quantity we cannot fill,
            # so we enqueue the order in the limit order book
            limit_order = LimitOrder(
                trader_id=trader_id,
                direction=direction,
                quantity=quantity,
                price=price,
            )

            self._add_order_to_queue(limit_order)

            # Update ask min
            if self.ask_min > price:
                self.ask_min = price

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
        return pandas.DataFrame([
            {
                "Price": price,
                "Quantity": sum([order.quantity for order in
                                 self.price_queues[price]]),
                "NumOrders": len(self.price_queues[price]),
            }
            for price in range(self.bid_max, self.bid_max - levels, -1)
        ])

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

        return pandas.DataFrame([
            {
                "Price": price,
                "Quantity": sum([order.quantity for order in
                                 self.price_queues[price]]),
                "NumOrders": len(self.price_queues[price]),
            }
            for price in range(self.ask_min, self.ask_min + levels)
        ])

