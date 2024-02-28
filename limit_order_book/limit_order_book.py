import abc
import pandas
from typing import AnyStr, List

from rich.panel import Panel
from rich.layout import Layout

from .order import LimitOrder, MatchedOrder
from .rich import df_to_rich_table, repr_rich


class BaseLimitOrderBook(abc.ABC):
    """Abstract base class to define common interface of a limit order book.

    Attributes:
    -----------
    name: AnyStr
        The name of the financial instrument associated to the limit order
        book.
    max_price: int
        The max price permissible within the limit order book.
    """

    def __init__(
        self,
        name: AnyStr,
        max_price: int,
    ):
        self.name = name
        self.max_price = max_price
        self._matches: List[MatchedOrder] = []

    @abc.abstractmethod
    def add(self, limit_order: LimitOrder) -> None:
        """Add a limit order to the limit order book."""
        raise NotImplementedError

    def execute(self, matched_order: MatchedOrder) -> None:
        """Execute match between limit orders.

        In a production setting, this would be a callback function that would
        provide the means to dispatch updates to market participants of trades.
        Here, we simply keep track of matches in `_matches`.
        """
        self._matches.append(matched_order)

    @abc.abstractmethod
    def cancel(self, order_id: AnyStr) -> None:
        """Removes an order from the limit order book."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def best_ask(self) -> int:
        """The current best ask price in the limit order book."""
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def best_bid(self) -> int:
        """The current best bid price in the limit order book."""
        raise NotImplementedError

    @property
    def spread(self) -> int:
        """The current spread between the current best bid and ask price."""
        return self.best_ask - self.best_bid

    @abc.abstractmethod
    def get_top_bids_as_dataframe(self, levels=10) -> pandas.DataFrame:
        """Returns the top bids of the order book as a DataFrame."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_top_asks_as_dataframe(self, levels=10) -> pandas.DataFrame:
        """Returns the top asks of the order book as a DataFrame."""
        raise NotImplementedError

    def get_latest_matches(self, n=10) -> pandas.DataFrame:
        """Returns the latest matches of the order book as a DataFrame."""
        return pandas.DataFrame(
            match.as_dict for match in self._matches[-n:]
        )

    def __rich__(self):
        """Method to provide a rich terminal representation of the order book

        Useful for debugging and visualising the order book.
        """

        # Fetch data at top of book
        bids = self.get_top_bids_as_dataframe(levels=10)
        asks = self.get_top_asks_as_dataframe(levels=10)
        matches = self.get_latest_matches(n=10)

        # Create layout
        layout = Layout(size=10)
        layout.split_row(
            Layout(df_to_rich_table(bids, title="Bids"), name="left"),
            Layout(df_to_rich_table(asks, title="Asks"), name="middle"),
            Layout(df_to_rich_table(matches, title="Matches"), name="right"),
        )

        return Panel(
            layout,
            title=f"Limit Order Book for {self.name}"
        )

    def __repr__(self):
        return repr_rich(self.__rich__())
