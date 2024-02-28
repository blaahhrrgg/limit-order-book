import pandas
from importlib import resources


def load_msft_orders() -> pandas.DataFrame:
    """Load MSFT limit order data

    This dataset contains limit orders from a sample file provided by LOBSTER
    for the ticker MSFT on 2012-06-21.

    The fields are defined as follows:
    * Time:
        Seconds after midnight with decimal precision of at least milliseconds
        and up to nanoseconds depending on the requested period.
    * Type:
        The event type,
            1: Submission of a new limit order
            2: Cancellation (Partial deletion
               of a limit order)
            3: Deletion (Total deletion of a limit order)
            4: Execution of a visible limit order
            5: Execution of a hidden limit order
            7: Trading halt indicator
    * OrderID:
        The unique order reference number (assigned in order flow).
    * Size:
        The number of shares.
    * Price
        The dollar price times 10000, i.e., a stock price of $91.14 is given
        by 911400.
    * Direction:
        The direction of the order,
            -1: Sell limit order
            1: Buy limit order

    Returns
    -------
    pandas.DataFrame
        A DataFrame of sample limit order data.

    Examples
    --------
    >>> from limit_order_book.datasets import load_msft_orders
    >>> df = load_msft_orders()
    >>> df.head()
               Time  Type   OrderID  Size   Price  Direction
    0  34200.013994     3  16085616   100  310400         -1
    1  34200.013994     1  16116348   100  310500         -1
    2  34200.015248     1  16116658   100  310400         -1
    3  34200.015442     1  16116704   100  310500         -1
    4  34200.015789     1  16116752   100  310600         -1

    References
    ----------
    . https://lobsterdata.com/info/DataSamples.php
    """
    fname = "MSFT_2012-06-21_34200000_37800000_message_50.csv.pq"
    path = resources.files("limit_order_book.datasets.data").joinpath(fname)

    return pandas.read_parquet(path)
