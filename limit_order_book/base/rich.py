import pandas
from rich import box
from rich.table import Table
from rich.console import Console, RenderableType


def df_to_rich_table(
        df: pandas.DataFrame,
        title: str = "",
        title_style: str = "",
        max_rows: int = 20,
        table_box: box = box.SIMPLE) -> Table:
    """Convert a DataFrame to a rich table

    Parameters
    ----------
    df: pandas.DataFrame
        The DataFrame to convert to a rich table.
    title: AnyStr
        The title of the table.
    title_style:
        The style of the title of the table.
    max_rows: int
        The maximum number of rows in the rich table
    table_box
        The rich box style, e.g., box.SIMPLE

    Returns
    -------
    rich.table.Table
        The rich.Table representation of the pandas.DataFrame
    """

    rich_table = Table(
        box=table_box,
        row_styles=["bold", ""],
        title=title,
        title_style=title_style or "bold"
    )

    for column in df.columns:
        rich_table.add_column(column)

    if len(df) > max_rows:
        head = df.head(max_rows // 2)
        tail = df.tail(max_rows // 2)

        data_for_display = pandas.concat([
            head,
            pandas.DataFrame(data=[{col: '...' for col in df.columns}]),
            tail]
        )

    else:
        data_for_display = df

    for index, value_list in enumerate(data_for_display.values.tolist()):
        row = [str(x) for x in value_list]
        rich_table.add_row(*row)

    return rich_table


def repr_rich(renderable: RenderableType) -> str:
    """Renders a rich object to a string

    It implements one of the methods of capturing output listed here

    https://rich.readthedocs.io/en/stable/console.html#capturing-output

    Parameters
    ----------
    renderable: RenderableType
        The rich renderable

    Returns
    -------
    AnyStr
        The string representation of the rich object
    """
    console = Console()
    with console.capture() as capture:
        console.print(renderable)
    str_output = capture.get()
    return str_output
