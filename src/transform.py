"""
    File name: transform.py
    Author: Daniel Liden
    Date created: 2021-08-19
    Date last modified: 2021-08-25
    Python Version: 3.9
"""

import pandas as pd
import numpy as np

from datetime import datetime


def transform_list_of_booklists(lists_dict: dict):
    lists = pd.DataFrame(lists_dict["results"])
    lists.loc[:, ["oldest_published_date", "newest_published_date"]] = lists.loc[
                                                                       :, ["oldest_published_date",
                                                                           "newest_published_date"]
                                                                       ].applymap(
        lambda x: datetime.strptime(x, "%Y-%m-%d").date())

    return lists


def _get_links(col: str, name: str) -> str:
    """returns specified links from buy_links by name"""
    return [d for d in col if d["name"] == name][0]["url"]


def transform_book_list(book_list_dict: dict) -> pd.DataFrame:
    """Transform NYT bestseller lists"""
    keep_columns = [
        "rank",
        "rank_last_week",
        "weeks_on_list",
        "asterisk",
        "dagger",
        "primary_isbn10",
        "primary_isbn13",
        "publisher",
        "description",
        "title",
        "author",
        "contributor",
        "contributor_note",
        "amazon_product_url",
        "age_group",
        "book_uri",
        "buy_links",
    ]
    books = pd.DataFrame(book_list_dict["results"]["books"]).loc[:, keep_columns]
    books["indiebound_buy_link"] = books["buy_links"].apply(
        _get_links, name="IndieBound"
    )
    books["bookshop_buy_link"] = books["buy_links"].apply(_get_links, name="Bookshop")

    books = books.astype(
        {"rank": np.int16, "rank_last_week": np.int16, "weeks_on_list": np.int16}
    )

    return books.drop("buy_links", axis=1)
