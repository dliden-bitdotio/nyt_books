"""
    File name: main.py
    Author: Daniel Liden
    Date created: 2021-08-19
    Date last modified: 2021-08-25
    Python Version: 3.9
"""
import extract
import transform
import load

import os

from dotenv import load_dotenv
from getpass import getpass


def _credential():
    """Attempt to load credentials for accessing NYT Books API

    Expects to find credentials in .env file.
    """
    load_dotenv()
    env_vars = ["NYT_KEY", "BITIO_KEY", "BITIO_USERNAME", "PG_STRING"]

    for key in env_vars:
        if key in os.environ:
            pass
        else:
            os.environ[key] = getpass(f"Please Enter Value for {key}\n")

    print(f'{", ".join(env_vars)} loaded into local environment')


if __name__ == "__main__":
    _credential()

    # Extract
    lists = extract.json_from_get_request(
        url="https://api.nytimes.com/svc/books/v3/lists/names.json",
        params={"api-key": os.getenv("NYT_KEY")},
    )

    selected_lists = [
        "combined-print-and-e-book-fiction",
        "combined-print-and-e-book-nonfiction",
    ]

    fic, nonfic = tuple(
        extract.json_from_get_request(
            url=f"https://api.nytimes.com/svc/books/v3/lists/current/{booklist}.json",
            params={"api-key": os.getenv("NYT_KEY")},
        )
        for booklist in selected_lists
    )

    # Transform
    lists_transformed = transform.transform_list_of_booklists(lists)
    fic_transformed = transform.transform_book_list(fic)
    nonfic_transformed = transform.transform_book_list(nonfic)

    # Load
    schema_name = "nyt_books"
    list_of_lists_name = "book_lists"
    fiction_name = selected_lists[0]
    nonfiction_name = selected_lists[1]
    ## Create new Schema if it does not already exist
    load.create_new_schema(
        schema_name=schema_name,
        pg_string=os.getenv("PG_STRING"),
        bitio_key=os.getenv("BITIO_KEY"),
        bitio_username=os.getenv("BITIO_USERNAME"),
    )

    ## Load to bit.io, replacing existing tables
    for (name, data) in zip(
        [list_of_lists_name, fiction_name, nonfiction_name],
        [lists_transformed, fic_transformed, nonfic_transformed],
    ):
        fully_qualified = f'{os.getenv("BITIO_USERNAME")}/{schema_name}.{name}'
        # breakpoint()
        load.to_table(
            df=data,
            destination=fully_qualified,
            pg_string=os.getenv("PG_STRING"),
        )
