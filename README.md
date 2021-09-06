# bit.io New York Times Books Pipeline
A simple bit.io ETL pipeline for getting data from the New York Times Books API into a Postgres Database on bit.io.

# Setup
1. Create a New York Times Developer Account and register an app ([instructions](https://developer.nytimes.com/get-started))
2. Create an account on [bit.io](https://bit.io)
3. Create a `.env` file with the necessary credentials:
  1. Edit the `env_template` file and fill in your credentials. `NYT_KEY` is the API key for the "app" created in step (1). You can find it by navigating [here](https://developer.nytimes.com/my-apps) after creating your account, clicking on the correct app, and copying the API key. `BITIO_KEY`, `BITIO_USERNAME`, and `PG_STRING` can all be found on bit.io. Find them by clicking `connect` from any bit.io repository page. See [here](https://docs.bit.io/docs/connecting-to-bitio) for more details.
  2. Save the edited `env_template` as `.env` in the project's root directory.
4. Create a python virtual environment and install the python package dependencies
  1. `python3 -m venv venv`
  2. `source venv/bin/activate`
  3. `python3 -m pip install --upgrade pip -r requirements.txt`
5. Execute the main script to create a repository (if needed), download the NYT Books lists, apply some basic processing and transformation, and upload the data to bit.io with `python src/main.py`. You can edit this script to, e.g., change the name of the repository and/or tables and to specify different bestseller lists to download. By default, the script downloads the "list of lists" and the combined print and e-book fiction and nonfiction bestseller lists.
