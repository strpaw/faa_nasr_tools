"""Load NASR data from CSV file into database"""
from pathlib import Path
from typing import Any

from pydantic import BaseModel
from yaml import safe_load

ColumName = str
ColumnValue = Any


class DBConnection(BaseModel):
    """
    Database connection settings.

    Attributes:
        host: Database server hostname or IP address.
        database: Name of the database.
        user: Username used for authentication.
        password: Password used for authentication.
    """
    host: str
    database: str
    user: str
    password: str


class DictTableSettings(BaseModel):
    """
    Settings for 'dictionary' tables.

    Attributes:
        name: table name
        data: data to be inserted into the table
    """
    name: str
    data: list[dict[ColumName, ColumnValue]]


class CSVSettings(BaseModel):
    """
    Settings for reading CSV files. All values are expected to match the corresponding parameters
    accepted by `pandas.read_csv`.

    Attributes:
        encoding: Character encoding used for the CSV file
        delimiter: Character used to separate fields
        quote_char: Character used to quote fields
    """
    encoding: str
    delimiter: str
    quote_char: str


class DataFileSettings(BaseModel):
    """
    Settings for processing NASR CSV file - reading, mapping to the corresponding table.

    Attributes:
        file_name: NASR CSV file name used as source data for the table
        table_name: corresponding table name.
        is_spatial (bool): Whether the data contains spatial information.
        columns: column names to load into table.
    """
    file_name: str
    table_name: str
    is_spatial: bool
    columns: list[str]


class Configuration(BaseModel):
    """
    Application configuration.

    Attributes:
        nasr_db: nasr database connection.
        csv_settings: global CSV settings to read NASR data CSV files
        data_dir: directory with NASR data CSV files
        dict_tables: settings for 'dictionary' tables (including data)
        data_tables: settings for NASR data tables including:
            - mapping between CSV file and table name
            - columns to be loaded from CSV into table
    """
    nasr_db: DBConnection
    csv_settings: CSVSettings
    data_dir: Path
    dict_tables: list[DictTableSettings]
    data_tables: list[DataFileSettings]


def load_config(path: Path) -> Configuration:
    """Return configuration object based on configuration file.

    :param path: path to the configuration file
    :return: script configuration
    """
    content = safe_load(path.read_text())
    return Configuration(**content)


def main():
    config = load_config(Path("config.yaml"))


if __name__ == "__main__":
    main()
