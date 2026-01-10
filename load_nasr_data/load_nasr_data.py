"""Load NASR data from CSV file into database"""
from dataclasses import dataclass
from logging import handlers
import logging
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel
import geopandas as gpd
from sqlalchemy import create_engine, Engine
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

def configure_logging(log_dir=Path("logs")) -> None:
    """Setup logging for the script.

    :param log_dir: path to the directory with log files.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "log.txt"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)


    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    file_handler = handlers.RotatingFileHandler(
        log_path,
        backupCount=7,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)


def load_log_decorator(func):
    """Decorator for logging loading data into tables."""
    def wrapper(*args, **kwargs):
        table_name = kwargs.get("table_name") or kwargs["data_file_setting"].table_name
        data = kwargs.get("data")

        logging.info("Table %s | Inserting data...", table_name)

        try:
            result = func(*args, **kwargs)
        except Exception as e:
            logging.exception(e)
            return None

        logging.info("Table %s | %s rows inserted.", table_name, len(data))
        return result

    return wrapper


def load_config(path: Path) -> Configuration:
    """Return configuration object based on configuration file.

    :param path: path to the configuration file
    :return: script configuration
    """
    content = safe_load(path.read_text())
    return Configuration(**content)


@load_log_decorator
def load_dict_table(table_name: str,
                       data: list[dict[str, Any]],
                       engine: Engine) -> None:
    """Load single 'dictionary' table with data.

    :param table_name: table name to insert data
    :param data: data to be inserted into the table
    :param engine: target database connection
    """
    df = pd.DataFrame(data)
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False
    )


def load_dict_tables(dict_tables: list[DictTableSettings],
                        engine: Engine) -> None:
    """Load multiple 'dictionary' tables with data.

    :param dict_tables: 'dictionary' tables to be inserted (including data itself)
    :param engine: target database connection
    """
    for table in dict_tables:
        load_dict_table(table_name=table.name,
                        data=table.data,
                        engine=engine)


@dataclass
class DataTableLoader:
    """
    Load data into tables from NASR CSV files based on configuration settings.

    Attributes:
        data_dir: directory with NASR data CSV files
        csv_settings: global CSV settings to read NASR data CSV files
        engine: target database connection
    """

    data_dir: Path
    csv_settings: CSVSettings
    engine: Engine


    def _prepare_data(self, data_file_setting: DataFileSettings) -> pd.DataFrame:
        """Helper method to read data from NASR CSV files.

        :param data_file_setting: file settings for NASR CSV file to be loaded into table
        :return: dataframe of data
        """
        df = pd.read_csv(self.data_dir / data_file_setting.file_name,
                         delimiter=self.csv_settings.delimiter,
                         encoding=self.csv_settings.encoding,
                         quotechar=self.csv_settings.quote_char,
                         usecols=data_file_setting.columns,
                         dtype=str)
        df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
        df.rename(columns={col: col.lower()
                           for col in df.columns},
                  inplace=True)
        return df

    @load_log_decorator
    def _load_non_spatial(self, data: pd.DataFrame,
                            data_file_setting: DataFileSettings) -> None:
        """Load NASR CSV file into tables without spatial information.

        :param data: data to be loaded into table
        :param data_file_setting: file settings for NASR CSV file to be loaded into table
        """
        data.to_sql(name=data_file_setting.table_name,
                    con=self.engine,
                    if_exists="append",
                    index=False)

    @load_log_decorator
    def _load_spatial(self,
                        data: pd.DataFrame,
                        data_file_setting: DataFileSettings) -> None:
        """Load NASR CSV file into tables with spatial information.

        :param data: data to be loaded into table
        :param data_file_setting: file settings for NASR CSV file to be loaded into table
        """
        gdf = gpd.GeoDataFrame(
            data=data,
            geometry=gpd.points_from_xy(data.long_decimal, data.lat_decimal),
            crs="EPSG:4326"
        )
        gdf.drop(columns=["long_decimal", "lat_decimal"], inplace=True)
        gdf.to_postgis(name=data_file_setting.table_name,
                       con=self.engine,
                        if_exists="append",
                        index=False,
                        chunksize=10000)


    def load_table(self, data_file_setting: DataFileSettings) -> None:
        """Load NASR CSV file into tables.

        :param data_file_setting: file settings for NASR CSV file to be loaded into table
        """
        data = self._prepare_data(data_file_setting)
        if data_file_setting.is_spatial:
            self._load_spatial(data=data,
                               data_file_setting=data_file_setting)
        else:
            self._load_non_spatial(data=data,
                                   data_file_setting=data_file_setting)


def main():
    configure_logging()
    config = load_config(Path("config.yaml"))
    engine = create_engine(
        "postgresql+psycopg2://{user}:{password}@{host}:5432/{database}".format(**config.nasr_db.model_dump())
    )

    load_dict_tables(dict_tables=config.dict_tables,
                     engine=engine)

    data_tables_lookup = {t.file_name : t for t in config.data_tables}
    dtl = DataTableLoader(data_dir=config.data_dir,
                          csv_settings=config.csv_settings,
                          engine=engine)
    dtl.load_table(data_file_setting=data_tables_lookup["AWOS.csv"])

    dtl.load_table(data_file_setting=data_tables_lookup["FIX_BASE.csv"])
    dtl.load_table(data_file_setting=data_tables_lookup["FIX_CHRT.csv"])
    dtl.load_table(data_file_setting=data_tables_lookup["FIX_NAV.csv"])

    dtl.load_table(data_file_setting=data_tables_lookup["LID.csv"])

    dtl.load_table(data_file_setting=data_tables_lookup["NAV_BASE.csv"])
    dtl.load_table(data_file_setting=data_tables_lookup["NAV_CKPT.csv"])
    dtl.load_table(data_file_setting=data_tables_lookup["NAV_RMK.csv"])

    dtl.load_table(data_file_setting=data_tables_lookup["RDR.csv"])

    dtl.load_table(data_file_setting=data_tables_lookup["WXL_BASE.csv"])
    dtl.load_table(data_file_setting=data_tables_lookup["WXL_SVC.csv"])


if __name__ == "__main__":
    main()
