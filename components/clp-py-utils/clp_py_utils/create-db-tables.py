import argparse
import logging
import pathlib
import subprocess
import sys

# Setup logging
# Create logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)
# Setup console logging
logging_console_handler = logging.StreamHandler()
logging_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
logging_console_handler.setFormatter(logging_formatter)
logger.addHandler(logging_console_handler)

from contextlib import closing
from sql_adapter import SQL_Adapter
from clp_py_utils.clp_config import (
    COMPRESSION_JOBS_TABLE_NAME,
    COMPRESSION_TASKS_TABLE_NAME,
    Database,
    SEARCH_JOBS_TABLE_NAME,
)
from clp_py_utils.core import read_yaml_config_file
def drop_tables(config_file_path: str):
    try:
        database_config = Database.parse_obj(read_yaml_config_file(config_file_path))
        if database_config is None:
            raise ValueError(f"Database configuration file '{config_file_path}' is empty.")
        sql_adapter = SQL_Adapter(database_config)
        clp_db_connection_params = database_config.get_clp_connection_params_and_type(True)
        table_prefix = clp_db_connection_params["table_prefix"]
        with closing(sql_adapter.create_connection(True)) as clp_db, closing(
            clp_db.cursor(dictionary=True)
        ) as clp_db_cursor:
            clp_db_cursor.execute(f"DROP TABLE IF EXISTS `{table_prefix}files`")
            clp_db_cursor.execute(f"DROP TABLE IF EXISTS `{table_prefix}archives`")
            clp_db_cursor.execute(f"DROP TABLE IF EXISTS `{COMPRESSION_TASKS_TABLE_NAME}`")
            clp_db_cursor.execute(f"DROP TABLE IF EXISTS `{COMPRESSION_JOBS_TABLE_NAME}`")

            clp_db.commit()
    except:
        logger.exception("Failed to drop clp database tables.")
        return -1

    return 0


def main(argv):
    args_parser = argparse.ArgumentParser(description="Creates database tables for CLP.")
    args_parser.add_argument("--config", required=True, help="Database config file.")
    parsed_args = args_parser.parse_args(argv[1:])

    config_file_path = pathlib.Path(parsed_args.config)

    drop_tables(config_file_path)

    script_dir = pathlib.Path(__file__).parent.resolve()

    # fmt: off
    cmd = [
        "python3", str(script_dir / "initialize-clp-metadata-db.py"),
        "--config", str(config_file_path),
    ]
    # fmt: on
    subprocess.run(cmd, check=True)

    # fmt: off
    cmd = [
        "python3", str(script_dir / "initialize-orchestration-db.py"),
        "--config", str(config_file_path),
    ]
    # fmt: on
    subprocess.run(cmd, check=True)

    return 0


if "__main__" == __name__:
    sys.exit(main(sys.argv))
