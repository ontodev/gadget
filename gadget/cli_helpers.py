import logging
import os
import re

from configparser import ConfigParser
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection
from typing import Union


def get_connection(path: str) -> Union[Connection, None]:
    """Get a database connection from either a SQLite database file, or a PostgreSQL config.

    :param path: path to database or config file
    :return: database connection or None on problem loading config file
    """
    if path.endswith(".db"):
        abspath = os.path.abspath(path)
        db_url = "sqlite:///" + abspath
        engine = create_engine(db_url)
        return engine.connect()
    elif path.endswith(".ini"):
        config_parser = ConfigParser()
        config_parser.read(path)
        if config_parser.has_section("postgresql"):
            params = {}
            for param in config_parser.items("postgresql"):
                params[param[0]] = param[1]
        else:
            logging.error(
                "Unable to create database connection; missing [postgresql] section from " + path
            )
            return None
        pg_user = params.get("user")
        if not pg_user:
            logging.error(
                "Unable to create database connection: missing 'user' parameter from " + path
            )
            return None
        pg_pw = params.get("password")
        if not pg_pw:
            logging.error(
                "Unable to create database connection: missing 'password' parameter from " + path
            )
            return None
        pg_db = params.get("database")
        if not pg_db:
            logging.error(
                "Unable to create database connection: missing 'database' parameter from " + path
            )
            return None
        pg_host = params.get("host", "127.0.0.1")
        pg_port = params.get("port", "5432")
        db_url = f"postgresql+psycopg2://{pg_user}:{pg_pw}@{pg_host}:{pg_port}/{pg_db}"
        engine = create_engine(db_url)
        return engine.connect()
    logging.error(
        "Either a database file or a config file must be specified with a .db or .ini extension"
    )
    return None


def get_terms(term_list: list, terms_file: str) -> list:
    """Get a list of terms from a list and/or a file from args.

    :param term_list: list of input terms
    :param terms_file: path to file containing input terms
    :return: list of terms
    """
    terms = term_list or []
    if terms_file:
        with open(terms_file, "r") as f:
            for line in f:
                if line.startswith("#"):
                    continue
                if not line.strip():
                    continue
                m = re.match(r"(.+)\s#.+", line)
                if m:
                    terms.append(m.group(1).strip())
                else:
                    terms.append(line.strip())
    return terms
