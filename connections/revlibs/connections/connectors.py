"""Connectors classes"""
from abc import ABC, abstractmethod

import psycopg2
import pyexasol

from revlibs.connections.config import Config
from revlibs.connections.exceptions import ConnectionEstablishError, ConnectionParamsError


class BaseConnector(ABC):
    """Base class for connectors"""

    def __init__(self, cfg: Config) -> None:
        self.config = cfg
        self.connection = None

    @abstractmethod
    def _connect(self):
        """Establish connection with database"""
        pass

    @abstractmethod
    def is_connection_closed(self):
        """Checks if connection with database closed already"""
        pass

    def get_connection(self):
        """Open new DB connection or return already established if its not closed"""
        if self.connection and not self.is_connection_closed():
            return self.connection
        self.connection = self._connect()
        return self.connection

    def close(self):
        """Close connection to database"""
        if not self.connection:
            return
        self.connection.close()
        self.connection = None


class ExasolConnector(BaseConnector):
    """Connector class for Exasol DB"""

    def is_connection_closed(self) -> bool:
        """Check if connection with database closed already"""
        return self.connection.is_closed

    def _connect(self) -> pyexasol.ExaConnection:
        """Establish connection with exasol"""
        params = {"compression": True}
        if "schema" in self.config:
            params["schema"] = self.config.schema
        params.update(self.config.params)

        try:
            return pyexasol.connect(
                dsn=self.config.dsn,
                user=self.config.user,
                password=self.config.password,
                fetch_dict=True,
                fetch_mapper=pyexasol.exasol_mapper,
                **params,
            )
        except pyexasol.exceptions.ExaConnectionDsnError as exc:
            raise ConnectionEstablishError(
                self.config.name, reason="Bad dsn", dsn=self.config.dsn
            ) from exc
        except pyexasol.exceptions.ExaAuthError as exc:
            raise ConnectionEstablishError(
                self.config.name,
                reason="Authentication failed",
                dsn=self.config.dsn,
                user=self.config.user,
            ) from exc
        except pyexasol.exceptions.ExaConnectionFailedError as exc:
            raise ConnectionEstablishError(
                self.config.name, reason="Connection refused", dsn=self.config.dsn,
            ) from exc
        except pyexasol.exceptions.ExaError as exc:
            raise ConnectionEstablishError(self.config.name, dsn=self.config.dsn) from exc


class PostgresConnector(BaseConnector):
    """Connector class for PostgreSQL DB"""

    def is_connection_closed(self) -> bool:
        """Check if connection with database closed already"""
        return self.connection.closed

    @staticmethod
    def _parse_dsn(data_source_name: str) -> str:
        """Convert connection URI to key/value connection string.

        >>> _parse_dsn('localhost:8888, 127.0.0.1:6543')
        ['host=localhost port=8888', 'host=127.0.0.1 port=6543']
        """
        dsns = data_source_name.split(",")
        for dsn in dsns:
            host, port = dsn.strip().split(":")
            yield f"host={host} port={port}"

    def _connect(self) -> psycopg2.extensions.connection:
        """Establish connection with postgres"""
        dbname = self.config.dbname if ("dbname" in self.config) else None

        connection, last_exception = None, None
        for dsn in self._parse_dsn(self.config.dsn):
            try:
                connection = psycopg2.connect(
                    dsn,
                    user=self.config.user,
                    password=self.config.password,
                    dbname=dbname,
                    **self.config.params,
                )
            except psycopg2.Error as exc:
                last_exception = exc

        if not connection:
            raise ConnectionEstablishError(
                self.config.name, dsn=self.config.dsn, user=self.config.user, dbname=dbname,
            ) from last_exception

        return connection
