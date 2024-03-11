"""Custom client handling, including GoogleSheetsStream base class."""

from __future__ import annotations
from os import PathLike

from typing import Iterable, Any
from pathlib import Path
from singer_sdk import Tap
from singer_sdk._singerlib.schema import Schema
from singer_sdk import typing as th  # JSON Schema typing helpers

from singer_sdk.streams import Stream
from gspread_pandas import Spread, Client


class GoogleSheetsStream(Stream):
    """Stream class for GoogleSheets streams."""

    def __init__(
        self, tap: Tap, schema: str | PathLike | dict[str, Any] | Schema | None = None, name: str | None = None
    ) -> None:
        self._config: dict = dict(tap.config)
        client = self.create_google_sheets_client()
        spread = Spread(self.config["sheet_id"], client=client)
        self.sheet = spread.sheet_to_df()
        self.name = self.config["sheet_name"]
        self._primary_keys = self.config["primary_keys"]
        super().__init__(tap, schema, name)

    @property
    def schema(self):
        schema = {
            "type": "object",
            "properties": {column: {"type": ["string", "null"]} for column in self.sheet.columns},
            "key_properties": [property for property in self.config["primary_keys"]],
        }
        self.logger.info(schema)
        return schema

    def create_google_sheets_client(self):
        service_account = {
            "type": "service_account",
            "project_id": self.config["project_id"],
            "private_key_id": self.config["private_key_id"],
            "private_key": str(self.config["private_key"]).replace("\\n", "\n"),
            "client_email": self.config["client_email"],
            "client_id": self.config["client_id"],
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_x509_cert_url": self.config["client_x509_cert_url"],
            "universe_domain": "googleapis.com",
        }

        client = Client(config=service_account)
        return client

    def get_records(
        self,
        context: dict | None,  # noqa: ARG002
    ) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.

        Args:
            context: Stream partition or context dictionary.

        Raises:
            NotImplementedError: If the implementation is TODO
        """
        return list(filter(lambda record: len(record) > 0, self.sheet.to_dict(orient="records")))
