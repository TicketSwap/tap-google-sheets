"""Custom client handling, including GoogleSheetsStream base class."""

from __future__ import annotations
from os import PathLike
import logging
import typing as t
import json

from typing import Iterable, Any
import singer_sdk._singerlib as singer
from pathlib import Path
from singer_sdk import Tap
from singer_sdk._singerlib.schema import Schema
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.mapper import StreamMap

if t.TYPE_CHECKING:
    import logging

    from singer_sdk.helpers._compat import Traversable
    from singer_sdk.tap_base import Tap

from singer_sdk.streams import Stream
from gspread_pandas import Spread, Client


class GoogleSheetsStream(Stream):
    """Stream class for GoogleSheets streams."""

    def __init__(
        self, tap: Tap, schema: str | PathLike | dict[str, Any] | Schema | None = None, name: str | None = None
    ) -> None:
        self._config: dict = dict(tap.config)
        self.name = self.config["sheet_name"]
        client = self.create_google_sheets_client()
        spread = Spread(self.config["sheet_id"], client=client)
        self.sheet = spread.sheet_to_df(index=0)
        self._primary_keys = self.config["primary_keys"]
        self.logger: logging.Logger = tap.logger.getChild(self.name)
        self.metrics_logger = tap.metrics_logger
        self.tap_name: str = tap.name
        self._tap = tap
        self._tap_state = tap.state
        self._tap_input_catalog: singer.Catalog | None = None
        self._stream_maps: list[StreamMap] | None = None
        self.forced_replication_method: str | None = None
        self._replication_key: str | None = None
        self._state_partitioning_keys: list[str] | None = None
        self._schema_filepath: Path | Traversable | None = None
        self._metadata: singer.MetadataMapping | None = None
        self._mask: singer.SelectionMask | None = None
        self._schema: dict
        self._is_state_flushed: bool = True
        self._last_emitted_state: dict | None = None
        self._sync_costs: dict[str, int] = {}
        self.child_streams: list[Stream] = []
        if schema:
            if isinstance(schema, (PathLike, str)):
                if not Path(schema).is_file():
                    msg = f"Could not find schema file '{self.schema_filepath}'."
                    raise FileNotFoundError(msg)

                self._schema_filepath = Path(schema)
            elif isinstance(schema, dict):
                self._schema = schema
            elif isinstance(schema, singer.Schema):
                self._schema = schema.to_dict()
            else:
                msg = f"Unexpected type {type(schema).__name__} for arg 'schema'."
                raise ValueError(msg)

        if self.schema_filepath:
            self._schema = json.loads(self.schema_filepath.read_text())

        if not self.schema:
            msg = (
                f"Could not initialize schema for stream '{self.name}'. A valid schema "
                "object or filepath was not provided."
            )
            raise ValueError(msg)

    @property
    def schema(self):
        schema = {
            "type": "object",
            "properties": {column: {"type": ["string", "null"]} for column in self.sheet.columns},
        }
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
