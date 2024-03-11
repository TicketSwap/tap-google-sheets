"""GoogleSheets tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_google_sheets import client


class TapGoogleSheets(Tap):
    """GoogleSheets tap class."""

    name = "tap-google-sheets"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "project_id",
            th.StringType,
            required=True,
            description="Google API service account",
        ),
        th.Property(
            "private_key_id",
            th.StringType,
            required=True,
            secret=True,
            description="Google API service account private key id",
        ),
        th.Property(
            "private_key",
            th.StringType,
            required=True,
            secret=True,
            description="Google API service account private key",
        ),
        th.Property(
            "client_email",
            th.StringType,
            required=True,
            description="Google API service account client email",
        ),
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            description="Google API service account client id",
        ),
        th.Property(
            "client_x509_cert_url",
            th.StringType,
            required=True,
            description="Google API service account client x509 cert url",
        ),
        th.Property(
            "sheet_id",
            th.StringType,
            required=True,
            description="Google Sheet ID",
        ),
        th.Property(
            "sheet_name",
            th.StringType,
            required=True,
            description="Google Sheet name (will be the stream name)",
        ),
        th.Property(
            "primary_keys",
            th.ArrayType(th.StringType()),
            required=True,
            description="Google Sheet primary keys",
        ),
    ).to_dict()

    def discover_streams(self) -> list[client.GoogleSheetsStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            client.GoogleSheetsStream(self),
        ]


if __name__ == "__main__":
    TapGoogleSheets.cli()
