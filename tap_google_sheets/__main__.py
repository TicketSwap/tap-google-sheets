"""GoogleSheets entry point."""

from __future__ import annotations

from tap_google_sheets.tap import TapGoogleSheets

TapGoogleSheets.cli()
