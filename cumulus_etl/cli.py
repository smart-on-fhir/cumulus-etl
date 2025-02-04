"""The command line interface to cumulus-etl"""

import argparse
import asyncio
import enum
import logging
import sys
import tempfile

import rich.logging

from cumulus_etl import common, etl, export, inliner, upload_notes
from cumulus_etl.etl import convert, init


class Command(enum.Enum):
    """Subcommand strings"""

    CONVERT = "convert"
    ETL = "etl"
    EXPORT = "export"
    INIT = "init"
    INLINE = "inline"
    UPLOAD_NOTES = "upload-notes"

    # Why isn't this part of Enum directly...?
    @classmethod
    def values(cls):
        return [e.value for e in cls]


def get_subcommand(argv: list[str]) -> str | None:
    """
    Determines which subcommand was requested by the given command line.

    Python's argparse has no good way of setting a default sub-parser.
    (i.e. one that parses the command line if no sub parser subcommand is specified)
    So instead, this method inspects the first positional argument.
    If it's a recognized command, we return it. Else None.
    """
    for i, arg in enumerate(argv):
        if arg in Command.values():
            return argv.pop(i)  # remove it to make later parsers' jobs easier
        elif not arg.startswith("-"):
            # first positional arg did not match a known command, assume default command
            return None


async def main(argv: list[str]) -> None:
    # Use RichHandler for logging because it works better when interacting with other rich components
    # (e.g. I've seen the default logger lose the last warning emitted when progress bars are also active).
    # But also turn off all the complex bits - we just want the message.
    logging.basicConfig(
        format="%(message)s",
        handlers=[rich.logging.RichHandler(show_time=False, show_level=False, show_path=False)],
    )

    subcommand = get_subcommand(argv)

    prog = "cumulus-etl"
    if subcommand:
        prog += f" {subcommand}"  # to make --help look nicer
    parser = argparse.ArgumentParser(prog=prog)

    if subcommand == Command.UPLOAD_NOTES.value:
        run_method = upload_notes.run_upload_notes
    elif subcommand == Command.CONVERT.value:
        run_method = convert.run_convert
    elif subcommand == Command.EXPORT.value:
        run_method = export.run_export
    elif subcommand == Command.INIT.value:
        run_method = init.run_init
    elif subcommand == Command.INLINE.value:
        run_method = inliner.run_inline
    else:
        parser.description = "Extract, transform, and load FHIR data."
        if not subcommand:
            # Add a note about other subcommands we offer, and tell argparse not to wrap our formatting
            parser.formatter_class = argparse.RawDescriptionHelpFormatter
            parser.description += "\n\nother commands available:\n"
            parser.description += "  convert\n  export\n  init\n  inline\n  upload-notes"
        run_method = etl.run_etl

    with tempfile.TemporaryDirectory() as tempdir:
        common.set_global_temp_dir(tempdir)
        await run_method(parser, argv)


def main_cli():
    asyncio.run(main(sys.argv[1:]))  # pragma: no cover


if __name__ == "__main__":
    main_cli()  # pragma: no cover
