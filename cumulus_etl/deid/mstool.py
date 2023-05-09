"""
Code to support running Microsoft's Anonymizer tool

See https://github.com/microsoft/Tools-for-Health-Data-Anonymization for more details.
"""

import asyncio
import os
import sys

from cumulus_etl import common, errors

MSTOOL_CMD = "Microsoft.Health.Fhir.Anonymizer.R4.CommandLineTool"


def config_path() -> str:
    return os.path.join(os.path.dirname(__file__), "ms-config.json")


async def run_mstool(input_dir: str, output_dir: str) -> None:
    """
    Runs Microsoft's Anonymizer tool on the input directory and puts the results in the output directory

    The input must be in ndjson format. And the output will be as well.
    """
    common.print_header("De-identifying data...")

    process = await asyncio.create_subprocess_exec(
        MSTOOL_CMD,
        "--bulkData",
        f"--configFile={config_path()}",
        f"--inputFolder={input_dir}",
        f"--outputFolder={output_dir}",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await process.communicate()

    if process.returncode != 0:
        print(
            f"An error occurred while de-identifying the input resources:\n\n{stderr.decode('utf8')}", file=sys.stderr
        )
        raise SystemExit(errors.MSTOOL_FAILED)
