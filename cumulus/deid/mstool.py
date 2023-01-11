"""
Code to support running Microsoft's Anonymizer tool

See https://github.com/microsoft/Tools-for-Health-Data-Anonymization for more details.
"""

import os
import subprocess  # nosec: B404
import sys

from cumulus import common, errors

MSTOOL_CMD = "Microsoft.Health.Fhir.Anonymizer.R4.CommandLineTool"


def config_path() -> str:
    return os.path.join(os.path.dirname(__file__), "ms-config.json")


def run_mstool(input_dir: str, output_dir: str) -> None:
    """
    Runs Microsoft's Anonymizer tool on the input directory and puts the results in the output directory

    The input must be in ndjson format. And the output will be as well.
    """
    common.print_header("De-identifying data...")
    try:
        # The following call only points at some temporary directory names (which we generate),
        # so it should be safe, and we thus disable the security linter warning about validating inputs.
        subprocess.run(  # nosec: subprocess_without_shell_equals_true
            [
                MSTOOL_CMD,
                "--bulkData",
                f"--configFile={config_path()}",
                f"--inputFolder={input_dir}",
                f"--outputFolder={output_dir}",
            ],
            capture_output=True,
            check=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        print(f"An error occurred while de-identifying the input resources:\n\n{exc.stderr}", file=sys.stderr)
        raise SystemExit(errors.MSTOOL_FAILED) from exc
