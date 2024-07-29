"""
Code to support running Microsoft's Anonymizer tool

See https://github.com/microsoft/Tools-for-Health-Data-Anonymization for more details.
"""

import asyncio
import glob
import os
import sys

from cumulus_etl import cli_utils, errors

MSTOOL_CMD = "Microsoft.Health.Fhir.Anonymizer.R4.CommandLineTool"


def config_path() -> str:
    return os.path.join(os.path.dirname(__file__), "ms-config.json")


async def run_mstool(input_dir: str, output_dir: str) -> None:
    """
    Runs Microsoft's Anonymizer tool on the input directory and puts the results in the output directory

    The input must be in ndjson format. And the output will be as well.
    """
    process = await asyncio.create_subprocess_exec(
        MSTOOL_CMD,
        "--bulkData",
        f"--configFile={config_path()}",
        f"--inputFolder={input_dir}",
        f"--outputFolder={output_dir}",
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )

    _, stderr = await _wait_for_completion(process, input_dir, output_dir)

    if process.returncode != 0:
        print(
            f"An error occurred while de-identifying the input resources:\n\n{stderr.decode('utf8')}",
            file=sys.stderr,
        )
        raise SystemExit(errors.MSTOOL_FAILED)


async def _wait_for_completion(
    process: asyncio.subprocess.Process, input_dir: str, output_dir: str
) -> (str, str):
    """Waits for the MS tool to finish, with a nice little progress bar, returns stdout and stderr"""
    stdout, stderr = None, None

    with cli_utils.make_progress_bar() as progress:
        task = progress.add_task("De-identifying data", total=1)
        target = _count_file_sizes(f"{input_dir}/*.ndjson")

        while process.returncode is None:
            try:
                # Wait for completion for a moment
                stdout, stderr = await asyncio.wait_for(process.communicate(), 1)
            except asyncio.TimeoutError:
                # MS tool isn't done yet, let's calculate percentage finished so far,
                # by comparing full PHI and de-identified file sizes.
                # They won't perfectly match up (de-id should be smaller), but it's something.
                current = _count_file_sizes(f"{output_dir}/*")
                percentage = _compare_file_sizes(target, current)
                progress.update(task, completed=percentage)

        progress.update(task, completed=1)

    return stdout, stderr


def _compare_file_sizes(target: dict[str, int], current: dict[str, int]) -> float:
    """Gives one percentage for how far toward the target file sizes we are currently"""
    total_expected = sum(target.values())
    total_current = 0
    for filename, size in current.items():
        if filename in target:
            # use target size, because current (de-identified) files will be smaller
            total_current += target[filename]
        else:  # an in-progress file is being written out
            total_current += size
    return total_current / total_expected


def _get_file_size_safe(path: str) -> int:
    try:
        return os.path.getsize(path)
    except FileNotFoundError:
        # The MS Tool moves temporary files around as it completes each file,
        # so we guard against an unlucky race condition of a file being moved
        # before we can query its size. (Total size will be wrong for a moment,
        # but it will correct itself in a second.)
        return 0


def _count_file_sizes(pattern: str) -> dict[str, int]:
    """Returns all files that match the given pattern and their sizes"""
    return {
        os.path.basename(filename): _get_file_size_safe(filename) for filename in glob.glob(pattern)
    }
