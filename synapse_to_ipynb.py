from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
)

logger = logging.getLogger(__name__)


class NotebookDirectoryManager:
    # https://ipython.org/ipython-doc/3/notebook/nbformat.html
    _DEFAULT_IPYNB = {
        "nbformat": 4,
        "nbformat_minor": 0,
        "metadata": {},
        "cells": [],
    }

    def __init__(self, synapse_dir: Path, ipynb_dir: Path) -> None:
        if not synapse_dir.is_dir():
            raise NotADirectoryError(f"'{synapse_dir}' is not a valid directory")
        if not ipynb_dir.is_dir():
            raise NotADirectoryError(f"'{ipynb_dir}' is not a valid directory")

        self.synapse_dir = synapse_dir.resolve(strict=True)
        self.ipynb_dir = ipynb_dir.resolve(strict=True)

        # Synapse does not have an actual folder structure,
        # it is stored in the json metadata so we just use *.json here.
        # Sorted by file name so it can be used with zip
        self.synapse_nbs = sorted(self.synapse_dir.glob("*.json"), key=lambda f: f.stem)
        self.ipynbs = sorted(self.ipynb_dir.glob("**/*.ipynb"), key=lambda f: f.stem)

        _synapse_nb_stems = [f.stem for f in self.synapse_nbs]
        _ipynb_stems = [f.stem for f in self.ipynbs]
        # Synapse does not allow duplicate file names
        _dup_synapse_stems = {
            f for f in _synapse_nb_stems if _synapse_nb_stems.count(f) > 1
        }
        _dup_ipynb_stems = {f for f in _ipynb_stems if _ipynb_stems.count(f) > 1}
        if _dup_ipynb_stems or _dup_synapse_stems:
            raise ValueError(
                "Duplicate File Name(s) found:\n"
                f"Synapse Directory: {_dup_synapse_stems}\n"
                f"IPYNB Notebook Directory: {_dup_ipynb_stems}\n"
            )

        self.synapse_nb_stems = set(_synapse_nb_stems)
        self.ipynb_stems = set(_ipynb_stems)

        self.synapse_only_nbs = [
            nb for nb in self.synapse_nbs if nb.stem not in self.ipynb_stems
        ]
        self.ipynb_only_nbs = [
            ipynb for ipynb in self.ipynbs if ipynb.stem not in self.synapse_nb_stems
        ]

    @staticmethod
    def update_synapse_notebook_from_ipynb(synnb_path: Path, ipynb_path: Path) -> None:
        """
        Update the 'cells' contents of a Synapse notebook with the contents
        of a Jupyter notebook (.ipynb).

        :param synnb_path: The Synapse notebook to update.
        :type synnb_path: str
        :param ipynb_path: The source ipynb to update the Synapse notebook from.
        :type ipynb_path: str

        :return: 0 if the notebook was updated successfully.
        :rtype: int
        """

        # Read the ipynb src that we will use to update the synapse notebook with
        with open(ipynb_path, encoding="utf-8") as nb:
            ipynb_cells = json.load(nb)["cells"]

        # Create a temporary notebook file
        fd, tmp_notebook_path = tempfile.mkstemp(dir=synnb_path.parent)
        # Close the descriptor. We'll manage this file ourselves.
        os.close(fd)
        try:
            # Copy the contents of the actual notebook file to the temporary one
            shutil.copyfile(synnb_path, tmp_notebook_path)

            # Update the temporary synpase notebook file's 'cells'
            # property with the ipynb src we read earlier
            with open(tmp_notebook_path, "r+", encoding="utf-8") as f:
                synnb_json = json.load(f)
                synnb_json["properties"]["cells"] = ipynb_cells
                f.seek(0)
                f.truncate()
                json.dump(synnb_json, f, ensure_ascii=False, indent=4)

            # Finally, replace the synpase notebook with the updated temporary one
            os.replace(tmp_notebook_path, synnb_path)
        except BaseException:
            os.remove(tmp_notebook_path)
            raise

    def create_ipynb_from_synapse_nb(self, synapse_nb_path: Path) -> Path:
        """
        Creates a Jupyter Notebook (.ipynb) from a Synapse Notebook.

        :return: The Path to the the newly created ipynb file
        :raises: KeyError: if the 'properties' key does not exist
        """

        # Read in the synapse notebook.
        with open(synapse_nb_path, encoding="utf-8") as nb:
            # We're only interested in the 'properties' key.
            synapse_notebook: dict[str, Any] = json.load(nb)["properties"]

        # Update the default data with the data from the synapse notebook
        ipynb_data = {}
        ipynb_data.update(
            {
                key: synapse_notebook.get(key, default)
                for key, default in self._DEFAULT_IPYNB.items()
            }
        )

        # Synapse doesnt create the 'metadata' key by default.
        # It's required so we'll create it here, if it doesnt exist.
        cells: list[dict[str, Any]] = ipynb_data["cells"]
        for cell in cells:
            cell.setdefault("metadata", {})

        # Create the new .ipynb file in the target directory.
        ipynb_filename = f"{synapse_nb_path.stem}.ipynb"

        try:
            sub_dir = str(synapse_notebook["folder"]["name"])
        except KeyError:
            sub_dir = None

        ipynb_dir = (self.ipynb_dir / sub_dir) if sub_dir else self.ipynb_dir
        ipynb_dir.mkdir(exist_ok=True, parents=True)
        ipynb_path = ipynb_dir / ipynb_filename
        with open(ipynb_path, "w", encoding="utf-8") as ipynb:
            json.dump(ipynb_data, ipynb, ensure_ascii=False, indent=4)

        return ipynb_path


def update_synapse_nbs(manager: NotebookDirectoryManager) -> int:
    # These need to be the same, so lets check.
    if manager.synapse_nb_stems != manager.ipynb_stems:
        msg = (
            "The file name's in the source directory "
            "dont't match those in the target directory:"
        )
        synapse_only_stems = manager.synapse_nb_stems - manager.ipynb_stems
        ipynb_only_stems = manager.ipynb_stems - manager.synapse_nb_stems
        if synapse_only_stems:
            msg += f"\nIn '{manager.synapse_dir.name}' only: {synapse_only_stems}"
        if ipynb_only_stems:
            msg += f"\nIn '{manager.ipynb_dir.name}' only: {ipynb_only_stems}"
        logger.error(msg)
        return 1

    ret = 0
    for synnb, ipynb in zip(manager.synapse_nbs, manager.ipynbs, strict=True):
        try:
            manager.update_synapse_notebook_from_ipynb(synnb, ipynb)
        except KeyError as e:
            msg = (
                f"Unable to update Synapse notebook '{synnb.name}' from '{ipynb.name}'"
            )
            logger.error(
                f"{msg}: {e}",
                stack_info=True,
            )
            ret |= 1
        except json.JSONDecodeError as e:
            msg = (
                f"An error occured updating '{synnb.name}' from '{ipynb.name}'. "
                f"The ipynb '{ipynb.name}' could not be loaded"
            )
            logger.error(
                f"{msg}: {repr(e)}",
                stack_info=True,
            )
            ret |= 1
        else:
            logger.info(f"Updated '{synnb.name}' from '{ipynb.name}'")
            ret |= 0
    return ret


def delete_files(files: list[Path]) -> int:
    ret = 0
    for f in files:
        try:
            f.unlink()
        except FileNotFoundError as e:
            logger.error(f"Unable to delete '{f.name}': {e}")
            ret |= 1
        else:
            logger.info(f"Deleted '{f.name}'")
            ret |= 0
    return ret


def create_synapse_only_nbs(manager: NotebookDirectoryManager) -> int:
    ret = 0
    for synapse_nb in manager.synapse_only_nbs:
        try:
            ipynb_path = manager.create_ipynb_from_synapse_nb(synapse_nb)
        except (KeyError, json.JSONDecodeError) as e:
            _msg = f"Unable to create ipynb from '{synapse_nb.name}': %s"
            logger.error(_msg % (f"Missing key: {e}" if isinstance(e, KeyError) else e))
            ret |= 1
        else:
            ret |= 0
            logger.info(f"Created '{ipynb_path.name}' from '{synapse_nb.name}'")
    return ret


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Create or Delete Jupyter notebooks from Synapse notebooks. "
            "If the '--update' flag is set, update existing Synapse notebooks "
            "from Jupyter notebooks"
        )
    )
    parser.add_argument(
        "--source",
        help="The source directory containing Synapse notebooks (.json)",
        type=Path,
        metavar="DIR",
        required=True,
    )
    parser.add_argument(
        "--target",
        help="The target directory containing Jupyter notebooks (.ipynb)",
        type=Path,
        metavar="DIR",
        required=True,
    )
    parser.add_argument(
        "--update",
        help=(
            "Set this flag to instead update the Synapse notebooks in the "
            "source directory with the contents of the Jupyter notebooks "
            "in the target directory"
        ),
        action="store_true",
    )

    args = parser.parse_args(argv)

    # If we are creating our first Notebook in Synapse, this directory might not exist
    create_target_dir = not args.update
    if not args.target.is_dir() and create_target_dir:
        args.target.mkdir()
        logger.info(f"Created directory '{args.target}'")

    try:
        manager = NotebookDirectoryManager(args.source, args.target)
    except (NotADirectoryError, ValueError) as e:
        logger.error(e)
        return 1

    ret = 0

    if args.update:
        # Handle Modified notebooks
        ret = update_synapse_nbs(manager)
    else:
        # Handle New / Deleted / Renamed notebooks
        if not (manager.synapse_only_nbs or manager.ipynb_only_nbs):
            logger.error("Nothing to create or delete.")
            return 1
        ret = create_synapse_only_nbs(manager)
        ret |= delete_files(manager.ipynb_only_nbs)

    return ret


if __name__ == "__main__":
    raise SystemExit(main())
