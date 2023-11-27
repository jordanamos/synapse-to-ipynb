from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest

from synapse_to_ipynb import delete_files
from synapse_to_ipynb import main
from synapse_to_ipynb import NotebookDirectoryManager
from synapse_to_ipynb import update_synapse_nbs
from testing.notebooks import IPYNB
from testing.notebooks import SYNAPSE_NB


@pytest.fixture
def temp_dirs(tmp_path: Path):
    dir1 = tmp_path / "dir1"
    dir2 = tmp_path / "dir2"
    [dir.mkdir() for dir in (dir1, dir2)]
    yield dir1, dir2


@pytest.fixture
def manager(request, temp_dirs):
    src_dir, trgt_dir = temp_dirs
    if hasattr(request, "param") and request.param:
        src_files, trgt_files = request.param
        for f in src_files:
            (src_dir / f).touch()
        for f in trgt_files:
            (trgt_dir / f).touch()
    return NotebookDirectoryManager(src_dir, trgt_dir)


@pytest.fixture
def tmp_ipynb(tmp_path):
    ipynb = tmp_path / "f.ipynb"
    with open(ipynb, "w") as f:
        json.dump(IPYNB, f)
    return ipynb


@pytest.fixture
def tmp_synb(tmp_path):
    synb = tmp_path / "f.json"
    with open(synb, "w") as f:
        json.dump(SYNAPSE_NB, f)
    return synb


def test_main_no_files(temp_dirs):
    src_dir, trgt_dir = (str(dir) for dir in temp_dirs)
    assert main(("--source", src_dir, "--target", trgt_dir)) == 1
    assert main(("--source", src_dir, "--target", trgt_dir, "--update")) == 0


def test_main_update_runs_once(temp_dirs):
    src_dir, trgt_dir = (str(dir) for dir in temp_dirs)
    with mock.patch(
        "synapse_to_ipynb.update_synapse_nbs", mock.Mock(return_value=0)
    ) as m:
        main(("--source", src_dir, "--target", trgt_dir, "--update"))
        m.assert_called_once()


def test_main_invalid_args(tmp_path):
    src_dir = str(tmp_path / "a")
    trgt_dir = str(tmp_path / "b")
    assert main(("--source", src_dir, "--target", trgt_dir)) == 1
    assert main(("--source", src_dir, "--target", trgt_dir, "--update")) == 1


@pytest.mark.parametrize(
    "manager",
    (
        (tuple(), ("j.ipynb",)),
        (("j.json",), ("j.ipynb", "h.ipynb")),
    ),
    indirect=True,
)
def test_update_flag_error_if_new(manager: NotebookDirectoryManager, caplog):
    args = (
        "--source",
        str(manager.synapse_dir),
        "--target",
        str(manager.ipynb_dir),
        "--update",
    )
    with mock.patch("synapse_to_ipynb.NotebookDirectoryManager") as m:
        m.return_value = manager
        assert main(args) == 1
        assert (
            "The file name's in the source directory "
            "dont't match those in the target directory"
        ) in caplog.text


@pytest.mark.parametrize(
    "manager",
    (
        (("j.json",), tuple()),
        (("j.json", "h.json"), ("j.ipynb",)),
    ),
    indirect=True,
)
def test_update_flag_error_if_deleted(manager: NotebookDirectoryManager, caplog):
    args = (
        "--source",
        str(manager.synapse_dir),
        "--target",
        str(manager.ipynb_dir),
        "--update",
    )
    with mock.patch("synapse_to_ipynb.NotebookDirectoryManager") as m:
        m.return_value = manager
        assert main(args) == 1
        assert (
            "The file name's in the source directory "
            "dont't match those in the target directory"
        ) in caplog.text


@pytest.mark.parametrize(
    "manager",
    (
        (tuple(), tuple()),
        (("j.json",), ("j.ipynb",)),
    ),
    indirect=True,
)
def test_error_if_no_new_or_deleted(manager: NotebookDirectoryManager, caplog):
    args = ("--source", str(manager.synapse_dir), "--target", str(manager.ipynb_dir))
    with mock.patch("synapse_to_ipynb.NotebookDirectoryManager") as m:
        m.return_value = manager
        assert main(args) == 1
        assert "Nothing to create or delete.\n" in caplog.text


@pytest.mark.parametrize(
    "manager",
    ((("j.json", "t.json"), ("j.ipynb", "t.ipynb")),),
    indirect=True,
)
def test_manager_init(manager: NotebookDirectoryManager):
    assert len(manager.synapse_nbs) == 2
    assert len(manager.ipynbs) == 2
    assert manager.ipynb_only_nbs == []
    assert manager.synapse_only_nbs == []


def test_manager_init_error_if_duplicate_files(temp_dirs):
    src_dir, trgt_dir = temp_dirs
    (trgt_dir / "f.ipynb").touch()
    (trgt_dir / "silver").mkdir()
    (trgt_dir / "silver" / "f.ipynb").touch()
    with pytest.raises(ValueError):
        NotebookDirectoryManager(src_dir, trgt_dir)


def test_dir_args_exist(temp_dirs):
    src_dir, trgt_dir = temp_dirs
    src_dir_a, trgt_dir_a = (dir / "a" for dir in temp_dirs)
    with pytest.raises(NotADirectoryError):
        NotebookDirectoryManager(src_dir, trgt_dir_a)
    with pytest.raises(NotADirectoryError):
        NotebookDirectoryManager(src_dir_a, trgt_dir)


def test_update_synapse_notebook_from_ipynb(tmp_synb, tmp_ipynb):
    NotebookDirectoryManager.update_synapse_notebook_from_ipynb(tmp_synb, tmp_ipynb)
    with (
        open(tmp_ipynb) as ipynb,
        open(tmp_synb) as synb,
    ):
        assert json.load(ipynb)["cells"] == json.load(synb)["properties"]["cells"]


def test_create_ipynb_from_synapse_nb(
    manager: NotebookDirectoryManager, tmp_synb: Path
):
    ipynb = manager.create_ipynb_from_synapse_nb(tmp_synb)
    # Check we created a file
    assert ipynb.exists()
    # Check its name is the same as the src
    assert ipynb.stem == tmp_synb.stem
    with (
        open(ipynb) as f,
        open(tmp_synb) as f2,
    ):
        ipynb_data = json.load(f)
        synb_data = json.load(f2)
    # Check we have all the keys
    assert ipynb_data.keys() == NotebookDirectoryManager._DEFAULT_IPYNB.keys()
    # And the values for each of these is equal
    for key in ["nbformat", "nbformat_minor", "metadata"]:
        assert ipynb_data[key] == synb_data["properties"][key]
    # And for each key we had in each cell (the metadata key is conditionally added)
    for i, cell in enumerate(synb_data["properties"]["cells"]):
        for k, v in cell.items():
            assert ipynb_data["cells"][i][k] == v


def test_create_ipynb_from_synapse_nb_respects_folder_structure(
    manager: NotebookDirectoryManager, tmp_synb: Path
):
    ipynb = manager.create_ipynb_from_synapse_nb(tmp_synb)
    # Check we created a file
    assert ipynb.exists()
    with open(tmp_synb) as f:
        synb_data = json.load(f)
    assert manager.ipynb_dir / synb_data["properties"]["folder"]["name"] == ipynb.parent


def test_delete_files(tmp_path):
    files = [(tmp_path / f) for f in ["a", "b", "c", "d"]]
    for f in files:
        f.touch()
    assert delete_files(files) == 0
    for f in files:
        assert not f.exists()


@pytest.mark.parametrize(
    "manager",
    (
        (("j.json", "t.json"), ("j.ipynb", "a.ipynb")),
        (("a.json", "t.json"), ("j.ipynb", "t.ipynb")),
        (("j.json", "t.json"), ("t.ipynb", "j.ipynb")),
        (("j.json", "t.json", "a.json"), ("t.ipynb", "j.ipynb")),
    ),
    indirect=True,
)
def test_update_synapse_notebooks_error_if_not_matching(manager):
    assert update_synapse_nbs(manager) == 1
