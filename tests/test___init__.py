from pathlib import Path
import shutil

import pytest

from overpack import Vpk


vpk_files = list((Path(__file__).parent / "vpk_examples").glob("*.vpk"))
vpk_file_names = [p.name for p in vpk_files]


@pytest.fixture(params=vpk_files, ids=vpk_file_names)
def vpk(request):
    yield request.param


@pytest.fixture(params=vpk_files, ids=vpk_file_names)
def unzipped_vpk(request, tmp_path):
    vpk_path = request.param
    unzipped_vpk_path = tmp_path / vpk_path.stem
    shutil.unpack_archive(vpk_path, unzipped_vpk_path, "zip")
    yield unzipped_vpk_path
    shutil.rmtree(unzipped_vpk_path)


def test_Vpk_load_from_package(vpk):
    Vpk.load(vpk)
    assert True


def test_Vpk_load_from_directory(unzipped_vpk):
    Vpk.load(unzipped_vpk)
    assert True
