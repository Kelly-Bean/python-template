import os

from python_template.cfg import DATA_DIR


def test_data_dir():
    assert os.path.exists(DATA_DIR), DATA_DIR
