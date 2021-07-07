import shutil
from tempfile import mkdtemp

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[*]").appName("DidomiDataTest").getOrCreate()

    return spark


@pytest.fixture(scope="function")
def tmp_dir():
    output_dir = mkdtemp()
    yield output_dir
    shutil.rmtree(output_dir)
