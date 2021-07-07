import json
import os
import tempfile

import pytest
from app.main import (compute_metrics, get_partitions, parse_input_data,
                      select_partitions_to_process, write_output)
from pyspark.sql.types import Row


def test_get_partitions(tmp_dir):
    os.mkdir(tmp_dir + "/datehour=2021-01-01-00")
    os.mkdir(tmp_dir + "/datehour=2021-01-01-01")
    os.mkdir(tmp_dir + "/datehour=2021-01-01-02")

    partitions = get_partitions("dev", tmp_dir)
    print(partitions)

    assert sorted(partitions) == sorted(
        [
            f"{tmp_dir}/datehour=2021-01-01-00/",
            f"{tmp_dir}/datehour=2021-01-01-01/",
            f"{tmp_dir}/datehour=2021-01-01-02/",
        ]
    )

    partitions = get_partitions("prd", tmp_dir)

    assert partitions == []


@pytest.mark.parametrize(
    "last_processed_partition, expected",
    [
        ("1900-01-01-00", "'2021-01-01-00','2021-01-01-01','2021-01-01-02'"),
        ("2021-01-02-00", ""),
    ],
)
def test_select_partitions_to_process(last_processed_partition, expected):
    partitions = [
        "test_path/datehour=2021-01-01-00/",
        "test_path/datehour=2021-01-01-01/",
        "test_path/datehour=2021-01-01-02/",
    ]
    filter_exp = select_partitions_to_process(partitions, last_processed_partition)

    assert filter_exp == expected


@pytest.mark.parametrize(
    "type,consent,expected",
    [
        ("pageview", "", [1, 0, 0, 0, 0]),
        ("pageview", '"analytics"', [1, 1, 0, 0, 0]),
        ("consent.asked", "", [0, 0, 1, 0, 0]),
        ("consent.given", "", [0, 0, 0, 1, 0]),
        ("consent.given", '"analytics"', [0, 0, 0, 1, 1]),
    ],
)
def test_parse_input_data(spark, tmp_dir, type, consent, expected):
    data = {
        "id": "1",
        "datetime": "2021-01-23 10:23:51",
        "domain": "www.web.com",
        "type": type,
        "user": {
            "id": "1",
            "country": "FR",
            "token": '{"purposes":{"enabled":[%s]}}' % consent,
        },
    }

    os.mkdir(tmp_dir + "/datehour=2021-01-23-10")
    tmp_file = tempfile.mktemp(dir=tmp_dir + "/datehour=2021-01-23-10", suffix=".json")
    with open(tmp_file, "w") as tmpfile:
        json.dump(data, tmpfile)

    df = parse_input_data(spark, tmp_dir, "'2021-01-23-10'")
    actual = df.select("pageviews_flag",
                       "pageviews_with_consent_flag",
                        "consents_asked_flag",
                        "consents_given_flag",
                        "consents_given_with_consent_flag"
                       ).rdd.flatMap(lambda x: x).collect()

    assert actual == expected


def test_compute_metrics(spark):

    df = spark.createDataFrame(
        [
            Row(
                datehour="2021-01-23-10",
                domain="www.web.com",
                user={"country": "GB", "id": 1},
                pageviews_flag=1,
                pageviews_with_consent_flag=1,
                consents_asked_flag=0,
                consents_given_flag=0,
                consents_given_with_consent_flag=0,
            ),
            Row(
                datehour="2021-01-23-10",
                domain="www.web.com",
                user={"country": "GB", "id": 1},
                pageviews_flag=1,
                pageviews_with_consent_flag=0,
                consents_asked_flag=0,
                consents_given_flag=0,
                consents_given_with_consent_flag=0,
            ),
        ]
    )

    primary_metrics, user_metrics = compute_metrics(df)

    actual_1 = primary_metrics.select("pageviews",
                                      "pageviews_with_consent",
                                      "consents_asked",
                                      "consents_given",
                                      "consents_given_with_consent",
                                      ).rdd.flatMap(lambda x: x).collect()

    actual_2 = user_metrics.select("avg_pageviews_per_user").rdd.flatMap(lambda x: x).collect()

    expected_1 = [2, 1, 0, 0, 0]
    expected_2 = [1.0]

    assert actual_1 == expected_1
    assert actual_2 == expected_2


def test_write_output(tmp_dir, spark):
    df = spark.createDataFrame(
        [
            Row(
                datehour="2021-01-23-10",
                domain="www.web.com",
                country="GB",
                pageviews=1,
                pageviews_with_consent=1,
                consents_asked=0,
                consents_given=0,
                consents_given_with_consent=0,
            )
        ]
    )

    write_output(df, "datehour", tmp_dir)

    expected = df.select(sorted(df.columns)).collect()

    tmp_df = spark.read.json(tmp_dir)
    actual = tmp_df.select(sorted(tmp_df.columns)).collect()

    assert actual == expected
