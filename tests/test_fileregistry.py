from datetime import datetime

import boto3
from pyspark.sql import functions as F

from delta_utils.fileregistry import S3FullScan


def test_initiate_fileregistry(spark, base_test_dir):
    file_registry = S3FullScan(f"{base_test_dir}fileregistry", spark)
    df = spark.read.load(file_registry.file_registry_path)

    assert df.columns == ["file_path", "date_lifted"]


def test_load_fileregistry(spark, base_test_dir, mocked_s3_bucket_name):
    # ARRANGE
    file_registry = S3FullScan(f"{base_test_dir}fileregistry", spark)

    # create files
    s3 = boto3.client("s3")
    s3.put_object(Bucket=mocked_s3_bucket_name, Key="raw/file1.json", Body=b"test")
    s3.put_object(Bucket=mocked_s3_bucket_name, Key="raw/file2.json", Body=b"test")
    s3.put_object(Bucket=mocked_s3_bucket_name, Key="raw/file3.xml", Body=b"test")

    # act
    file_paths = file_registry.load(
        f"s3://{mocked_s3_bucket_name}/raw/", suffix=".json"
    )

    # ASSERT
    assert len(file_paths) == 2
    assert set(file_paths) == {
        "s3://mybucket/raw/file1.json",
        "s3://mybucket/raw/file2.json",
    }


def test_update_fileregistry_all(spark, base_test_dir, mocked_s3_bucket_name):
    # ARRANGE
    file_registry = S3FullScan(f"{base_test_dir}fileregistry", spark)

    df = spark.createDataFrame(
        [
            ("s3://mybucket/raw/file1.json", None),
            ("s3://mybucket/raw/file1.json", datetime(2021, 11, 18)),
        ],
        ["file_path", "date_lifted"],
    )
    df.write.save(file_registry.file_registry_path, format="delta", mode="append")

    # ACT
    file_registry.update()

    # ASSERT
    df_res = spark.read.load(file_registry.file_registry_path)
    assert df_res.where(F.col("date_lifted").isNotNull()).count() == 2
