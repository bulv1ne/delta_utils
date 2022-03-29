from datetime import datetime, timedelta

import boto3

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

    # ACT
    file_paths = file_registry.load(
        f"s3://{mocked_s3_bucket_name}/raw/", suffix=".json"
    )

    # ASSERT
    assert file_paths == [
        "s3://mybucket/raw/file1.json",
        "s3://mybucket/raw/file2.json",
    ]


def test_update_fileregistry_all(spark, base_test_dir, mocked_s3_bucket_name):
    # ARRANGE
    file_registry = S3FullScan(f"{base_test_dir}fileregistry", spark)

    df = spark.createDataFrame(
        [
            ("s3://mybucket/raw/file1.json", None),
            ("s3://mybucket/raw/file2.json", datetime(2021, 11, 18)),
        ],
        ["file_path", "date_lifted"],
    )
    df.write.save(file_registry.file_registry_path, format="delta", mode="append")

    # ACT
    file_registry.update()

    # ASSERT
    df_res = spark.read.load(file_registry.file_registry_path).orderBy("file_path")

    result = {row["file_path"]: row["date_lifted"] for row in df_res.collect()}
    now = datetime.utcnow()
    assert (
        now - timedelta(hours=1)
        < result["s3://mybucket/raw/file1.json"]
        < now + timedelta(hours=1)
    )
    assert result["s3://mybucket/raw/file2.json"] == datetime(2021, 11, 18)


def test_update_fileregistry_single(spark, base_test_dir, mocked_s3_bucket_name):
    # ARRANGE
    file_registry = S3FullScan(f"{base_test_dir}fileregistry", spark)

    df = spark.createDataFrame(
        [
            ("s3://mybucket/raw/file1.json", None),
            ("s3://mybucket/raw/file2.json", datetime(2021, 11, 18)),
            ("s3://mybucket/raw/file3.json", None),
        ],
        ["file_path", "date_lifted"],
    )
    df.write.save(file_registry.file_registry_path, format="delta", mode="append")

    # ACT
    file_registry.update(["s3://mybucket/raw/file2.json"])

    # ASSERT
    df_res = spark.read.load(file_registry.file_registry_path).orderBy("file_path")

    result = {row["file_path"]: row["date_lifted"] for row in df_res.collect()}
    now = datetime.utcnow()
    assert result["s3://mybucket/raw/file1.json"] is None
    assert result["s3://mybucket/raw/file3.json"] is None
    assert (
        now - timedelta(hours=1)
        < result["s3://mybucket/raw/file2.json"]
        < now + timedelta(hours=1)
    )
