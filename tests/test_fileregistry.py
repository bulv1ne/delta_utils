from delta_utils.fileregistry import S3FullScan


def test_initiate_fileregistry(spark):
    file_registry = S3FullScan("/mnt/husqvarna-datalake-omega-live/analytics/usr/linus.wallin/step-poc/file-registry/raw/step-product" , spark)
