"""File registry that works with a prefix in S3."""
from collections import namedtuple
from dataclasses import dataclass
from typing import List

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from delta_utils.s3_path import S3Path

# pylint: disable=E1101,W0221
FileRegistryRow = namedtuple("FileRegistryRow", "file_path, date_lifted")


@dataclass
class S3FullScan:
    """File registry that works with any prefix in S3."""

    file_registry_path: str
    spark: SparkSession

    def __post_init__(self) -> None:
        self.spark.sql(
            f"""
        CREATE TABLE IF NOT EXISTS delta.`{self.file_registry_path}`
        (file_path STRING, date_lifted TIMESTAMP)
        USING delta
        """
        )

        self.schema = T.StructType(
            [
                T.StructField("file_path", T.StringType(), True),
                T.StructField("date_lifted", T.TimestampType(), True),
            ]
        )

    def update(self, paths: List[str] = None) -> None:
        """Update file registry column date_lifted to current date."""
        if paths:
            statement = F.col("file_path").isin(paths)
        else:
            statement = F.col("date_lifted").isNull()

        df = self.spark.read.load(self.file_registry_path).where(statement)
        df.createOrReplaceTempView("tmptable")

        sql_statement = [
            f"MERGE INTO delta.`{self.file_registry_path}` source",
            "USING tmptable updates",
            "ON source.file_path = updates.file_path",
            "WHEN MATCHED THEN UPDATE SET date_lifted = current_timestamp()",
        ]

        self.spark.sql(" ".join(sql_statement))
        self.spark.catalog.dropTempView("tmptable")

    def load(self, s3_path: str, suffix: str) -> List[str]:
        """Fetch new filepaths that have not been lifted from s3."""
        list_of_rows = self._get_new_s3_files(s3_path, suffix)
        self._update_file_registry(list_of_rows)
        list_of_paths = self._get_files_to_lift()

        return list_of_paths

    ###########
    # PRIVATE #
    ###########
    def _get_files_to_lift(self) -> List[str]:
        """Get a list of S3 paths from the file registry that needs to be lifted."""
        data = (
            self.spark.read.load(self.file_registry_path)
            .where(F.col("date_lifted").isNull())
            .select("file_path")
            .orderBy("file_path")
            .collect()
        )

        return [row.file_path for row in data]

    def _update_file_registry(self, list_of_rows: List[FileRegistryRow]):
        """Update the file registry and do not insert duplicates."""
        updates_df = self._rows_to_dataframe(list_of_rows)

        updates_df.createOrReplaceTempView("tmptable")
        sql_statement = [
            f"MERGE INTO delta.`{self.file_registry_path}` source",
            "USING tmptable updates",
            "ON source.file_path = updates.file_path",
            "WHEN NOT MATCHED THEN INSERT *",
        ]

        self.spark.sql(" ".join(sql_statement))
        self.spark.catalog.dropTempView("tmptable")

    @staticmethod
    def _get_new_s3_files(s3_path: str, suffix: str) -> List[FileRegistryRow]:
        """Get all files in S3 as a dataframe."""
        base_s3path = S3Path(s3_path)
        keys = base_s3path.glob(suffix)

        # Convert keys into a file registry row
        list_of_rows = [FileRegistryRow(key, None) for key in keys]

        return list_of_rows

    def _rows_to_dataframe(self, rows: List[FileRegistryRow]) -> DataFrame:
        """Create a dataframe from a list of paths with the file registry schema."""
        return self.spark.createDataFrame(rows, self.schema)
