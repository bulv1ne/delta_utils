"""File registry that works with a prefix in S3."""
from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime
from typing import List, Union

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T
from tqdm import tqdm

from delta_utils.delta_table import DeltaTable
from delta_utils.errors import handle_delta_files_dont_exist
from delta_utils.logger import get_logger
from delta_utils.s3_path import S3Path

# pylint: disable=E1101,W0221
LOGGER = get_logger(__name__)
FileRegistryRow = namedtuple("FileRegistryRow", "file_path, date_lifted")


@dataclass
class S3FullScan:
    """File registry that works with any prefix in S3."""

    file_registry_path: str
    spark: SparkSession

    def __post_init__(self) -> None:
        self.schema = T.StructType(
            [
                T.StructField("file_path", T.StringType(), True),
                T.StructField("date_lifted", T.TimestampType(), True),
            ]
        )
        self._get_or_create()

    def update(self, paths: List[str] = None) -> None:
        """Update file registry column date_lifted to current date."""
        if paths:
            statement = F.col("file_path").isin(paths)
        else:
            statement = F.col("date_lifted").isNull()

        self.delta_table.delta_table.update(
            statement, {"date_lifted": F.lit(datetime.now())}
        )

    def load(self, s3_path: str, suffix: str) -> List[str]:
        """Fetch new filepaths that have not been lifted from s3."""
        list_of_rows = self._get_new_s3_files(s3_path, suffix)
        updated_dataframe = self._update_file_registry(list_of_rows)
        list_of_paths = self._get_files_to_lift(updated_dataframe)

        LOGGER.info("Found %s new keys in s3", len(list_of_paths))

        return list_of_paths

    ###########
    # PRIVATE #
    ###########
    def _get_or_create(self):
        """Get or create a delta table instance for a file registry."""
        dataframe = self.fetch_file_registry(self.file_registry_path, self.spark)

        # If file registry is found
        if not dataframe:
            LOGGER.info(f"No registry found create one at {self.file_registry_path}")
            self._create_file_registry()
        else:
            LOGGER.info(f"File registry found at {self.file_registry_path}")

        self.delta_table = DeltaTable(self.file_registry_path, self.spark)

    @staticmethod
    def _get_files_to_lift(dataframe: DataFrame) -> List[str]:
        """Get a list of S3 paths from the file registry that needs to be lifted."""
        data = (
            dataframe.where(F.col("date_lifted").isNull()).select("file_path").collect()
        )

        return [row.file_path for row in data]

    def _update_file_registry(self, list_of_rows: List[FileRegistryRow]) -> DataFrame:
        """Update the file registry and do not insert duplicates."""
        updates_df = self._rows_to_dataframe(list_of_rows)

        # Update the file registry
        return self.delta_table.insert_all(
            updates_df, "source.file_path = updates.file_path"
        )

    @staticmethod
    def _get_new_s3_files(s3_path: str, suffix: str) -> List[FileRegistryRow]:
        """Get all files in S3 as a dataframe."""
        base_s3path = S3Path(s3_path)
        keys = tqdm(base_s3path.glob(suffix))

        # Convert keys into a file registry row
        list_of_rows = [FileRegistryRow(key, None) for key in keys]

        # Keys found under the s3_path
        LOGGER.info("Search %s for files. Found: %s files", s3_path, len(list_of_rows))

        return list_of_rows

    def fetch_file_registry(
        self, path: str, spark: SparkSession
    ) -> Union[DataFrame, None]:
        """Return a dataframe if one can be found otherwise None."""
        with handle_delta_files_dont_exist():
            dataframe = spark.read.load(path, format="delta")
            if not dataframe.rdd.isEmpty():
                return dataframe

        return None

    def _create_file_registry(self):
        """When there is now existing file registry create one."""
        dataframe = self.spark.createDataFrame([], self.schema)

        breakpoint()
        dataframe.write.save(
            path=self.file_registry_path, format="delta", mode="overwrite"
        )

    def _rows_to_dataframe(self, rows: List[FileRegistryRow]) -> DataFrame:
        """Create a dataframe from a list of paths with the file registry schema."""
        return self.spark.createDataFrame(rows, self.schema)
