import re
from typing import List

from pyspark.sql import SparkSession, types as T

invalid_chars = r'[\[\]\(\)\.\s"\,\;\{\}\-\ :]'


def replace_invalid_column_char(col_name: str, replacer: str = "_") -> str:
    return re.sub(invalid_chars, replacer, col_name)


def surround_field(name: str) -> str:
    if re.search(invalid_chars, name):
        return f"`{name}`"

    return name


def flatten_schema(schema: T.StructType, prefix: str = None) -> List[str]:
    fields = []

    for field in schema.fields:
        field_name = surround_field(field.name)
        name = prefix + "." + field_name if prefix else field_name
        dtype = field.dataType

        if isinstance(dtype, T.StructType):
            fields += flatten_schema(dtype, prefix=name)
        else:
            fields.append(name)

    return fields


def rename_flatten_schema(fields: List[str]) -> List[str]:
    new_fields = []

    for field in fields:
        if "." in field:
            new_col = replace_invalid_column_char(
                field.replace(".", "_").replace("`", "")
            )
            new_fields.append(f"{field} as `{new_col}`")
        else:
            new_col = replace_invalid_column_char(field.replace("`", ""))
            new_fields.append(f"{field} as `{new_col}`")

    return new_fields


def flatten(df: SparkSession) -> SparkSession:
    """
    Will take a nested dataframe and flatten it out.

    Args:
        df (SparkSession): The dataframe you want to flatten

    Returns:
        SparkSession: Returns a flatter dataframe

    """
    fields = rename_flatten_schema(flatten_schema(df.schema))  # type: ignore

    return df.selectExpr(*fields)  # type: ignore
