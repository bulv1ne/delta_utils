import re
from collections import Counter
from typing import Dict, List, Tuple

from pyspark.sql import types as T
from pyspark.sql.dataframe import DataFrame

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


def rename_flatten_schema(fields: List[str]) -> List[Tuple[str, str]]:
    new_fields = []

    for field in fields:
        if "." in field:
            new_col = replace_invalid_column_char(
                field.replace(".", "_").replace("`", "")
            )
            new_fields.append((field, new_col))
        else:
            new_col = replace_invalid_column_char(field.replace("`", ""))
            new_fields.append((field, new_col))

    return new_fields


def get_duplicates(fields: List[Tuple[str, str]]) -> Dict[str, int]:
    # Create a flat list of all new column names
    new_fields = [field[1] for field in fields]

    # Count the number of occurences in the list
    occurences = dict(Counter(new_fields))

    # Retrun only the once that occur more than once
    return {k: v for k, v in occurences.items() if v > 1}


def rename_duplicates(fields: List[Tuple[str, str]]):
    duplicates = get_duplicates(fields)
    new_fields = []

    for old_name, new_name in fields:
        if new_name in duplicates.keys():
            new_fields.append((old_name, f"{new_name}_{duplicates[new_name]}"))
            duplicates[new_name] = duplicates[new_name] - 1
        else:
            new_fields.append((old_name, new_name))

    return new_fields


def flatten(df: DataFrame) -> DataFrame:
    """
    Will take a nested dataframe and flatten it out.

    Args:
        df (DataFrame): The dataframe you want to flatten

    Returns:
        DataFrame: Returns a flatter dataframe

    """
    fields = rename_duplicates(rename_flatten_schema(flatten_schema(df.schema)))
    fields = [f"{k} as `{v}`" for k, v in fields]

    return df.selectExpr(*fields)
