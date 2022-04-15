from pyspark.sql import types as T

from delta_utils.clean import flatten


def test_flatten_table(spark):
    spark.conf.set("spark.sql.caseSensitive", "true")

    schema = T.StructType(
        [
            T.StructField(
                "name",
                T.StructType(
                    [
                        T.StructField("first name", T.StringType(), True),
                        T.StructField("id", T.StringType(), True),
                        T.StructField("ID", T.StringType(), True),
                        T.StructField("last,name", T.StringType(), True),
                        T.StructField("lastname.test", T.StringType(), True),
                        T.StructField(
                            "nested",
                            T.StructType(
                                [T.StructField("test sfd", T.StringType(), True)]
                            ),
                            True,
                        ),
                    ]
                ),
            ),
            T.StructField(
                "items",
                T.ArrayType(
                    T.StructType([T.StructField("swo:rd", T.BooleanType(), True)])
                ),
            ),
            T.StructField("id", T.StringType(), True),
            T.StructField("dupli,cate", T.StringType(), True),
            T.StructField("dupli;cate", T.StringType(), True),
            T.StructField("ge n(de-r", T.StringType(), True),
            T.StructField("sa;lar)y", T.IntegerType(), True),
        ]
    )

    data = [
        (
            ("Linus", "123", "456", "Wallin", "W2", ("asd",)),
            [(True,)],
            "1",
            "asd",
            "asd2",
            "Unknown",
            4,
        ),
        (
            ("Niels", "123", "768", "Lemmens", "L2", ("asd",)),
            [(True,)],
            "2",
            "asd",
            "asd2",
            "Man",
            3,
        ),
    ]

    df = spark.createDataFrame(data, schema)
    columns = flatten(df).columns
    assert columns == [
        "name_first_name",
        "name_id",
        "name_ID",
        "name_last_name",
        "name_lastname_test",
        "name_nested_test_sfd",
        "items",
        "id",
        "dupli_cate_2",
        "dupli_cate_1",
        "ge_n_de_r",
        "sa_lar_y",
    ]
