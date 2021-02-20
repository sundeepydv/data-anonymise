from faker import Faker
from pyspark.sql.functions import regexp_replace, col
from pyspark.sql.types import *

from constants import *


class Person(object):
    @staticmethod
    def generate_csv_file(n):
        """
        :param n:
        :return:
        """
        fake = Faker('en_AU')
        data = []
        for _ in range(n):
            data.append(
                {
                    "first_name": str(fake.first_name()),
                    "last_name": str(fake.last_name()),
                    "address": str(fake.address()),
                    "date_of_birth": str(fake.date_of_birth())
                }
            )

        sc = SparkContext(conf=conf)
        people_rdd = sc.parallelize(data)
        people_df = sc.read.json(people_rdd)
        people_schema = people_df \
            .withColumn("first_name", people_df["first_name"].cast(StringType())) \
            .withColumn("last_name", people_df["last_name"].cast(StringType())) \
            .withColumn('address', regexp_replace(col("address"), "\n", " ")) \
            .withColumn("date_of_birth", people_df["date_of_birth"].cast(DateType()))
        return people_schema