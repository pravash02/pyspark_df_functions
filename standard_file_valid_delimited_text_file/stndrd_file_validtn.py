# spark-submit standard_file_valid_delimited_text_file/stndrd_file_validtn.py

from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
from standard_file_valid_delimited_text_file.file_mthd import FileMethods


class SVLDJobDelimited(FileMethods):
    def __init__(self):
        pass

    def extract_data(self, spark):
        self.df_file_cntnt = super().extract_source_file_data(spark)

    def transform_data(self, spark):
        super().read_header(spark, self.df_file_cntnt, header_filter="value like 'H%'",
                            input_file_dml='standard_file_valid_delimited_text_file/input_source_file_schema.json')

        # super().read_trailer(spark, self.df_file_cntnt, trailer_filter="value like 'T%'",
        #                      input_file_dml='standard_file_valid_delimited_text_file/input_source_file_schema.json')
        #
        # # reading data record
        # super().read_data(spark, self.df_file_cntnt, data_filter="value like 'D%'",
        #                   input_file_dml='standard_file_valid_delimited_text_file/input_source_file_schema.json')


def main():
    job = SVLDJobDelimited()
    job.spark = SparkSession.builder \
        .master("local") \
        .appName("TEST_DF") \
        .config("spark.some.config.option", "some-value").getOrCreate()

    job.extract_data(job.spark)
    job.transform_data(job.spark)


if __name__ == '__main__':
    main()
