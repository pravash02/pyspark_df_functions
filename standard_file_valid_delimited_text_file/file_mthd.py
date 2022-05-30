from functools import reduce
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.window import Window
from pyspark.sql.functions import *
import json

class FileMethods:
    def __init__(self):
        pass

    def read_file(self, schema_filename):   # aws_functions.py
        with open(schema_filename, encoding='utf-8') as filename:
            data = filename.read()
        return data

    def get_schema(self, schema_filename):  # class_etl_ethods.py
        schema_file = self.read_file(schema_filename)
        schema_dict = json.loads(schema_file)
        return json.dumps(schema_dict)

    def extract_source_file_data(self, spark, object_path='standard_file_valid_delimited_text_file/source_file.txt',
                                 data_has_newline='N'):
        if data_has_newline == 'Y':
            df = spark.read.text(object_path)
        else:
            df = spark.read.text(object_path).repartition(50)

        return df

    def apply_schema_to_delimited_data(self, spark, df_record, record_identifier, nums_char_skip=None,
                                       input_file_dml=None):
        if input_file_dml is None:
            dict_dml = json.loads(self.get_schema('standard_file_valid_delimited_text_file/input_source_file_schema.json'))
        else:
            dict_dml = json.loads(self.get_schema(input_file_dml))

        df_record.createOrReplaceTempView('temp')
        # special char replace
        # var_delimiter = str(dict_dml[record_identifier][0]['delimiter'])
        # df_record = spark.sql("select regexp_replace(value, '[^\x20-\x7E]," + var_delimiter + "]', ' ') value from temp")
        # df_record = df_record.withColumn("value",
        #                                  expr("substring(value, cast(" + str(int(nums_char_skip) + 1) + " as int), length(value))"))
        df_record.createOrReplaceTempView('record')

        var_rec_column_index = 0
        record_select_expr = ""

        for row in dict_dml[record_identifier]:
            print('row - ', row)
            substr_with_comma = "trim(split(value, '\\\\" + row['delimiter'] + "')[" + str(var_rec_column_index) + "])"
            # handle string types
            record_select_expr = record_select_expr + " cast(" + substr_with_comma + " as " + row['type'] + ") as " + row['name'] + ","
            var_rec_column_index += 1

        print(record_select_expr)
        df_new_rec = spark.sql("select " + record_select_expr[:-1] + " from record")
        df_new_rec.show()

        return df_new_rec

    def read_header(self, spark, df, header_filter=None, input_file_dml=None):
        nums_char_skip = 1
        df_header = df.filter(header_filter)
        df_header_schema = self.apply_schema_to_delimited_data(spark, df_header, 'H', nums_char_skip, input_file_dml)

    def read_trailer(self, spark, df, trailer_filter=None, input_file_dml=None):
        nums_char_skip = 1
        df_trailer = df.filter(trailer_filter)
        df_trailer_schema = self.apply_schema_to_delimited_data(spark, df_trailer, 'T', nums_char_skip, input_file_dml)

    def read_data(self, spark, df, data_filter=None, input_file_dml=None):
        nums_char_skip = 1
        df_data = df.filter(data_filter)
        df_data_schema = self.apply_schema_to_delimited_data(spark, df_data, 'D', nums_char_skip, input_file_dml)

