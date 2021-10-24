from pyspark_project.fixed_width_flat_file_handling.pyspark_odi_ctl.etl_helper import *
from pyspark_project.fixed_width_flat_file_handling.pyspark_odi_ctl.parser_utils import *


def serialize_format():
    """
    This function reads the command line arguments, loads the control file
    parser, extracts the file format and serializes the object into the
    file in the specified location.
    """
    ctl_file_path = "data.ctl"
    target_path = "pyspark_project/fixed_width_flat_file_handling/pyspark_odi_ctl"
    parser = CTLParser(ctl_file_path, "", target_path, "")

    print("Number of lines: {} ".format(len(parser.line_list)))

    ETLHelper.serialize_object(name='data', content=parser)

    for table in parser.tables:
        table_info = parser.tables[table]
        condition_str = table_info.load_condition
        print("Load condition - ", condition_str.to_string())
        print("Table: {}, LoadCondition: {}, {}, {}".format(table_info.name,
                                                            condition_str.start_index,
                                                            condition_str.end_index,
                                                            condition_str.value))
        for info in table_info.fields.values():
            print(info.to_string())


if __name__ == "__main__":
    serialize_format()
    parser = ETLHelper.deserialize()
