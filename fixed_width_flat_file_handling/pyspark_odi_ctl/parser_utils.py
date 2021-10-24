import csv
import os


class TableInfo:
    """
    This class provides functionality for parsing configuration information
    into table schema.
    """

    def __init__(self, name):
        """
        Constructor for instantiating TableInfo class
        """

        print("TableInfo Constructor")
        self.name = name
        self.load_condition = None
        self.row_num = 1
        self.record_len = -1
        self.fields = {}
        self.records = []

    def set_load_condition(self, condition):
        """
        Setting Parsing condition for loading table rows
        """
        self.load_condition = condition

    def to_string(self):
        print("Name: {}, LoadCondition: {}".format(self.name, self.load_condition.to_string()))

    def add_field(self, field_info):
        self.fields[field_info.name] = field_info
        return

    @property
    def crc(self):
        """
        :return: Return True/False based on the name
        """
        if self.name.lower() == "prkey_rec_control_t" and len(self.records) == 1:
            return True
        return False

    def process_data_row(self, row):
        """
        This method takes the row as parameter and checks if condition is
        valid on the raw data then processes field values and dd record with
        the field values.
        """
        # Check if condition is valid on the raw data
        valid = self.load_condition.is_valid_row(row)
        if not valid:
            return valid
        if self.record_len == -1:
            self.record_len = len(row)

        record = {
            "row_num": self.row_num
        }
        self.row_num = self.row_num + 1
        # Process field values
        for field in self.fields:
            # Add record with the field values
            record[field] = self.fields[field].get_value(row)
            if field == 'FILE_CREATED_DATE':
                record["DATE_FORMAT"] = self.fields["FILE_CREATED_DATE"].format_string

        self.records.append(record)
        return valid

    # def serialize_to_s3(self, workflow, feed_file_path, target_path, file_type):
    #     parts = feed_file_path.split("/")
    #     key = f"{file_type}/feed={parts[len(parts) - 1]}/workflow={workflow}/{self.name}.csv"
    #
    #     if self.name.lower().startswith("prkey_rec_control"):
    #         key = f"CONTROL_RECORD/feed={parts[len(parts) - 1]}/workflow={workflow}/{self.name}.csv"
    #     self.write_data_to_s3(target_path, key)
    #
    # def write_data_to_s3(self, target_path, key):
    #     output = io.StringIO()
    #     headers = self.records[0].keys()
    #     writer = csv.DictWriter(output, fieldnames=headers)
    #     writer.writeheader()
    #     for record in self.records:
    #         writer.writerow(record)
    #
    #     uri = urlparse(target_path, allow_fragments=False)
    #     object_key = f"{uri.path.lstrip('/')}/{key}"
    #     S3Utility.write_data(output.getvalue(), uri.netloc, object_key)

    def serialize_to_disk(self, workflow, feed_file_path, target_path, file_type):

        parts = feed_file_path.split("/")
        feed_tokens = parts[len(parts) - 1].split(".")
        key = f"{file_type}/{feed_tokens[0]}/{self.name}.csv"
        if self.name.lower().startswith("prkey_rec_control"):
            key = f"{file_type}/{feed_tokens[0]}/{self.name}.csv"
        self.write_csv_data_to_disk(target_path, key)

    def serialize_to_disk_ex(self, workflow, feed_file_path, target_path, file_type):

        parts = feed_file_path.split("/")
        key = self.name + '.csv'
        if self.name.lower().startswith("prkey_rec_control"):
            key = file_type + '_' + self.name + '.csv'
        self.write_csv_data_to_disk(target_path, key)

    def write_csv_data_to_disk(self, target_path, key):
        full_path = f"{target_path}/{key}"
        directory = os.path.dirname(full_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
        headers = self.records[0].keys()
        with open(full_path, 'w', newline='\n', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for record in self.records:
                writer.writerow(record)

    def write_data_to_disk(self, target_path, key):
        with open(target_path + '/' + key, 'w') as f:
            w = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
            w.writerow(self.records[0].keys())
            for record in self.records:
                w.writerow(record.values())


class LoadCondition:
    """
    This class represents the Parsing condition state and behavior.
    This condition determines the compatibility of data row with the table it
    is going to be associated.
    """

    def __init__(self, start, end, val=None, inverse=False):
        """
        Constructor for load condition
        """
        self.start_index = start
        self.end_index = end
        self.value = val
        self.not_condition = inverse

    def to_string(self):
        print("StartIndex: {}, EndIndex: {}, Value: {}".format(self.start_index, self.end_index, self.value))

    def get_value(self, row, trim_value=True):
        """
        This method returns the data with trimmed spaces as per the trim value
        condition.
        """
        data = row[(self.start_index - 1):self.end_index]
        if trim_value:
            # DEFECT ID : 176
            # Code fixed to remove hidden characters(\x00) from all source attributes
            return data.strip('\x00').rstrip()
        else:
            # DEFECT ID : 64
            # Code fixed to remove end of line character from DESC2 columns
            return data.strip('\n').rstrip()

    def is_valid_row(self, row):
        """
        This method checks if condition is valid on the row data and returns
        true/false value matches the same.
        """
        if self.value[0] == '-1' and len(row) > 0:
            # Skip the condition and load the row data
            return True

        # Check if condition is valid on the raw data
        data = self.get_value(row)

        if self.not_condition:
            return data not in self.value

        return data in self.value and len(row) > 0


class FieldInfo:
    """
    This class provides individual column information
    """

    def __init__(self, name, field_type="", data_type="CHAR"):
        self.name = name
        self.field_type = field_type
        self.data_type = data_type

    def to_string(self):
        print("{}: [{} - {}]".format(self.name, self.field_type, self.data_type))

    def get_value(self, raw_data):
        return


class ConstantFieldInfo(FieldInfo):
    """
    This class inherits FieldInfo and provides Constant field information
    """
    value = None

    def __init__(self, name, val, data_type="CHAR"):
        FieldInfo.__init__(self, name, "CONSTANT")
        self.value = val

    def to_string(self):
        print("{}: [{} - {}]".format(self.name, self.field_type, self.value))

    def get_value(self, raw_data):
        return self.value


class PositionFieldInfo(FieldInfo):
    """
    This class inherits FieldInfo and provides Position field information
    """
    value = None

    def __init__(self, name, condition, data_type="CHAR", format_string=None):
        FieldInfo.__init__(self, name, "POSITION")
        self.load_condition = condition
        self.format_string = format_string

    def to_string(self):
        print("{}: [{} - {}]".format(self.name, self.field_type, self.data_type))

    def get_value(self, raw_data):
        trim_value = True
        if self.name in ("DESC1", "DESC2", "DESC2_PART1", "DESC2_PART2", "DESC2_PART3", "DESC2_PART4"):
            trim_value = False

        self.value = self.load_condition.get_value(raw_data, trim_value)
        return self.value


class CTLParser:
    """
    CTLParser class is used to read the control file for the specific feeder
    file format and store the byte code in the serialized format.
    """

    options_loaded = False
    is_loading_table_info = False
    current_table_name = None
    is_next_line_for_table_load_condition = False
    loading_fields_started = False
    target_path = None
    options = {}
    tables = {}

    def __init__(self, ctl_file, feed_file, target_path, workflow):
        """
        This initializes line_list, options, tables, target_path, process
        """
        content = None
        # Check if this is S3 path
        if ctl_file.lower().startswith("s3://"):
            # content = CTLParser.get_s3_object(ctl_file)
            pass
        else:
            content = open(ctl_file)

        self.line_list = [line.rstrip('\n').strip() for line in content]
        self.options = {}
        self.tables = {}

        content = None
        self.target_path = target_path

        self.process()

    # @staticmethod
    # def get_s3_object(self, file_path):
    #     """
    #     This method is used to read the S3 object
    #     :param self:
    #     :param file_path:
    #     :return: It returns S3 object
    #     """
    #     o = urlparse(file_path, allow_fragments=False)
    #     logging.info(f"Bucket: {o.netloc}, Key: {o.path}")
    #     result = S3Utility.read_object(o.netloc, o.path.lstrip("/"), read_lines=True)
    #     return result

    def process(self):
        """
        This method at first loads Condition then checks is this line for Table
        Condition or starts Fields Load then checks if fields are loaded then
        checks for Load options and then creates Table info.
        """
        for line in self.line_list:

            # Skip if null or empty
            if len(line) == 0 or line.isspace() or line.strip().startswith("--"):
                continue

            # Load Condition
            if line.strip().startswith("WHEN"):
                self.is_next_line_for_table_load_condition = True
                continue

            # Is this line for Table Condition
            if self.is_next_line_for_table_load_condition:
                self.read_table_load_condition(line)
                self.is_next_line_for_table_load_condition = False
                continue

            # Start Fields Load
            if line.startswith("FIELDS"):
                continue

            # Start Fields Load
            if line.startswith("("):
                self.loading_fields_started = True
                continue

            # End Fields Load
            if line.startswith(")"):
                self.loading_fields_started = False
                self.current_table_name = None
                continue

            # Check if fields are loaded
            if self.loading_fields_started:
                constant_index = line.lower().find("constant")
                position_index = line.lower().find("position")

                if constant_index > 0:
                    self.read_constant_field_info(line)
                elif position_index > 0:
                    self.read_position_field_info(line)
                else:
                    self.read_sql_field_info(line)

                continue

            # Load options
            if line.startswith("OPTIONS"):
                self.check_for_options(line)
                continue

            # Create TableInfo
            if line.strip().startswith("APPEND INTO TABLE ") or line.strip().startswith("INTO TABLE"):
                self.create_table_info(line)
                continue

    def process_row_data(self, data):
        """
        This method takes data as parameter then calls the  process_row_data
        method for further processing.
        """
        valid = False
        for table in self.tables.values():
            valid = table.process_data_row(data)
            if valid:
                break

        return

    def read_constant_field_info(self, line):
        """
        This method takes line as input parameter and reads constant field
        information.
        """
        parts = line.strip().split(" ")
        name = parts[0].strip().strip('"')
        field_type = parts[1].strip().upper()
        value = parts[2].strip(",").strip().strip('"')
        const_field = ConstantFieldInfo(name, value)
        self.tables[self.current_table_name].add_field(const_field)

    def read_position_field_info(self, line):
        """
        This method takes line as input parameter and reads position filed
        information.
        """
        parts = line.strip().split()
        # Remove FILLER token from the field definition
        if "FILLER" in parts:
            parts.remove("FILLER")
        if "BOUNDFILLER" in parts:
            parts.remove("BOUNDFILLER")

        name = parts[0].strip().strip('"')
        field_type = parts[1].strip().upper()
        position_value = parts[2].strip().strip("(").strip(")").strip().split(":")
        condition = LoadCondition(int(position_value[0]), int(position_value[1]))

        data_type = parts[3].strip()
        format_string = None
        if len(parts) > 4:
            format_string = parts[4].strip().strip('"')

        pos_field = PositionFieldInfo(name, condition, data_type, format_string)
        self.tables[self.current_table_name].add_field(pos_field)

    def read_sql_field_info(self, line):
        return

    def read_table_load_condition(self, line):
        """
        This method takes line as input parameter and reads table load condition.
        """
        self.is_next_line_for_table_load_condition = False
        not_condition = False
        index_parts = line.strip().split("=")
        if line.find("!=") > 0:
            index_parts = line.strip().split("!=")
            not_condition = True

        value = index_parts[1].strip().strip("'").strip("'").split(',')
        position_value = index_parts[0].strip().strip("(").strip(")").strip().split(":")
        condition = LoadCondition(int(position_value[0]), int(position_value[1]), value, not_condition)
        self.tables[self.current_table_name].set_load_condition(condition)

    def check_for_options(self, line):
        """
        This method takes line as input parameter and checks for OPTIONS.
        """
        params = line.strip("OPTIONS").strip().strip("(").strip(")").strip().split(",")
        for param in params:
            key_val = param.strip().strip(",").split("=")
            self.options[key_val[0]] = key_val[1]

        self.options_loaded = True
        return

    def create_table_info(self, line):
        """
        This method takes line as input parameter and creates table information.
        """
        name = line.strip("APPEND").strip().strip("INTO TABLE ").strip().strip('"')
        # Setting up a new table
        self.current_table_name = name
        self.tables[name] = TableInfo(name)

    # def get_crc_record(self):
    #     """
    #     This method is used to get the control record check data.
    #     :return:
    #     """
    #     for table in self.tables.values():
    #         if table.crc:
    #             return table.records[0]
    #
    #     return None
    #
    # def get_txn_records(self, table_name=None):
    #     """
    #     This method is used to get the transaction/detailed records..
    #     :return:
    #     """
    #
    #     for table in self.tables.values():
    #         if not table.crc and table_name is None:
    #             return table.records
    #         elif not table.crc and table_name:
    #             if table.name == table_name:
    #                 return table.records
