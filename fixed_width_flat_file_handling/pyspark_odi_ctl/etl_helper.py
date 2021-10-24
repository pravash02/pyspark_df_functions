import pickle
import os


class ETLHelper:
    def __init__(self):
        pass

    @staticmethod
    def serialize_object(name, content="",
                         location="pyspark_project/fixed-width_flat_file_handling/pyspark_odi_ctl/"):
        print(f"Location: {os.getcwd()}")
        with open(os.getcwd().rstrip("/") + "/" + name + ".pickle", "wb") as file_out:
            pickle.dump(content, file_out, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def deserialize(location="pyspark_project/fixed-width_flat_file_handling/pyspark_odi_ctl/data.pickle"):
        try:
            content = None
            with open('data.pickle', "rb") as file_in:
                content = pickle.load(file_in)
            return content

        except Exception as e:
            print(e)
            raise
