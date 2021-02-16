import os
import pandas as pd


class ReadFile:
    def __init__(self, corpus_path):
        self.corpus_path = corpus_path

    def read_file(self, file_name, read_corpus=False):
        """
        This function is reading a parquet file contains several tweets
        The file location is given as a string as an input to this function.
        :param read_corpus:
        :param file_name: string - indicates the path to the file we wish to read.
        :return: a dataframe contains tweets.
        """
        if not read_corpus:
            full_path = os.path.join(self.corpus_path, file_name)
        else:
            full_path = file_name

        df = pd.read_parquet(full_path, engine="pyarrow")
        return df.values.tolist()

    def read_corpus(self):
        corpus_list = [os.path.join(d, x)
                       for d, dirs, files in os.walk(self.corpus_path)
                       for x in files if x.endswith(".parquet")]

        return corpus_list
