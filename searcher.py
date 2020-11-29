import json

from indexer import Indexer
from parser_module import Parse
from ranker import Ranker
import utils
try:
    import _pickle as pickle
except:
    import pickle
import math
from query import query_object

class Searcher:

    def __init__(self, inverted_index, number_of_documents):
        """
        :param inverted_index: dictionary of inverted index
        """
        self.parser = Parse()
        self.ranker = Ranker()
        self.inverted_index = inverted_index
        self.current_file_name = ""
        self.current_posting = None

        self.term_posting_dict = {}
        self.sorted_query_dict = {}
        self.number_of_documents = number_of_documents
        self.docs_dict = {}

    def relevant_docs_from_posting(self, query):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        query_dict = query.query_dict
        self.sorted_query_dict = {k: query_dict[k] for k in sorted(query_dict)}
        for term in self.sorted_query_dict:
            if term in self.inverted_index:
                posting_file_to_load = self.inverted_index[term][1]
            elif term.lower() in self.inverted_index:
                posting_file_to_load = self.inverted_index[term.lower()][1]
            else:
                continue

            if posting_file_to_load != self.current_file_name:
                self.current_file_name = posting_file_to_load
                self.current_posting = self.read_posting(posting_file_to_load)

            if term in self.current_posting:
                self.term_posting_dict[term] = self.current_posting[term]

        self.document_dict_init(self.term_posting_dict, query)

        return self.docs_dict

    def read_posting(self, posting_name):
        pickle_in = open("{}".format(posting_name), "rb")
        dict_to_load = pickle.load(pickle_in)
        pickle_in.close()

        return dict_to_load

    def document_dict_init(self, postings_dict, query):

        tf_idf_list = [0] * query.query_length
        sorted_posting_dict = {k: postings_dict[k] for k in sorted(postings_dict)}

        for idx, (term, doc_list) in enumerate(sorted_posting_dict.items()):
            for doc_tuple in doc_list:
                if doc_tuple[0] not in self.docs_dict:
                    self.docs_dict[doc_tuple[0]] = tf_idf_list

                dfi = self.inverted_index[term][0]

                idf = math.log(self.number_of_documents / dfi, 10)
                tf_idf = idf * doc_tuple[2]

                self.docs_dict[doc_tuple[0]][idx] = tf_idf
                tf_idf_list = [0] * query.query_length
            print(dfi)

    def normalized_query(self, query):
        normalized = []
        max_freq_term = query.max_freq_term

        for key in self.sorted_query_dict:
            tf = self.sorted_query_dict[key]
            normalized.append(tf / max_freq_term)

        return normalized



