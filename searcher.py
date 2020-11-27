import json

from indexer import Indexer
from parser_module import Parse
from ranker import Ranker
import utils
import _pickle as pickle


class Searcher:

    def __init__(self, inverted_index):
        """
        :param inverted_index: dictionary of inverted index
        """
        self.parser = Parse()
        self.ranker = Ranker()
        self.inverted_index = inverted_index
        self.current_file_name = ""
        self.current_posting = None
        self.term_posting_dict = {}


    def relevant_docs_from_posting(self, query):
        """
        This function loads the posting list and count the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """
        sorted_query = {k: query[k] for k in sorted(query)}
        for term in sorted_query:
            posting_file_to_load = self.inverted_index[term][1]
            if posting_file_to_load != self.current_file_name:
                self.current_file_name = posting_file_to_load
                self.current_posting = self.read_posting(posting_file_to_load)

            if term in self.current_posting:
                self.term_posting_dict[term] = self.current_posting[term]

        print(self.term_posting_dict)


        posting = utils.load_obj("posting")
        relevant_docs = {}
        for term in query:
            try:  # an example of checks that you have to do
                posting_doc = posting[term]
                for doc_tuple in posting_doc:
                    doc = doc_tuple[0]
                    if doc not in relevant_docs.keys():
                        relevant_docs[doc] = 1
                    else:
                        relevant_docs[doc] += 1
            except:
                print('term {} not found in posting'.format(term))
        return relevant_docs

    def read_posting(self, posting_name):
        #TODO: MOTEK this is the loading format. i didnt test it. good luck
        pickle_in = open("{}".format(posting_name), "rb")
        dict_to_load = ""
        for j in range(Indexer.NUM_OF_TERMS_IN_POSTINGS):
                try:
                    if dict_to_load == "":
                        dict_to_load = pickle.load(pickle_in)
                    else:
                        dict_to_load += ", " + pickle.load(pickle_in)
                except:
                       break
        dict_to_load = "{" + dict_to_load + "}"
        dict_to_load = json.loads(dict_to_load)

        pickle_in.close()

        return dict_to_load
