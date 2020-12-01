import json
import time
from indexer import Indexer
from parser_module import Parse
from ranker import Ranker
from spellchecker import SpellChecker
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
        :param number_of_documents: number of documents in the corpus
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
        self.spell = SpellChecker(local_dictionary='spell_dict.json', distance=1)

    def relevant_docs_from_posting(self, query):
        """
        This function loads the posting list and counts the amount of relevant documents per term.
        :param query: query
        :return: dictionary of relevant documents.
        """

        query_dict = query.query_dict
        query_dict = self.spell_correction(query_dict)
        for term in query_dict:
            if term in self.inverted_index:
                continue

            elif term.isupper() and term not in self.inverted_index:
                if term.lower() in self.inverted_index:
                    query_dict[term.lower()] = query_dict.pop(term)

            elif term.islower() and term not in self.inverted_index:
                if term.upper() in self.inverted_index:
                    query_dict[term.upper()] = query_dict.pop(term)

        self.sorted_query_dict = {k: query_dict[k] for k in sorted(query_dict)}
        for term in self.sorted_query_dict:
            if term in self.inverted_index:
                posting_file_to_load = self.inverted_index[term][1]

            else:
                continue

            if posting_file_to_load != self.current_file_name:
                self.current_file_name = posting_file_to_load
                self.current_posting = self.read_posting(posting_file_to_load)

            if term in self.current_posting:
                self.term_posting_dict[term] = self.current_posting[term]

        self.document_dict_init(self.term_posting_dict, query.query_length)

        return self.docs_dict

    def spell_correction(self, query_dict):
        """
        This function finds a misspelled word and finds its closest similarity.
        first by tracking all of its candidates. the candidate with the most appearances in the inverted index
        will be the "replacer"
        :param query: query dictionary
        :return: query dictionary with replaced correct words.
        """

        for term in query_dict:

            if term.lower() not in self.inverted_index and term.upper() not in self.inverted_index:

                misspelled_checker = self.spell.unknown([term])

                if len(misspelled_checker) != 0:
                    candidates = list(self.spell.edit_distance_1(term))

                    super_candidates = list(self.spell.candidates(term))
                    candidates.extend(super_candidates)


                    max_freq_in_corpus = 0
                    max_freq_name = ''

                    for i, candidate in enumerate(candidates):
                        if candidate in self.inverted_index:
                            curr_freq = self.inverted_index[candidate][0]
                            if curr_freq > max_freq_in_corpus:
                                max_freq_in_corpus = curr_freq
                                max_freq_name = candidate

                        elif candidate.upper() in self.inverted_index:
                            curr_freq = self.inverted_index[candidate.upper()][0]
                            if curr_freq > max_freq_in_corpus:
                                max_freq_in_corpus = curr_freq
                                max_freq_name = candidate

                    if max_freq_name != '':
                        query_dict[max_freq_name] = query_dict.pop(term)
                    else:
                        continue

        return query_dict

    def read_posting(self, posting_name):
        """
        This function seeks for the file name and reads it from the disk.
        :param posting_name: file name
        :return: posting file
        """
        pickle_in = open("{}".format(posting_name), "rb")
        dict_to_load = pickle.load(pickle_in)
        pickle_in.close()

        return dict_to_load

    def document_dict_init(self, postings_dict, query_length):
        """
       This function initiates the sorted dictionary that will contain each term of the query and its
       corresponding posting list
       :param postings_dict: a dictionary of term (key) and a posting list (value)
       :param query_length: query length
       :return:
       """

        tf_idf_list = [0] * query_length
        sorted_posting_dict = {k: postings_dict[k] for k in sorted(postings_dict)}

        for idx, (term, doc_list) in enumerate(sorted_posting_dict.items()):
            for doc_tuple in doc_list:
                if doc_tuple[0] not in self.docs_dict:
                    self.docs_dict[doc_tuple[0]] = tf_idf_list

                try:
                    dfi = self.inverted_index[term][2]
                except:
                    dfi = self.inverted_index[term.lower()][2]

                idf = math.log(self.number_of_documents / dfi, 10)
                tf_idf = idf * doc_tuple[2]

                self.docs_dict[doc_tuple[0]][idx] = tf_idf
                tf_idf_list = [0] * query_length

    def normalized_query(self, query):
        """
       This function normalizes each term in the auery by the max term freq in the SORTED query dict.
       :param query: a query object
       :return: normalized query values
       """

        normalized = []
        max_freq_term = query.max_freq_term

        for key in self.sorted_query_dict:
            tf = self.sorted_query_dict[key]
            normalized.append(tf / max_freq_term)

        return normalized



