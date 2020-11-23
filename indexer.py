import concurrent
import os
import time

from parser_module import Parse
import _pickle as pickle
import bisect
from threading import Thread
from concurrent.futures import ThreadPoolExecutor


class Indexer:
    PICKLE_COUNTER = 1
    NUM_OF_TERMS_IN_POSTINGS = 30000

    def __init__(self, config):
        self.inverted_idx = {}
        self.postingDict = {}
        self.config = config
        self.num_of_terms = 0
        self.executor = ThreadPoolExecutor(8)

    def add_new_doc(self, document):
        """
        This function perform indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """
        # term1 = [('1280915320774033410', 1), ('1280915357792759808', 1), ('1280915404081246215', 1), ('1280915431843340288', 1)]
        # term2 = [('1280915485517729792', 1), ('1280915531374063617', 1), ('1280915682113261568', 1)]
        # term_merge = self.merge(term1, term2)

        document_dictionary = document.term_doc_dictionary
        max_freq_term = document.max_freq_term
        # Go over each term in the doc

        for term in document_dictionary.keys():
            try:
                # Update inverted index and posting
                if term not in self.inverted_idx:
                    self.inverted_idx[term] = 1
                else:
                    self.inverted_idx[term] += 1

                term_freq = document_dictionary[term]
                if term not in self.postingDict:
                    self.postingDict[term] = [(document.tweet_id, document_dictionary[term], term_freq / max_freq_term)]
                else:
                    bisect.insort(self.postingDict[term],
                                  (document.tweet_id, document_dictionary[term], term_freq / max_freq_term))
                    # self.postingDict[term].append((document.tweet_id, document_dictionary[term]))

                self.num_of_terms += 1

                if self.num_of_terms == Indexer.NUM_OF_TERMS_IN_POSTINGS:
                    sorted_keys_dict = {k: self.postingDict[k] for k in sorted(self.postingDict)}

                    pickle_out = open("postings\\posting_{}".format(Indexer.PICKLE_COUNTER), "wb")
                    pickle.dump(sorted_keys_dict, pickle_out)
                    pickle_out.close()

                    self.num_of_terms = 0
                    Indexer.PICKLE_COUNTER += 1
                    self.postingDict = {}

            except:
                print('problem with the following key {}'.format(term))

    def remove_capital_entity(self):

        entity_dict_keys = Parse.entity_dict_global.keys()
        for key in entity_dict_keys:
            if Parse.entity_dict_global[key] < 2:
                del self.inverted_idx[key]
                del self.postingDict[key]

        capital_dict_keys = Parse.capital_letter_dict_global.keys()
        for key in capital_dict_keys:
            if Parse.capital_letter_dict_global[key] is False:
                if key in self.inverted_idx:
                    count_docs = self.inverted_idx[key]
                    posting_file = self.postingDict[key]
                    del self.inverted_idx[key]
                    del self.postingDict[key]
                    self.inverted_idx[key.lower()] += count_docs
                    self.postingDict[key.lower()].extend(posting_file)

    def merge_files(self):
        postings = [os.path.join(d, x)
                       for d, dirs, files in os.walk("postings")
                       for x in files]

        while len(postings) != len(self.inverted_idx):
            for i in range(len(postings) - 1):
                pickle_in = open("{}".format(postings[i]), "rb")
                dict1 = pickle.load(pickle_in)
                pickle_in.close()

                pickle_in = open("{}".format(postings[i+1]), "rb")
                dict2 = pickle.load(pickle_in)
                pickle_in.close()

                intersection_of_dicts_keys = dict1.keys() & (dict2.keys())

                merged_dict1 = {}
                merged_dict2 = {}
                for key in intersection_of_dicts_keys:
                    left = dict1[key]
                    right = dict2[key]
                    future = self.executor.submit(self.merge, left, right)
                    united_posting = future.result()

                    if(len(merged_dict1) <= Indexer.NUM_OF_TERMS_IN_POSTINGS):
                        merged_dict1[key] = united_posting
                    else:
                        merged_dict2[key] = united_posting





    def merge(self, left, right):
        """Merge sort merging function."""
        left_index, right_index = 0, 0
        result = []
        while left_index < len(left) and right_index < len(right):
            if left[left_index][0] < right[right_index][0]:
                result.append(left[left_index])
                left_index += 1
            else:
                result.append(right[right_index])
                right_index += 1

        result += left[left_index:]
        result += right[right_index:]
        return result
