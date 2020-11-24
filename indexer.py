import concurrent
import json
import os
import time

from parser_module import Parse
import _pickle as pickle
import bisect
from threading import Thread
from concurrent.futures import ThreadPoolExecutor


class Indexer:
    PICKLE_COUNTER = 1
    NUM_OF_TERMS_IN_POSTINGS = 100

    def __init__(self, config):
        self.inverted_idx = {}
        self.postingDict = {}
        self.config = config
        self.num_of_terms = 0
        self.executor = ThreadPoolExecutor(8)
        self.files_to_merge = []
        self.file_descriptor_dict = {}
        self.first_read = True
        self.postings_files_names = []

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

                    pickle_out = open("postings\\posting_{}".format(Indexer.PICKLE_COUNTER), "ab")
                    for key, value in sorted_keys_dict.items():
                        pickle.dump("\"{}\": \"{}\"".format(key, value), pickle_out)
                    pickle_out.close()
                    print("dumped {}".format(Indexer.PICKLE_COUNTER))

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
        self.postings_files_names = [os.path.join(d, x)
                                     for d, dirs, files in os.walk("postings")
                                     for x in files]

        for i, posting in enumerate(self.postings_files_names):
            part_of_posting = self.read_part_of_posting(posting, i + 1)
            self.files_to_merge.append(part_of_posting)

        all_files_merged = self.k_elements_sort()

        # has_intersection = True
        # while has_intersection:
        #     has_intersection = False
        #
        #     for i in range(len(postings) - 1):
        #         pickle_in = open("{}".format(postings[i]), "rb")
        #         dict1 = pickle.load(pickle_in)
        #         pickle_in.close()
        #
        #         pickle_in = open("{}".format(postings[i+1]), "rb")
        #         dict2 = pickle.load(pickle_in)
        #         pickle_in.close()
        #
        #         intersection_of_dicts_keys = dict1.keys() & (dict2.keys())
        #         if len(intersection_of_dicts_keys) > 0:
        #             has_intersection = True
        #
        #         merged_dict1 = {}
        #         merged_dict2 = {}
        #         for key in intersection_of_dicts_keys:
        #             left = dict1[key]
        #             right = dict2[key]
        #             future = self.executor.submit(self.merge, left, right)
        #             united_posting = future.result()
        #
        #             if(len(merged_dict1) <= Indexer.NUM_OF_TERMS_IN_POSTINGS):
        #                 merged_dict1[key] = united_posting
        #             else:
        #                 merged_dict2[key] = united_posting

    def read_part_of_posting(self, posting, num_of_file):
        """gets a posting NAME and it's index!! and reads it's content from the disk
           store the file descriptor fo current posting file"""
        pickle_in = open("{}".format(posting), "rb")
        if posting in self.file_descriptor_dict:
            pickle_in.seek(self.file_descriptor_dict[posting])
        part_of_posting = ""
        amount_to_read = int(Indexer.NUM_OF_TERMS_IN_POSTINGS / Indexer.PICKLE_COUNTER)

        for i in range(amount_to_read):
            if part_of_posting == "":
                part_of_posting = pickle.load(pickle_in)
            else:
                part_of_posting += ", " + (pickle.load(pickle_in))

        part_of_posting = "{" + part_of_posting + "}"
        part_of_posting = json.loads(part_of_posting)
        self.file_descriptor_dict[num_of_file] = pickle_in.tell()
        pickle_in.close()

        return part_of_posting

    def k_elements_sort(self):
        amount_to_read = int(Indexer.NUM_OF_TERMS_IN_POSTINGS / Indexer.PICKLE_COUNTER)
        index_list = [0] * amount_to_read
        merged_dict = {}
        posting_with_min_key = 0  #:the number of the list containing the smallest key of the iteration
        idx_in_min_key_posting = 0  #:the index in which the smallest key is
        is_start = True
        temp = ""

        while (True):  #TODO: convert to NOT_FINISHED, we need to realize what is the break condition (entire corpus done)
            if is_start:
                temp = self.files_to_merge[0][index_list[0]]
            for i in range(amount_to_read):  #: iterate all the loop!!!
                #: dont forget to check out of range in each posting, and load another posting

                for j, idx in range(len(index_list)):
                    if idx == amount_to_read:
                        #new read pickle!!
                        posting_to_insert = self.read_part_of_posting(self.postings_files_names[j], j)
                        self.files_to_merge.pop(j)
                        self.files_to_merge.insert(j, posting_to_insert)
                        index_list[j] = 0

                    current_key = self.files_to_merge[j][idx]
                    if not is_start and current_key == temp:
                        index_list[j] += 1  #: progress to next index in the current posting file
                        left = self.files_to_merge[j][idx]
                        right = self.files_to_merge[posting_with_min_key][idx_in_min_key_posting]

                        merge_func_result = self.merge(left, right)
                        self.files_to_merge[posting_with_min_key][idx_in_min_key_posting] = merge_func_result
                        del self.files_to_merge[j][idx]

                    elif current_key < temp:
                        temp = current_key
                        posting_with_min_key = j
                        idx_in_min_key_posting = idx

                        index_list[j] += 1  #: progress to next index in the current posting file
                    else:
                        index_list[j] += 1
                    is_start = False

                merged_dict[temp] = self.files_to_merge[posting_with_min_key][idx_in_min_key_posting]
                #TODO:: need to count len of merged_dict. when hits Indexer.NUM_OF_TERMS_IN_POSTINGS -->>
                #TODO:: write dict to a NEW!! posting file' with a unique name

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
