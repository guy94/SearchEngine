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
    PICKLE_COUNTER = 0
    NUM_OF_TERMS_IN_POSTINGS = 100
    FINAL_POSTINGS_COUNTER = 0

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
                    self.inverted_idx[term] = [1, ""]
                else:
                    self.inverted_idx[term][0] += 1

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
                    Indexer.PICKLE_COUNTER += 1

                    pickle_out = open("postings\\posting_{}".format(Indexer.PICKLE_COUNTER), "ab")
                    for key, value in sorted_keys_dict.items():
                        # pickle.dump("\"{}\": \"{}\"".format(key, value), pickle_out)
                        pickle.dump([key, value], pickle_out)
                    pickle_out.close()

                    self.num_of_terms = 0
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
            part_of_posting = self.read_part_of_posting(posting, i)
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
        num_of_file += 1
        pickle_in = open("{}".format(posting), "rb")
        if num_of_file in self.file_descriptor_dict:
            fdr = self.file_descriptor_dict[num_of_file]
            pickle_in.seek(fdr)
        part_of_posting = []
        amount_to_read = int(Indexer.NUM_OF_TERMS_IN_POSTINGS / Indexer.PICKLE_COUNTER)

        for i in range(amount_to_read):
            key_value = pickle.load(pickle_in)
            part_of_posting.append(key_value)

        if num_of_file == 3:
            print()

        self.file_descriptor_dict[num_of_file] = pickle_in.seek(pickle_in.tell())
        pickle_in.close()

        return part_of_posting

    def k_elements_sort(self):
        amount_to_read = int(Indexer.NUM_OF_TERMS_IN_POSTINGS / Indexer.PICKLE_COUNTER)
        index_list = [0] * Indexer.PICKLE_COUNTER
        merged_dict = {}
        posting_with_min_key = 0  #:the number of the list containing the smallest key of the iteration
        idx_in_min_key_posting = 0  #:the index in which the smallest key is
        pivot = []

        while (True):  #TODO: convert to NOT_FINISHED, we need to realize what is the break condition (entire corpus done)
            for i in range(amount_to_read):  #: iterate all the loop!!!
                pivot = self.files_to_merge[0][index_list[0]]
                is_pivot = True
                is_pivot_the_smallest = True

                for j, idx in enumerate(index_list):
                    if idx == 32:
                        print()
                    if idx == amount_to_read:
                        #new read pickle!!
                        posting_to_insert = self.read_part_of_posting(self.postings_files_names[j], j)
                        #TODO: idx is not updated
                        self.files_to_merge[j] = posting_to_insert
                        index_list[j] = 0
                        continue

                    current_key = self.files_to_merge[j][idx]
                    if current_key[0] == pivot[0] and not is_pivot:
                        left = self.files_to_merge[j][idx][1]
                        right = self.files_to_merge[posting_with_min_key][idx_in_min_key_posting][1]

                        merge_func_result = self.merge(left, right)
                        self.files_to_merge[posting_with_min_key][idx_in_min_key_posting][1] = merge_func_result
                        index_list[j] += 1  #: progress to next index in the current posting file

                    elif current_key[0] < pivot[0]:
                        is_pivot_the_smallest = False
                        pivot = current_key
                        posting_with_min_key = j
                        idx_in_min_key_posting = idx
                        index_list[j] += 1  #: progress to next index in the current posting file

                    is_pivot = False

                if is_pivot_the_smallest:
                    posting_with_min_key = 0
                    idx_in_min_key_posting = index_list[0]
                    index_list[0] += 1

                # print(index_list)
                merged_dict[pivot[0]] = self.files_to_merge[posting_with_min_key][idx_in_min_key_posting][1]

                #TODO:: need to count len of merged_dict. when hits Indexer.NUM_OF_TERMS_IN_POSTINGS -->>
                #TODO:: write dict to a NEW!! posting file' with a unique name

                if len(merged_dict) == Indexer.NUM_OF_TERMS_IN_POSTINGS:
                    Indexer.FINAL_POSTINGS_COUNTER += 1
                    file_name = "postings\\final_posting_{}".format(Indexer.FINAL_POSTINGS_COUNTER)
                    pickle_out = open(file_name, "wb")
                    for key, value in merged_dict.items():
                        pickle.dump("\"{}\": \"{}\"".format(key, value), pickle_out)
                        self.inverted_idx[key][1] = file_name
                    pickle_out.close()

                    print("dumped {}".format(Indexer.FINAL_POSTINGS_COUNTER))


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
