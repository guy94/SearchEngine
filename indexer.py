import concurrent
import json
import os
import time
from parser_module import Parse
import _pickle as pickle
import bisect
from concurrent.futures import ThreadPoolExecutor


class Indexer:
    PICKLE_COUNTER = 0
    NUM_OF_TERMS_IN_POSTINGS = 100
    FINAL_POSTINGS_COUNTER = 0
    TOTAL_TERMS_AFTER_MERGE = 0

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
        self.is_last_doc = False

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
        num_of_terms_last = 0
        for term in document_dictionary:
            num_of_terms_last += 1
            try:
                # Update inverted index and posting
                if term not in self.inverted_idx:
                    self.inverted_idx[term] = [1, ""]
                    self.num_of_terms += 1
                else:
                    self.inverted_idx[term][0] += 1

                term_freq = document_dictionary[term]
                if term not in self.postingDict:
                    self.postingDict[term] = [(document.tweet_id, document_dictionary[term], term_freq / max_freq_term)]
                else:
                    bisect.insort(self.postingDict[term],
                                  (document.tweet_id, document_dictionary[term], term_freq / max_freq_term))
                    # self.postingDict[term].append((document.tweet_id, document_dictionary[term]))

                if self.num_of_terms == Indexer.NUM_OF_TERMS_IN_POSTINGS:
                    self.dump_from_indexer_to_disk()

                    self.num_of_terms = 0
                    self.postingDict = {}


            except:
                print('problem with the following key {}'.format(term))

        if self.is_last_doc:
            if len(self.postingDict) > 0:
                self.dump_from_indexer_to_disk()

                self.num_of_terms = 0
                self.postingDict = {}

    def dump_from_indexer_to_disk(self):
        sorted_keys_dict = {k: self.postingDict[k] for k in sorted(self.postingDict)}
        Indexer.PICKLE_COUNTER += 1

        pickle_out = open("postings\\posting_{}".format(Indexer.PICKLE_COUNTER), "ab")
        for key, value in sorted_keys_dict.items():
            pickle.dump([key, value], pickle_out)
        pickle_out.close()

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

        self.k_elements_sort()

        print("inverted size is {}".format(len(self.inverted_idx)))
        print("total terms merged {}".format(Indexer.TOTAL_TERMS_AFTER_MERGE))
        # self.test_func()

    # def test_func(self):
        # pickle_in1 = open("postings\\posting_1", "rb")
        # pickle_in2 = open("postings\\posting_2", "rb")
        # pickle_in3 = open("postings\\posting_3", "rb")

        # lst1 = []
        # lst2 = []
        # lst3 = []
        # final_dict1 = ""
        # final_dict2 = ""

        # pickle_final1 = open("finalPostings\\final_posting_1", "rb")
        # while True:
        #     try:
        #         if final_dict1 == "":
        #             final_dict1 = pickle.load(pickle_final1)
        #         else:
        #             final_dict1 += ", " + pickle.load(pickle_final1)
        #     except:
        #         break
        # final_dict1 = "{" + final_dict1 + "}"
        # final_dict1 = json.loads(final_dict1)
        #
        # pickle_final2 = open("finalPostings\\final_posting_2", "rb")
        # while True:
        #     try:
        #         if final_dict2 == "":
        #             final_dict2 = pickle.load(pickle_final2)
        #         else:
        #             final_dict2 += ", " + pickle.load(pickle_final2)
        #     except:
        #         break
        # final_dict2 = "{" + final_dict2 + "}"
        # final_dict2 = json.loads(final_dict2)
        #
        # while True:
        #     try:
        #         key_value = pickle.load(pickle_in1)
        #         lst1.append(key_value)
        #     except:
        #         break
        #
        # while True:
        #     try:
        #         key_value = pickle.load(pickle_in2)
        #         lst2.append(key_value)
        #     except:
        #         break
        #
        # while True:
        #     try:
        #         key_value = pickle.load(pickle_in3)
        #         lst3.append(key_value)
        #     except:
        #         break

        # unique = []
        # for pair in lst1:
        #     unique.append(pair[0])
        #
        # for pair in lst2:
        #     if pair[0] not in unique:
        #         unique.append(pair[0])

        # for pair in lst3:
        #     if pair[0] not in unique:
        #         unique.append(pair[0])
        # unique = sorted(unique)
        # print(sorted(self.inverted_idx.keys()))
        # print(final_dict1.keys())
        # print("final postings len is {}".format(len(final_dict1) + len(final_dict2)))
        # print("set size is {}".format(len(unique)))

        # d1 = [["a", [('20', 5), ('40', 8)]], ["aa", [('3', 4)]], ["aaa", [('4', 9)]]]
        # d2 = [["aa", [('4', 2)]], ["aaa", [('8', 7)]], ["bal", [('4110', 77)]]]
        # self.files_to_merge = []
        # self.files_to_merge.append(d1)
        # self.files_to_merge.append(d2)

    def read_part_of_posting(self, posting, num_of_file):
        """gets a posting NAME and it's index!! and reads it's content from the disk
           store the file descriptor fo current posting file"""
        num_of_file += 1  # this gives values of 1..* to file names, skipping 0
        pickle_in = open("{}".format(posting), "rb")
        if num_of_file in self.file_descriptor_dict:
            fdr = self.file_descriptor_dict[num_of_file]
            pickle_in.seek(fdr)
        part_of_posting = []
        amount_to_read = int(Indexer.NUM_OF_TERMS_IN_POSTINGS / Indexer.PICKLE_COUNTER)

        for i in range(amount_to_read):
            try:
                key_value = pickle.load(pickle_in)
                part_of_posting.append(key_value)
            except:
                break

        self.file_descriptor_dict[num_of_file] = pickle_in.tell()
        pickle_in.close()

        return part_of_posting

    def k_elements_sort(self):
        amount_to_read = int(Indexer.NUM_OF_TERMS_IN_POSTINGS / Indexer.PICKLE_COUNTER)
        index_list = [0] * Indexer.PICKLE_COUNTER
        merged_dict = {}
        posting_with_min_key = 0  # the number of the list containing the smallest key of the iteration
        idx_in_min_key_posting = 0  # the index in which the smallest key is
        file_name_index_list = [i for i in range(Indexer.PICKLE_COUNTER)]  # maintains correct file number to read from
        pivot_indices = [0]*2 #  num of list and index in that list of the current smallest element

        if len(self.files_to_merge) == 1:
            for k in self.files_to_merge[0]:
                merged_dict[k[0]] = k[1]
            self.final_postings_dump_func(merged_dict)
            return

        while True:
            for i in range(amount_to_read):
                if index_list[0] == len(self.files_to_merge[0]):
                    print()
                pivot = self.files_to_merge[0][index_list[0]]
                pivot_indices[0] = 0
                pivot_indices[1] = index_list[0]
                is_pivot = True
                is_pivot_the_smallest = True

                for j, idx in enumerate(index_list):
                    if idx == len(self.files_to_merge[j]):
                        #new read pickle!!
                        num_of_file = file_name_index_list[j]
                        posting_to_insert = self.read_part_of_posting(self.postings_files_names[j], num_of_file)
                        if not posting_to_insert:
                            file_name_index_list[j] = file_name_index_list[-1]
                            file_name_index_list.pop()
                            self.files_to_merge[j] = self.files_to_merge[-1]
                            self.files_to_merge.pop()
                            index_list[j] = index_list[-1]
                            index_list.pop()

                            if len(file_name_index_list) == 1:
                                for k in range(index_list[0], len(self.files_to_merge[0])):
                                    merged_dict[self.files_to_merge[0][k][0]] = self.files_to_merge[0][k][1]
                                self.final_postings_dump_func(merged_dict)
                                return
                        else:
                            self.files_to_merge[j] = posting_to_insert
                            index_list[j] = 0
                        continue

                    current_key = self.files_to_merge[j][idx]
                    if pivot[0] == "bind" or current_key == "bind":
                        print()
                    if current_key[0] == pivot[0] and not is_pivot:
                        posting_with_min_key = pivot_indices[0]
                        idx_in_min_key_posting = pivot_indices[1]

                        left = self.files_to_merge[j][idx][1]
                        right = self.files_to_merge[posting_with_min_key][idx_in_min_key_posting][1]
                        merge_func_result = self.merge(left, right)

                        self.files_to_merge[posting_with_min_key][idx_in_min_key_posting][1] = merge_func_result
                        index_list[j] += 1  #: progress to next index in the current posting file

                    elif current_key[0] < pivot[0]:
                        is_pivot_the_smallest = False
                        pivot = current_key
                        pivot_indices[0] = j
                        pivot_indices[1] = idx
                        posting_with_min_key = j
                        idx_in_min_key_posting = idx

                    is_pivot = False

                if is_pivot_the_smallest:
                    posting_with_min_key = 0
                    idx_in_min_key_posting = index_list[0]
                    # index_list[0] += 1

                merged_dict[pivot[0]] = self.files_to_merge[posting_with_min_key][idx_in_min_key_posting][1]
                index_list[posting_with_min_key] += 1

                if index_list[0] == len(self.files_to_merge[0]):
                    #new read pickle!!
                    num_of_file = file_name_index_list[0]
                    posting_to_insert = self.read_part_of_posting(self.postings_files_names[0], num_of_file)

                    if not posting_to_insert:
                        file_name_index_list[0] = file_name_index_list[-1]
                        file_name_index_list.pop()
                        self.files_to_merge[0] = self.files_to_merge[-1]
                        self.files_to_merge.pop()
                        index_list[0] = index_list[-1]
                        index_list.pop()

                        if len(file_name_index_list) == 1:
                            for k in range(index_list[0], len(self.files_to_merge[0])):
                                merged_dict[self.files_to_merge[0][k][0]] = self.files_to_merge[0][k][1]
                            self.final_postings_dump_func(merged_dict)
                            return
                    else:
                        self.files_to_merge[0] = posting_to_insert
                        index_list[0] = 0
                    continue

                if len(merged_dict) == Indexer.NUM_OF_TERMS_IN_POSTINGS:
                    self.final_postings_dump_func(merged_dict)
                    merged_dict = {}

    def final_postings_dump_func(self, merged_dict):

        Indexer.FINAL_POSTINGS_COUNTER += 1
        file_name = "finalPostings\\final_posting_{}".format(Indexer.FINAL_POSTINGS_COUNTER)
        pickle_out = open(file_name, "wb")
        for key, value in merged_dict.items():
            pickle.dump("\"{}\": \"{}\"".format(key, value), pickle_out)
            self.inverted_idx[key][1] = file_name
        pickle_out.close()

        Indexer.TOTAL_TERMS_AFTER_MERGE += len(merged_dict)

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

        self.test_merge(result)

        return result

    def test_merge(self, result):
        for i in range(len(result) - 1):
            if result[i][0] > result[i+1][0]:
                print(False)
        # print(True)
