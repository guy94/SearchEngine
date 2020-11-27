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
    NUM_OF_TERMS_IN_POSTINGS = 100000
    FINAL_POSTINGS_COUNTER = 0
    TOTAL_TERMS_AFTER_MERGE = 0

    def __init__(self, config):

        self.not_finished_capital = {}
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


        self.counter = 0

    def add_new_doc(self, document):
        """
        This function performs indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """
        # term1 = [('1280915320774033410', 1), ('1280915357792759808', 1), ('1280915404081246215', 1), ('1280915431843340288', 1)]
        # term2 = [('1280915485517729792', 1), ('1280915531374063617', 1), ('1280915682113261568', 1)]
        # term_merge = self.merge(term1, term2)

        document_dictionary = document.term_doc_dictionary
        indices_dict = document.location_dict
        max_freq_term = document.max_freq_term
        # Go over each term in the doc
        for term in document_dictionary:
            try:
                # Update inverted index and posting
                if term not in self.inverted_idx:
                    self.inverted_idx[term] = [1, ""]
                    self.num_of_terms += 1
                else:
                    self.inverted_idx[term][0] += 1

                if term in indices_dict:
                    list_of_indices = indices_dict[term]
                else:
                    list_of_indices = []
                term_freq = document_dictionary[term]

                if term not in self.postingDict :
                    self.postingDict[term] = [(document.tweet_id)]#, document_dictionary[term], term_freq / max_freq_term,
                                               #list_of_indices)]

                else:
                    bisect.insort(self.postingDict[term],
                                  (document.tweet_id))#, document_dictionary[term], term_freq / max_freq_term,
                                    #list_of_indices))

                if len(self.postingDict) == Indexer.NUM_OF_TERMS_IN_POSTINGS:
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

        #####################
        # self.counter += len(sorted_keys_dict)
        # print("posting_{} len is: {}".format(Indexer.PICKLE_COUNTER, len(sorted_keys_dict)))

    def remove_capital_entity(self, key, merge_dict):

        if key in Parse.entity_dict_global and Parse.entity_dict_global[key] < 2:
            if key in self.inverted_idx:
                del self.inverted_idx[key]
                merge_dict[key] = []

        if key in Parse.capital_letter_dict_global and Parse.capital_letter_dict_global[key] is False:
            if key in self.inverted_idx:
                count_docs = self.inverted_idx[key]
                posting_file = merge_dict[key]
                del self.inverted_idx[key]
                merge_dict[key] = []
                if key.lower() in merge_dict:
                    self.inverted_idx[key.lower()] += count_docs
                    merge_dict[key.lower()].extend(posting_file)
                else:
                    self.not_finished_capital[key.lower] = [count_docs, posting_file]

        if key in self.not_finished_capital:
            count_docs_to_delete = self.not_finished_capital[key][0]
            posting_dict_to_delete = self.not_finished_capital[key][1]
            self.inverted_idx[key.lower()] += count_docs_to_delete
            merge_dict[key.lower()].extend(posting_dict_to_delete)


    def merge_files(self):
        self.postings_files_names = sorted([os.path.join(d, x)
                                     for d, dirs, files in os.walk("postings")
                                     for x in files])

        for i, posting in enumerate(self.postings_files_names):
            part_of_posting = self.read_part_of_posting(posting, i)
            self.files_to_merge.append(part_of_posting)

        self.k_elements_sort()
        # self.test_func()
        # print(self.counter)

        # pickle_out = open("inverted_index".format(Indexer.PICKLE_COUNTER), "wb")
        # pickle.dump(self.inverted_idx, pickle_out)
        # pickle_out.close()

        print("inverted size is {}".format(len(self.inverted_idx)))
        print("total terms merged {}".format(Indexer.TOTAL_TERMS_AFTER_MERGE))

    def test_func(self):
        # pickle_in1 = open("postings\\posting_1", "rb")
        # pickle_in2 = open("postings\\posting_2", "rb")
        # pickle_in3 = open("postings\\posting_3", "rb")
        # pickle_in4 = open("postings\\posting_4", "rb")
        #
        # lst1 = []
        # lst2 = []
        # lst3 = []
        # final_dict1 = ""
        # final_dict2 = ""
        #
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
        #
        # unique = []
        # for pair in lst1:
        #     unique.append(pair[0])
        #
        # for pair in lst2:
        #     if pair[0] not in unique:
        #         unique.append(pair[0])
        #
        # for pair in lst3:
        #     if pair[0] not in unique:
        #         unique.append(pair[0])
        # unique = sorted(unique)

        # pickle_final1 = open("finalPostings\\final_posting_1", "rb")
        # pickle_final2 = open("finalPostings\\final_posting_2", "rb")
        # pickle_final3 = open("finalPostings\\final_posting_3", "rb")
        # pickle_final4 = open("finalPostings\\final_posting_4", "rb")
        #
        # pickles = [pickle_final1, pickle_final2, pickle_final3, pickle_final4]
        #
        # dictss = []
        # final_dict1 = ""
        #
        #
        # for i in pickles:
        #     for j in range(30):
        #         try:
        #             if final_dict1 == "":
        #                 final_dict1 = pickle.load(pickle_final4)
        #             else:
        #                 final_dict1 += ", " + pickle.load(pickle_final4)
        #         except:
        #             final_dict1 = "{" + final_dict1 + "}"
        #             final_dict1 = json.loads(final_dict1)
        #             dictss.append(final_dict1)
        #             final_dict1 = ""
        #             break
        #
        #     print("keys1: {}".format(dictss[0].keys()))
        #     print("keys2: {}".format(dictss[1].keys()))
        #     print("keys3: {}".format(dictss[2].keys()))
        #     print("keys4: {}".format(dictss[3].keys()))

        print(sorted(self.inverted_idx.keys()))
        # print("final postings len is {}".format(len(final1) + len(final2) + len(final3) + len(final4)))
        # print("set size is {}".format(len(unique)))

    # d1 = [["a", [('20', 5), ('40', 8)]], ["aa", [('3', 4)]], ["aaa", [('4', 9)]]]
    # d2 = [["aa", [('4', 2)]], ["aaa", [('8', 7)]], ["bal", [('4110', 77)]]]
    # self.files_to_merge = []
    # self.files_to_merge.append(d1)
    # self.files_to_merge.append(d2)

    def read_part_of_posting(self, posting, num_of_file, last_file=False):
        """gets a posting NAME and it's index!! and reads it's content from the disk
           store the file descriptor fo current posting file"""
        num_of_file += 1  # this gives values of 1..* to file names, skipping 0
        pickle_in = open("{}".format(posting), "rb")
        if num_of_file in self.file_descriptor_dict:
            fdr = self.file_descriptor_dict[num_of_file]
            pickle_in.seek(fdr)
        part_of_posting = []

        if int(Indexer.NUM_OF_TERMS_IN_POSTINGS / Indexer.PICKLE_COUNTER) > 0:
            amount_to_read = int(Indexer.NUM_OF_TERMS_IN_POSTINGS / Indexer.PICKLE_COUNTER)
        else:
            amount_to_read = Indexer.NUM_OF_TERMS_IN_POSTINGS

        if last_file:
            amount_to_read = Indexer.NUM_OF_TERMS_IN_POSTINGS

        for i in range(amount_to_read):
            try:
                key_value = pickle.load(pickle_in)
                part_of_posting.append(key_value)
            except:
                break

        ###################################
        # self.counter -= len(part_of_posting)
        # print("{} len is: {}".format(posting, len(part_of_posting)))
        ###################################

        self.file_descriptor_dict[num_of_file] = pickle_in.tell()
        pickle_in.close()

        return part_of_posting

    def k_elements_sort(self):
        if int(Indexer.NUM_OF_TERMS_IN_POSTINGS / Indexer.PICKLE_COUNTER) > 0:
            amount_to_read = int(Indexer.NUM_OF_TERMS_IN_POSTINGS / Indexer.PICKLE_COUNTER)
        else:
            amount_to_read = Indexer.NUM_OF_TERMS_IN_POSTINGS
        index_list = [0] * Indexer.PICKLE_COUNTER
        merged_dict = {}
        posting_with_min_key = 0  # the number of the list containing the smallest key of the iteration
        idx_in_min_key_posting = 0  # the index in which the smallest key is
        file_name_index_list = [i for i in range(Indexer.PICKLE_COUNTER)]  # maintains correct file number to read from
        pivot_indices = [0] * 2  # num of list and index in that list of the current smallest element

        merge_count = 0


        if len(self.files_to_merge) == 1:
            for k in self.files_to_merge[0]:
                merged_dict[k[0]] = k[1]
            self.final_postings_dump_func(merged_dict)
            return

        while True:
            for i in range(amount_to_read):

                pivot = self.files_to_merge[0][index_list[0]]
                pivot_indices[0] = 0
                pivot_indices[1] = index_list[0]
                is_pivot = True
                is_pivot_the_smallest = True

                for j, idx in enumerate(index_list):
                    if idx == len(self.files_to_merge[j]):
                        # new read pickle!!
                        num_of_file = file_name_index_list[j]
                        posting_to_insert = self.read_part_of_posting(self.postings_files_names[num_of_file], num_of_file)

                        if not posting_to_insert:
                            self.swap_lists(index_list, file_name_index_list, merged_dict, j)

                            if len(file_name_index_list) == 1:
                                num_of_file = file_name_index_list[0]
                                last_posting_to_insert = self.read_part_of_posting(self.postings_files_names[num_of_file], num_of_file
                                                                                   , last_file=True)

                                self.files_to_merge[0].extend(last_posting_to_insert)
                                for k in range(index_list[0], len(self.files_to_merge[0])):
                                    merged_dict[self.files_to_merge[0][k][0]] = self.files_to_merge[0][k][1]
                                self.final_postings_dump_func(merged_dict)
                                return

                        else:
                            self.files_to_merge[j] = posting_to_insert
                            index_list[j] = 0
                        continue

                    current_key = self.files_to_merge[j][idx]
                    if current_key[0] == pivot[0] and not is_pivot:
                        posting_with_min_key = pivot_indices[0]
                        idx_in_min_key_posting = pivot_indices[1]

                        left = self.files_to_merge[j][idx][1]
                        right = self.files_to_merge[posting_with_min_key][idx_in_min_key_posting][1]
                        merge_func_result = self.merge(left, right)

                        merge_count += 1

                        self.files_to_merge[posting_with_min_key][idx_in_min_key_posting][1] = merge_func_result
                        index_list[j] += 1  #: progress to next index in the current posting file

                        if index_list[j] == len(self.files_to_merge[j]):
                            num_of_file = file_name_index_list[j]
                            posting_to_insert = self.read_part_of_posting(self.postings_files_names[num_of_file], num_of_file)

                            if not posting_to_insert:

                                self.swap_lists(index_list, file_name_index_list, merged_dict, j)
                                if len(file_name_index_list) == 1:
                                    num_of_file = file_name_index_list[0]
                                    last_posting_to_insert = self.read_part_of_posting(self.postings_files_names[num_of_file],
                                                                                       num_of_file, last_file=True)

                                    self.files_to_merge[0].extend(last_posting_to_insert)
                                    for k in range(index_list[0], len(self.files_to_merge[0])):
                                        merged_dict[self.files_to_merge[0][k][0]] = self.files_to_merge[0][k][1]
                                    self.final_postings_dump_func(merged_dict)
                                    return

                            else:
                                self.files_to_merge[j] = posting_to_insert
                                index_list[j] = 0
                            continue

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

                merged_dict[pivot[0]] = self.files_to_merge[posting_with_min_key][idx_in_min_key_posting][1]
                index_list[posting_with_min_key] += 1

                if index_list[0] == len(self.files_to_merge[0]):
                    # new read pickle!!
                    num_of_file = file_name_index_list[0]
                    posting_to_insert = self.read_part_of_posting(self.postings_files_names[num_of_file], num_of_file)

                    if not posting_to_insert:
                        self.swap_lists(index_list, file_name_index_list, merged_dict, 0)

                        if len(file_name_index_list) == 1:
                            num_of_file = file_name_index_list[0]
                            last_posting_to_insert = self.read_part_of_posting(self.postings_files_names[num_of_file],
                                                                               num_of_file, last_file=True)

                            self.files_to_merge[0].extend(last_posting_to_insert)
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


    def swap_lists(self, index_list, file_name_index_list, merged_dict, index):
        # if len(self.files_to_merge) == 2:
        #     print("total terms after merge: {}".format(Indexer.TOTAL_TERMS_AFTER_MERGE))
        #     print("merged dict len: {}".format(len(merged_dict)))
        file_name_index_list[index] = file_name_index_list[-1]
        file_name_index_list.pop()
        self.files_to_merge[index] = self.files_to_merge[-1]
        self.files_to_merge.pop()
        index_list[index] = index_list[-1]
        index_list.pop()


        # if len(file_name_index_list) == 1:
        #     for k in range(index_list[0], len(self.files_to_merge[0])):
        #         merged_dict[self.files_to_merge[0][k][0]] = self.files_to_merge[0][k][1]
        #     self.final_postings_dump_func(merged_dict)

    def final_postings_dump_func(self, merged_dict):

        Indexer.FINAL_POSTINGS_COUNTER += 1
        file_name = "finalPostings\\final_posting_{}".format(Indexer.FINAL_POSTINGS_COUNTER)
        pickle_out = open(file_name, "wb")

        num_of_pops = 0
        for key, value in merged_dict.items():
            # pickle.dump("\"{}\": \"{}\"".format(key, value), pickle_out)
            self.inverted_idx[key][1] = file_name
            if key in Parse.entity_dict_global or key in Parse.capital_letter_dict_global:
                self.remove_capital_entity(key, merged_dict)
                if merged_dict[key]:
                    self.inverted_idx[key][1] = file_name
                    pickle.dump("\"{}\": \"{}\"".format(key, value), pickle_out)
                else:
                    num_of_pops += 1

            else:
                self.inverted_idx[key][1] = file_name
                pickle.dump("\"{}\": \"{}\"".format(key, value), pickle_out)
        pickle_out.close()
        Indexer.TOTAL_TERMS_AFTER_MERGE += (len(merged_dict) - num_of_pops)

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

    # def test_merge(self, result):
    #     for i in range(len(result) - 1):
    #         if result[i][0] > result[i+1][0]:
    #             print(False)
    #     # print(True)
