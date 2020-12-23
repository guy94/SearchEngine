import json
import os

import utils
from parser_module import Parse

try:
    import _pickle as pickle
except:
    import pickle
import bisect
from datetime import datetime
import gc


class Indexer:
    PICKLE_COUNTER = 0
    NUM_OF_TERMS_IN_POSTINGS = 200000
    FINAL_POSTINGS_COUNTER = 0
    TOTAL_TERMS_AFTER_MERGE = 0

    def __init__(self, config):

        self.pos_count = 0
        self.not_finished_capital = {}
        self.inverted_idx = {}
        self.postingDict = {}
        self.docs_dict = {}
        self.config = config
        self.num_of_terms = 0
        self.files_to_merge = []
        self.file_descriptor_dict = {}
        self.first_read = True
        self.postings_files_names = []
        self.is_last_doc = False
        self.values_size = 0
        self.spell_dict = {}
        self.one_time_appearance_list = []

        ######
        self.counter = 0

        ######

        if config.toStem:
            self.dump_path = config.saveFilesWithStem
        else:
            self.dump_path = config.saveFilesWithoutStem

    def add_new_doc(self, document):
        """
        This function performs indexing process for a document object.
        Saved information is captures via two dictionaries ('inverted index' and 'posting')
        :param document: a document need to be indexed.
        :return: -
        """

        document_dictionary = document.term_doc_dictionary
        indices_dict = document.location_dict
        max_freq_term = document.max_freq_term
        # Go over each term in the doc
        for term in document_dictionary:

            try:
                # Update inverted index and posting
                if term not in self.inverted_idx:
                    self.inverted_idx[term] = [1, "", 0]
                    self.spell_dict[term] = 1
                    self.num_of_terms += 1

                else:
                    self.inverted_idx[term][0] += 1
                    self.spell_dict[term] += 1

                if term in indices_dict:
                    list_of_indices = indices_dict[term]
                else:
                    list_of_indices = []
                term_freq = document_dictionary[term]

                if term not in self.postingDict:
                    self.postingDict[term] = [(int(document.tweet_id), document_dictionary[term], term_freq / max_freq_term)]
                    # list_of_indices)]
                    self.values_size += 1

                # else:
                #     bisect.insort(self.postingDict[term],
                #                   (document.tweet_id, document_dictionary[term], term_freq / max_freq_term,
                #                    list_of_indices))

                else:
                    self.postingDict[term].extend(
                        [(int(document.tweet_id), document_dictionary[term], term_freq / max_freq_term)])
                    # list_of_indices)])
                    self.values_size += 1

                if document.tweet_id not in self.docs_dict:
                    tweet_date = document.tweet_date
                    date_in_hours = self.date_diff(tweet_date)

                    self.docs_dict[document.tweet_id] = [document.doc_length, date_in_hours, max_freq_term]

                if len(self.postingDict) == Indexer.NUM_OF_TERMS_IN_POSTINGS:
                    # print(self.values_size)
                    self.dump_from_indexer_to_disk()

                    self.values_size = 0
                    self.num_of_terms = 0
                    self.postingDict = {}

            except:
                print("problem with term: {}".format(term))

        if self.is_last_doc:
            if len(self.postingDict) > 0:
                self.dump_from_indexer_to_disk()

                self.num_of_terms = 0
                self.postingDict = {}
                self.values_size = 0

    def dump_from_indexer_to_disk(self):
        sorted_keys_dict = {k: self.postingDict[k] for k in sorted(self.postingDict)}
        Indexer.PICKLE_COUNTER += 1

        file_name = self.dump_path + "\\posting_{}".format(Indexer.PICKLE_COUNTER)
        pickle_out = open(file_name, "ab")
        for key, value in sorted_keys_dict.items():
            pickle.dump([key, value], pickle_out)
        pickle_out.close()

        self.postings_files_names.append(file_name)

        #####################
        # self.counter += len(sorted_keys_dict)
        # print("posting_{} len is: {}".format(Indexer.PICKLE_COUNTER, len(sorted_keys_dict)))

    def date_diff(self, tweet_date):

        current_time = datetime.now()

        tweet_date_as_a_DATE = datetime.strptime(tweet_date, '%a %b %d %H:%M:%S +0000 %Y')
        date_sub = current_time - tweet_date_as_a_DATE

        date_in_minutes = int((date_sub.days * 60 * 24) + (date_sub.seconds // 3600))
        return date_in_minutes

    def remove_capital_entity(self, key, merge_dict):

        if key in Parse.ENTITY_DICT and Parse.ENTITY_DICT[key] < 2:
            if key in self.inverted_idx:
                del self.inverted_idx[key]
                del merge_dict[key]

        elif key in Parse.CAPITAL_LETTER_DICT and Parse.CAPITAL_LETTER_DICT[key] is False:
            if key in self.inverted_idx:
                count_docs = self.inverted_idx[key]
                posting_file = merge_dict[key]
                del self.inverted_idx[key]
                del merge_dict[key]

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
        # self.postings_files_names = sorted([os.path.join(d, x)  #
        #                              for d, dirs, files in os.walk("posting\\WithoutStem")
        #                              for x in files if "final" not in x])

        for i, posting in enumerate(self.postings_files_names):
            part_of_posting = self.read_part_of_posting(posting, i, False, True)
            self.files_to_merge.append(part_of_posting)

        self.docs_dict = {}
        self.spell_dict = {}
        self.postingDict = {}

        self.k_elements_sort()

        self.remove_redundant_terms()

        print("inverted size is {}".format(len(self.inverted_idx)))
        print("total terms merged {}".format(Indexer.TOTAL_TERMS_AFTER_MERGE))

    # def test_func(self):
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

    # print(sorted(self.inverted_idx.keys()))
    # print("final postings len is {}".format(len(final1) + len(final2) + len(final3) + len(final4)))
    # print("set size is {}".format(len(unique)))

    def read_part_of_posting(self, posting, num_of_file, last_file=False, first_read=False):
        """gets a posting NAME and it's index!! and reads it's content from the disk
           store the file descriptor of current posting file"""
        num_of_file += 1  # this gives values of 1..* to file names, skipping 0

        with open(posting, 'rb') as pickle_in:
            # pickle_in = open("{}".format(posting), "rb")
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

            # amount_to_read = 2325
            if first_read:
                for i in range(amount_to_read):
                    try:
                        key_value = utils.load_list(pickle_in)
                        # key_value = pickle.load(pickle_in)
                        part_of_posting.append(key_value)
                    except:
                        break
            else:
                for i in range(amount_to_read):
                    try:
                        key_value = utils.load_list(pickle_in)
                        # key_value = pickle.load(pickle_in)
                        part_of_posting.append(key_value)
                        self.values_size += len(key_value[1])

                        if self.values_size >= 2000000:
                            self.values_size = 0
                            break
                    except:
                        break

            self.file_descriptor_dict[num_of_file] = pickle_in.tell()
        # pickle_in.close()

        return part_of_posting

    def k_elements_sort(self):
        if int(Indexer.NUM_OF_TERMS_IN_POSTINGS / Indexer.PICKLE_COUNTER) > 0:
            amount_to_read = int(Indexer.NUM_OF_TERMS_IN_POSTINGS / Indexer.PICKLE_COUNTER)
        else:
            amount_to_read = Indexer.NUM_OF_TERMS_IN_POSTINGS
        index_list = [0] * Indexer.PICKLE_COUNTER

        # amount_to_read = 2325
        # index_list = [0] * 49

        merged_dict = {}
        posting_with_min_key = 0  # the number of the list containing the smallest key of the iteration
        idx_in_min_key_posting = 0  # the index in which the smallest key is
        file_name_index_list = [i for i in range(Indexer.PICKLE_COUNTER)]  # maintains correct file number to read from
        pivot_indices = [0] * 2  # num of list and index in that list of the current smallest element
        values_size = 0  # counts the length of posting lists
        # merge_count = 0

        if len(self.files_to_merge) == 1:
            for k in self.files_to_merge[0]:
                if len(k[1]) > 1:
                    merged_dict[k[0]] = k[1]
                else:
                    self.one_time_appearance_list.append(k[0])
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
                        posting_to_insert = self.read_part_of_posting(self.postings_files_names[num_of_file],
                                                                      num_of_file)

                        if not posting_to_insert:
                            self.swap_lists(index_list, file_name_index_list, j)

                            if len(file_name_index_list) == 1:
                                num_of_file = file_name_index_list[0]
                                last_posting_to_insert = self.read_part_of_posting(
                                    self.postings_files_names[num_of_file], num_of_file
                                    , last_file=True)

                                self.files_to_merge[0].extend(last_posting_to_insert)
                                for k in range(index_list[0], len(self.files_to_merge[0])):
                                    # if len(self.files_to_merge[0][k][1]) > 1:
                                    merged_dict[self.files_to_merge[0][k][0]] = self.files_to_merge[0][k][1]
                                    values_size += len(self.files_to_merge[0][k][1])  ##############################
                                    # else:
                                    #     self.one_time_appearance_list.append(self.files_to_merge[0][k][0])

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

                        # merge_func_result = self.merge(left, right)
                        left.extend(right)
                        merge_func_result = left

                        # merge_count += 1

                        self.files_to_merge[posting_with_min_key][idx_in_min_key_posting][1] = merge_func_result
                        index_list[j] += 1  #: progress to next index in the current posting file

                        if index_list[j] == len(self.files_to_merge[j]):
                            num_of_file = file_name_index_list[j]
                            posting_to_insert = self.read_part_of_posting(self.postings_files_names[num_of_file],
                                                                          num_of_file)
                            if not posting_to_insert:

                                self.swap_lists(index_list, file_name_index_list, j)
                                if len(file_name_index_list) == 1:
                                    num_of_file = file_name_index_list[0]
                                    last_posting_to_insert = self.read_part_of_posting(
                                        self.postings_files_names[num_of_file],
                                        num_of_file, last_file=True)

                                    self.files_to_merge[0].extend(last_posting_to_insert)
                                    for k in range(index_list[0], len(self.files_to_merge[0])):
                                        # if len(self.files_to_merge[0][k][1]) > 1:
                                        merged_dict[self.files_to_merge[0][k][0]] = self.files_to_merge[0][k][1]
                                        values_size += len(self.files_to_merge[0][k][1])  #########################
                                        # else:
                                        #     self.one_time_appearance_list.append(self.files_to_merge[0][k][0])

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

                add_last = True
                if self.files_to_merge[posting_with_min_key][idx_in_min_key_posting][0] in merged_dict and add_last:
                    add_last = False
                    merged_dict[self.files_to_merge[posting_with_min_key][idx_in_min_key_posting][0]]\
                        .extend(self.files_to_merge[posting_with_min_key][idx_in_min_key_posting][1])

                if add_last:
                    if len(self.files_to_merge[posting_with_min_key][idx_in_min_key_posting][1]) > 1:
                        merged_dict[self.files_to_merge[posting_with_min_key][idx_in_min_key_posting][0]] = \
                            self.files_to_merge[posting_with_min_key][idx_in_min_key_posting][1]
                        values_size += len(self.files_to_merge[posting_with_min_key][idx_in_min_key_posting][1])
                    else:
                        self.one_time_appearance_list.append(self.files_to_merge[posting_with_min_key][idx_in_min_key_posting][0])

                index_list[posting_with_min_key] += 1

                if index_list[0] == len(self.files_to_merge[0]):
                    # new read pickle!!
                    num_of_file = file_name_index_list[0]
                    posting_to_insert = self.read_part_of_posting(self.postings_files_names[num_of_file], num_of_file)

                    if not posting_to_insert:
                        self.swap_lists(index_list, file_name_index_list, 0)

                        if len(file_name_index_list) == 1:
                            num_of_file = file_name_index_list[0]
                            last_posting_to_insert = self.read_part_of_posting(self.postings_files_names[num_of_file],
                                                                               num_of_file, last_file=True)

                            self.files_to_merge[0].extend(last_posting_to_insert)
                            for k in range(index_list[0], len(self.files_to_merge[0])):
                                # if len(self.files_to_merge[0][k][1]) > 1:
                                merged_dict[self.files_to_merge[0][k][0]] = self.files_to_merge[0][k][1]
                                values_size += len(self.files_to_merge[0][k][1])
                                # else:
                                #     self.one_time_appearance_list.append(self.files_to_merge[0][k][0])

                            self.final_postings_dump_func(merged_dict)
                            return

                    else:
                        self.files_to_merge[0] = posting_to_insert
                        index_list[0] = 0
                    continue

                # if len(merged_dict) == Indexer.NUM_OF_TERMS_IN_POSTINGS:  # limits the legnth of the total posting lists in each final file
                if values_size >= 2000000:
                    self.final_postings_dump_func(merged_dict)
                    merged_dict = {}
                    values_size = 0

    def swap_lists(self, index_list, file_name_index_list, index):
        file_name_index_list[index] = file_name_index_list[-1]
        file_name_index_list.pop()
        self.files_to_merge[index] = self.files_to_merge[-1]
        self.files_to_merge.pop()
        index_list[index] = index_list[-1]
        index_list.pop()

    def final_postings_dump_func(self, merged_dict):

        Indexer.FINAL_POSTINGS_COUNTER += 1
        file_name = Indexer.FINAL_POSTINGS_COUNTER
        pickle_out = open(self.dump_path + "\\" + str(file_name), "wb")

        for key in list(merged_dict):
            self.inverted_idx[key][1] = file_name
            self.inverted_idx[key][2] += len(merged_dict[key])
            if key in Parse.ENTITY_DICT or key in Parse.CAPITAL_LETTER_DICT:
                self.remove_capital_entity(key, merged_dict)

        pickle.dump(merged_dict, pickle_out)
        pickle_out.close()
        Indexer.TOTAL_TERMS_AFTER_MERGE += (len(merged_dict))

    def merge(self, left, right):
        """Merge sort merging function."""
        ## TODO: if we use that function, consider convert tweetid to int
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

    def remove_redundant_terms(self):
        with open("spell_dict.json", "r") as json_in:
            spell_dict = json.load(json_in)

        for term in self.one_time_appearance_list:
            if term in self.inverted_idx:
                del self.inverted_idx[term]

            if term in spell_dict:
                del spell_dict[term]
        with open('spell_dict.json', 'w') as f:
            json.dump(spell_dict, f)
