import time

from parser_module import Parse
import _pickle as pickle
import bisect


class Indexer:
    PICKLE_COUNTER = 1
    NUM_OF_DOCS_IN_POSTINGS = 500

    def __init__(self, config):
        self.inverted_idx = {}
        self.postingDict = {}
        self.config = config
        self.num_of_docs = 0

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
                    bisect.insort(self.postingDict[term], (document.tweet_id, document_dictionary[term], term_freq / max_freq_term))
                    # self.postingDict[term].append((document.tweet_id, document_dictionary[term]))

            except:
                print('problem with the following key {}'.format(term))

        self.num_of_docs += 1

        if self.num_of_docs == Indexer.NUM_OF_DOCS_IN_POSTINGS:
            sorted_keys_dict = {k: self.postingDict[k] for k in sorted(self.postingDict)}

            pickle_out = open("postings\\posting_{}".format(Indexer.PICKLE_COUNTER), "wb")
            pickle.dump(sorted_keys_dict, pickle_out)
            pickle_out.close()

            self.num_of_docs = 0
            Indexer.PICKLE_COUNTER += 1
            self.postingDict = {}

            # pickle_in = open("postings\\posting_1", "rb")
            # example = pckl.load(pickle_in)
            # print(example)

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
