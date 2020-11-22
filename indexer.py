import time

from parser_module import Parse
import _pickle as pickle
import bisect

class Indexer:
    pickle_counter = 1

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

        document_dictionary = document.term_doc_dictionary
        # Go over each term in the doc
        for term in document_dictionary.keys():
            try:
                # Update inverted index and posting
                if term not in self.inverted_idx:
                    self.inverted_idx[term] = 1
                else:
                    self.inverted_idx[term] += 1

                if term not in self.postingDict:
                    self.postingDict[term] = [(document.tweet_id, document_dictionary[term])]
                else:
                    bisect.insort(self.postingDict[term], (document.tweet_id, document_dictionary[term]))
                    # self.postingDict[term].append((document.tweet_id, document_dictionary[term]))

                self.num_of_docs += 1

                if self.num_of_docs == 500:
                    sorted_keys_dict = {k: self.postingDict[k] for k in sorted(self.postingDict)}

                    pickle_out = open("postings\\posting_{}".format(Indexer.pickle_counter), "wb")
                    pickle.dump(sorted_keys_dict, pickle_out)
                    pickle_out.close()

                    self.num_of_docs = 0
                    Indexer.pickle_counter += 1
                    self.postingDict = {}

                    # pickle_in = open("postings\\posting_1", "rb")
                    # example = pckl.load(pickle_in)
                    # print(example)

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
                if key in self.inverted_idx.keys():
                    count_docs = self.inverted_idx[key]
                    posting_file = self.postingDict[key]
                    del self.inverted_idx[key]
                    del self.postingDict[key]
                    self.inverted_idx[key.lower()] += count_docs
                    self.postingDict[key.lower()].extend(posting_file)


