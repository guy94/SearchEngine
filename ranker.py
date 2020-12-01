import datetime
import time

from numpy import dot
from numpy.linalg import norm


class Ranker:

    def __init__(self):
        pass

    @staticmethod
    def rank_relevant_doc(relevant_docs, normalized_query, inverted_documents_dict):
        """
        This function provides rank for each relevant document and sorts them by their scores as a first attribute,
        and their date in minutes as second attribute.
        The current score considers tfIdf and cosine similarity.
        :param relevant_doc: dictionary of documents that contains at least one term from the query.
        :param normalized_query: query vector normalized by its max frequency term
        :param inverted_documents_dict: dictionary of documents ids and the date of creation of each document
        :return: sorted list of documents by score
        """
        ranked_docs_dict = {}

        for doc in relevant_docs:
            cos_sim = dot(relevant_docs[doc], normalized_query) / (norm(relevant_docs[doc]) * norm(normalized_query))
            ranked_docs_dict[doc] = cos_sim

        sorted_ranked_docs_dict = {k: v for k, v in
                                   sorted(ranked_docs_dict.items(), key=lambda item: (item[1],
                                                                  1 / inverted_documents_dict[item[0]][1]), reverse=True)}

        return sorted_ranked_docs_dict

    @staticmethod
    def retrieve_top_k(sorted_relevant_doc, k=1):
        """
        return a list of top K tweets based on their ranking from highest to lowest
        :param sorted_relevant_doc: list of all candidates docs.
        :param k: Number of top document to return
        :return: list of relevant document
        """

        k_docs_to_retrieve = []
        for idx, (key, value) in enumerate(sorted_relevant_doc.items()):
            if idx == k:
                break

            else:
                k_docs_to_retrieve.append((key, value))
        return k_docs_to_retrieve
