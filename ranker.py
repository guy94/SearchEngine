# from Spell_Correction import spell
import datetime
from numpy import dot
from numpy.linalg import norm

class Ranker:

    def __init__(self):
        pass
    @staticmethod
    def rank_relevant_doc(relevant_docs, normalized_query):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers tfIdf and cosine similarity.
        :param relevant_doc: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        ranked_docs_dict = {}

        for doc in relevant_docs:
            cos_sim = dot(relevant_docs[doc], normalized_query) / (norm(relevant_docs[doc])*norm(normalized_query))
            ranked_docs_dict[doc] = cos_sim

        # sorted_ranked_docs_dict = {k: ranked_docs_dict[k] for k in sorted(ranked_docs_dict)}
        sorted_ranked_docs_dict = {k: v for k, v in sorted(ranked_docs_dict.items(), key=lambda item: item[1], reverse=True)}

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

    def rank_of_date(self, tweet_date):

        current_time = datetime.now()

        tweet_date_as_a_DATE = datetime.strptime(tweet_date, '%a %b %d %H:%M:%S +0000 %Y')
        date_sub = current_time - tweet_date_as_a_DATE

        return date_sub


