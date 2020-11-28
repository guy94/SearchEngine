from Spell_Correction import spell
import datetime

class Ranker:

    def __init__(self):
        self.docs_dict = {}


    @staticmethod
    def rank_relevant_doc(relevant_docs):
        """
        This function provides rank for each relevant document and sorts them by their scores.
        The current score considers tfIdf and cosine similarity.
        :param relevant_doc: dictionary of documents that contains at least one term from the query.
        :return: sorted list of documents by score
        """
        # for term in relevant_docs
        # return sorted(relevant_doc.items(), key=lambda item: item[1], reverse=True)

    @staticmethod
    def retrieve_top_k(sorted_relevant_doc, k=1):
        """
        return a list of top K tweets based on their ranking from highest to lowest
        :param sorted_relevant_doc: list of all candidates docs.
        :param k: Number of top document to return
        :return: list of relevant document
        """
        return sorted_relevant_doc[:k]

    def rank_of_date(self, tweet_date):

        current_time = datetime.now()

        tweet_date_as_a_DATE = datetime.strptime(tweet_date, '%a %b %d %H:%M:%S +0000 %Y')
        date_sub = current_time - tweet_date_as_a_DATE

        return date_sub


