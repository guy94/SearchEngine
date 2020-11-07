from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from document import Document
import re


class Parse:

    def __init__(self):
        self.stop_words = stopwords.words('english')

        ################
        self.no_lowercase_tokens = None
        ################

    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """
        text_tokens = word_tokenize(text)
        self.no_lowercase_tokens = text_tokens
        text_tokens_without_stopwords = [w.lower() for w in text_tokens if w not in self.stop_words]
        return text_tokens_without_stopwords

    def parse_doc(self, doc_as_list):
        """
        This function takes a tweet document as list and break it into different fields
        :param doc_as_list: list re-preseting the tweet.
        :return: Document object with corresponding fields.
        """
        tweet_id = doc_as_list[0]
        tweet_date = doc_as_list[1]
        full_text = doc_as_list[2]
        url = doc_as_list[3]
        retweet_text = doc_as_list[4]
        retweet_url = doc_as_list[5]
        quote_text = doc_as_list[6]
        quote_url = doc_as_list[7]
        term_dict = {}
        tokenized_text = self.parse_sentence(full_text)

        #############################
        tokenized_at = self.parse_at()  #: @ Sign
        tokenized_hashtag = self.parse_hashtag()  #: # Sign
        tokenized_url = self.parse_url("https://www.instagram.com/p/CD7fAPWs3WM/?igshid=o9kf0ugp1l8x")  #: url break up

        #############################

        doc_length = len(tokenized_text)  # after text operations.

        for term in tokenized_text:
            if term not in term_dict.keys():
                term_dict[term] = 1
            else:
                term_dict[term] += 1

        document = Document(tweet_id, tweet_date, full_text, url, retweet_text, retweet_url, quote_text,
                            quote_url, term_dict, doc_length)
        return document

    def parse_at(self):
        tokens_with_at = []
        for i in range(len(self.no_lowercase_tokens) - 1):
            if self.no_lowercase_tokens[i][0] is '@':
                word_to_add = self.no_lowercase_tokens[i][0] + self.no_lowercase_tokens[i + 1]
                tokens_with_at.append(word_to_add)
        return tokens_with_at

    def parse_hashtag(self):
        tokens_with_hashtag = []
        for i in range(len(self.no_lowercase_tokens) - 1):
            if self.no_lowercase_tokens[i][0] is '#':
                word_to_add = self.no_lowercase_tokens[i][0] + self.no_lowercase_tokens[i + 1]
                tokens_with_hashtag.append(word_to_add)

                broken_hashtag = ([a for a in re.split(r'([A-Z][a-z]*)', self.no_lowercase_tokens[i + 1]) if a])  # breaking the
                # hashtagged word to words
                # and lower case them
                for j in range(len(broken_hashtag)):
                    tokens_with_hashtag.append(broken_hashtag[j].lower())

        return tokens_with_hashtag

    def parse_url(self, url):
        no_delimeters_tokens = []
        tokenized_url = word_tokenize(url)

        for h in range(len(tokenized_url) - 1):  # extracts the domain: www.instagram.com......
            if "www." in tokenized_url[h]:
                domain = tokenized_url.pop(h)
                parsed_domain = domain.split("//www.")
                parsed_domain.pop(0)
                str_lst = re.split('[/=:?#]', parsed_domain[0])
                no_delimeters_tokens.extend(str_lst)

        for i in range(len(tokenized_url)):  # processes the rest of the url
            str_lst = re.split('[/=:?#]', tokenized_url[i])
            no_delimeters_tokens.extend(str_lst)

        tokenized_url.clear()
        tokenized_url.append("www")
        for k in range(len(no_delimeters_tokens)):  # eliminates empty "" ("" is part of split())
            if no_delimeters_tokens[k] is not "":
                tokenized_url.append(no_delimeters_tokens[k])

        return tokenized_url
