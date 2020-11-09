from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from document import Document
from decimal import Decimal
import re


class Parse:

    def __init__(self):
        self.stop_words = stopwords.words('english')
        self.tokens = None

    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """
        text_tokens = word_tokenize(text)
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
        self.tokens = full_text.split(" ")
        tokenized_text = self.parse_sentence(full_text)

        #############################


        for token in self.tokens:
            parsed_token = ''
            if token.startswith('@'):
                parsed_token = token
            if token.startswith('#'):
                parsed_token = self.parse_hashtag(token)

            if parsed_token != '':
                if parsed_token not in term_dict.keys():
                    term_dict[parsed_token] = 1
                else:
                    term_dict[parsed_token] += 1

        # tokenized_url = self.parse_url("https://www.instagram.com/p/CD7fAPWs3WM/?igshid=o9kf0ugp1l8x")  #: url break up
        tokenized_percentage = self.parse_percent(" i have 6 percent of my money 6.5%")  #: percentage -> %
        tokenized_int = self.parse_numbers("i work since 1,975 thousands evey 55 day 152,656 and 44, 34 Thousands 55.56 Million")  #: nums (123,000 -> 123k)
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

    def parse_at(self, token):
        tokens_with_at = []
        # for i in range(len(self.tokens) - 1):
        #     if self.tokens[i][0] is '@':
        word_to_add = self.tokens[i][0] + self.tokens[i + 1]
        tokens_with_at.append(word_to_add)
        return tokens_with_at

    def parse_hashtag(self, token):
        tokens_with_hashtag = []
        for i in range(len(self.tokens) - 1):
            if self.tokens[i][0] is '#':
                word_to_add = self.tokens[i][0] + self.tokens[i + 1]
                tokens_with_hashtag.append(word_to_add)

                broken_hashtag = ([a for a in re.split(r'([A-Z][a-z]*)', self.tokens[i + 1]) if a])  # breaking the
                # hashtagged word to words
                # and lower case them
                for j in range(len(broken_hashtag)):
                    tokens_with_hashtag.append(broken_hashtag[j].lower())

        return tokens_with_hashtag

    # def parse_url(self, url):
    #     no_delimeters_tokens = []
    #     tokenized_url = word_tokenize(url)
    #
    #     for h in range(len(tokenized_url) - 1):  # extracts the domain: www.instagram.com......
    #         if "www." in tokenized_url[h]:
    #             domain = tokenized_url.pop(h)
    #             parsed_domain = domain.split("//www.")
    #             parsed_domain.pop(0)
    #             str_lst = re.split('[/=:?#]', parsed_domain[0])
    #             no_delimeters_tokens.extend(str_lst)
    #
    #     for i in range(len(tokenized_url)):  # processes the rest of the url
    #         str_lst = re.split('[/=:?#]', tokenized_url[i])
    #         no_delimeters_tokens.extend(str_lst)
    #
    #     tokenized_url.clear()
    #     tokenized_url.append("www")
    #     for k in range(len(no_delimeters_tokens)):  # eliminates empty "" ("" is part of split())
    #         if no_delimeters_tokens[k] is not "":
    #             tokenized_url.append(no_delimeters_tokens[k])
    #
    #     return tokenized_url

    def parse_url_text(self, index):



    def parse_percent(self, text):

        textAsAList = []
        only_decimal = re.findall('\d*\.?\d+', text)
        tokenized_url = word_tokenize(text)
        for d in only_decimal:
            j = tokenized_url.index(d)
            if tokenized_url[j + 1] == '%' or tokenized_url[j + 1] == 'percent' or tokenized_url[j + 1] == 'percentage':
                textAsAList.append(d+'%')

        return textAsAList

    def parse_numbers(self, text):

        text_as_list = []
        only_decimals = re.findall("[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?", text)
        tokenized_url = word_tokenize(text)
        for d in only_decimals:
            d_no_commas = re.sub("[^\d\.]", "", d)
            j = tokenized_url.index(d)
            index_exist_before = self.index_exists(tokenized_url, j-1)
            index_exist_after = self.index_exists(tokenized_url, j+1)

            if ((index_exist_after and (tokenized_url[j + 1] is not '%' and tokenized_url[j + 1] is not '$')) or
                (index_exist_before and (tokenized_url[j - 1] != '%' or tokenized_url[j - 1] != '$'))):
                if "." in d_no_commas:
                    d_as_number = float(d_no_commas)

                else:
                    d_as_number = int(d_no_commas)

                # num_in_decimal = Decimal(d.replace(',', '.'))
                strep = ''
                if d_as_number < 1000:
                    if index_exist_after and (tokenized_url[j + 1] == "Thousands" or tokenized_url[j + 1] == "Thousand"):
                        strep = str(d_as_number) + 'K'
                    elif index_exist_after and (tokenized_url[j + 1] == "Millions" or tokenized_url[j + 1] == "Million"):
                        strep = str(d_as_number) + 'M'
                    elif index_exist_after and (tokenized_url[j + 1] == "Billions" or tokenized_url[j + 1] == "Billion"):
                        strep = str(d_as_number) + 'B'
                    else:
                        strep = str(d_as_number)
                elif d_as_number < 1000000:
                    numrep = d_as_number / 1000
                    strep = str(numrep) + 'K'
                elif 1000000 < d_as_number < 1000000000:
                    numrep = d_as_number / 1000000
                    strep = str(numrep) + 'M'
                elif d_as_number > 1000000000:
                    numrep = d_as_number / 1000000000
                    strep = str(numrep) + 'B'

                # print(strep)
                if strep != '':
                    text_as_list.append(strep)

        return text_as_list

    def index_exists(self, ls, i):
        return (0 <= i < len(ls)) or (-len(ls) <= i < 0)



