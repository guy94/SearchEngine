import re
from urllib.parse import urlparse
import spacy
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import en_core_web_sm
from document import Document
import ast
nlp = spacy.load("en_core_web_sm")

class Parse:

    capital_letter_dict = {}
    idx = 0

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
        urls = doc_as_list[3]  #: check if not empty before (ast.literal_eval(urls))
        indices = doc_as_list[4]  #: check if not empty before (list(filter(''.__ne__, re.findall("\d*", doc_as_list[4]))))
        retweet_text = doc_as_list[5]
        retweet_urls = doc_as_list[6]
        retweet_indices = doc_as_list[7]
        quoted_text = doc_as_list[8]
        quote_urls = doc_as_list[9]
        quoted_indices = doc_as_list[10]
        retweet_quoted_text = doc_as_list[11]
        retweet_quoted_urls = doc_as_list[12]
        retweet_quoted_indices = doc_as_list[13]
        term_dict = {}
        tokenized_text = self.parse_sentence(full_text)
        self.tokens = tokenized_text

        #############################
        print("full text: " + full_text)
        print("url: " + urls)
        print("indices: " + indices)
        print("--------------------")
        # print(quoted_text)
        # print(quote_urls)
        # print(quoted_indices)
        # print("--------------------")
        # print(retweet_text)
        # print(retweet_urls)
        # print(retweet_indices)
        # print("----------------")
        print(retweet_quoted_text)
        print(retweet_quoted_urls)
        print(retweet_quoted_indices)
        print("----------------")
        parsed_token_list = self.parse_raw_url()
        ###############################
        

        # if len(parsed_token_list) > 0:
        #     for term in parsed_token_list:
        #         if term not in term_dict.keys():
        #             term_dict[term] = 1
        #         else:
        #             term_dict[term] += 1

        # self.tes_func()
        # entities = self.parse_entities(full_text)
        # entities = []

        # for i, token in enumerate(self.tokens):
        #
        #     number_as_list = re.findall("[-+]?[\d]+(?:\.\d+)?/[-+]?[\d]+(?:\.\d+)?\w?[k|K|m|M|b|B]?"
        #                   "|[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?[k|K|m|M|b|B]?", token)
        #
        #     if token.isalpha():  #: capital letters
        #         self.check_if_capital(token)
        #
        #     if token.startswith('@'):  #: @ sign
        #         if i < len(self.tokens) - 1:
        #             parsed_token_list = list(token + self.tokens[i + 1])
        #
        #     elif token.startswith('#'):  #: # sign
        #         parsed_token_list = self.parse_hashtag(token)
        #
        #     elif len(number_as_list) != 0:  #: numbers
        #         if i == 0 and i < len(self.tokens) - 1:
        #             parsed_token_list = list(self.parse_numbers(number_as_list[0], None, self.tokens[i + 1]))
        #         elif i < len(self.tokens) - 1:
        #             parsed_token_list = list(self.parse_numbers(number_as_list[0], self.tokens[i - 1], self.tokens[i + 1]))
        #         else:
        #             parsed_token_list = list(self.parse_numbers(number_as_list[0], self.tokens[i - 1], None))
        #
        #     if len(parsed_token_list) > 0:
        #         for term in parsed_token_list:
        #             if term not in term_dict.keys():
        #                 term_dict[term] = 1
        #             else:
        #                 term_dict[term] += 1
        #############################

        doc_length = len(tokenized_text)  # after text operations.

        for term in tokenized_text:
            if term not in term_dict.keys():
                term_dict[term] = 1
            else:
                term_dict[term] += 1

        document = Document(tweet_id, tweet_date, full_text, urls, retweet_text, retweet_urls, quoted_text,
                            quote_urls, term_dict, doc_length)
        return document


    def check_if_capital(self, token):  # counting may be unnescesary
        rest_of_token = token[1:].upper()
        token = token[0] + rest_of_token
        if token.isupper():
            if token in Parse.capital_letter_dict:
                Parse.capital_letter_dict[token][1] += 1
            else:
                Parse.capital_letter_dict[token] = [True, 1]
        else:
            new_word = token.upper()  # title
            if new_word in Parse.capital_letter_dict:
                Parse.capital_letter_dict[new_word][0] = False
                Parse.capital_letter_dict[new_word][1] += 1

    def parse_hashtag(self, token):
        tokens_with_hashtag = [token.lower()]
        token = token.split("#")[1]
        tokens_with_hashtag.extend(([a.lower() for a in re.split(r'([A-Z]*[a-z]*)', token) if a]))

        return tokens_with_hashtag

    def parse_url_text(self, token):
        # domain = list(re.findall(r'(www\.)?(\w+[-?\w+]?)(\.\w+)', token))
        domain = urlparse(token).netloc
        tokenize_url = re.split('[/=:?#]', token)
        index = tokenize_url.index(domain)
        www_str = ''
        if "www." in domain:
            domain = domain[4:]
            www_str = "www"

        tokenize_url.pop(index)
        tokenize_url.insert(index, www_str)
        tokenize_url.insert(index + 1, domain)

        to_return = []
        for i in range(len(tokenize_url)):
            if tokenize_url[i] != "":
                to_return.append(tokenize_url[i])

        return to_return

    def parse_numbers(self, number_as_str, word_before, word_after):
        # print(str(Parse.idx) + ": " + number_as_str)
        # Parse.idx += 1
        str_no_commas = re.sub("[^-?\d\./]", "", number_as_str)
        signs = {'usd': '$', 'aud': '$', 'eur': '€', '$': '$', '€': '€', '£': '£', 'percent': '%', 'percentage': '%',
                 '%': '%'}
        quantities = ["thousands", "thousand", "millions", "million", "billions", "billion"]
        quantity = ""
        result = ""
        alpha = ''

        if number_as_str[-1].isalpha():
            alpha = number_as_str[-1]

        if word_before is not None:
            word_before = word_before.lower()

        if word_after is not None:
            word_after = word_after.lower()

        if "/" in str_no_commas:
            num, denum = str_no_commas.split('/')
            as_number = float(num) / float(denum)
        elif "." in str_no_commas:
            as_number = float("{:.3f}".format(float(str_no_commas)))
        else:
            as_number = int(str_no_commas)
            
        numbers_signs_list = [""]*3

        if word_before in signs:  # looks for signs like $ %
            numbers_signs_list[0] = signs[word_before]

        if word_after is None or (word_after not in signs and word_after not in quantities):
            if as_number < 1000:
                numbers_signs_list[1] = str(as_number)
            elif as_number < 1000000:
                quantity = 'K'
                numbers_signs_list[1] = str(as_number / 1000)
            elif 1000000 < as_number < 1000000000:
                quantity = 'M'
                numbers_signs_list[1] = str(as_number / 1000000)
            elif as_number > 1000000000:
                quantity = 'B'
                numbers_signs_list[1] = str(as_number / 1000000000)

        else:
            if word_after in signs:  # looks for signs like $ %
                numbers_signs_list[1] = str(as_number)
                numbers_signs_list[2] = signs[word_after]

            elif word_after in quantities:  # thousand, million etc.
                if word_after == "thousands" or word_after == "thousand":
                    if as_number < 1000:
                        quantity = 'K'
                        numbers_signs_list[1] = str(as_number)
                    elif as_number < 1000000:
                        quantity = 'M'
                        numbers_signs_list[1] = str(as_number / 1000)
                    else:
                        quantity = 'B'
                        numbers_signs_list[1] = str(as_number / 1000000)

                elif word_after == "millions" or word_after == "million":
                    if as_number < 1000:
                        quantity = 'M'
                        numbers_signs_list[1] = str(as_number)
                    elif as_number < 1000000:
                        quantity = 'B'
                        numbers_signs_list[1] = str(as_number / 1000)

                elif word_after == "billions" or word_after == "billion":
                    quantity = 'B'
                    numbers_signs_list[1] = str(as_number)

        if "." in numbers_signs_list[1]:
            numbers_signs_list[1] = str(float("{:.3f}".format(float(numbers_signs_list[1]))))
        if quantity == "":
            numbers_signs_list[1] = numbers_signs_list[1] + alpha

        numbers_signs_list[1] = numbers_signs_list[1] + quantity
        ret = result.join(numbers_signs_list)

        #####
        # if word_after is not None and word_before is not None:
        #     print("before: " + word_before + number_as_str + word_after + ", after: " + ret)
        # if word_after is not None:
        #     print("before: " + number_as_str + word_after + ", after: " + ret)
        # if word_before is not None:
        #     print("before: " + word_before + number_as_str + ", after: " + ret)
        #####
        return ret

    def parse_entities(self, token):
        doc = nlp(token)
        entity_list = [i for i in doc.ents]

        return entity_list
    def parse_raw_url(self, url, retweet_url, quote_url, full_text):
        parsed_token_list = []
        if url == "":
            if re.search("(?P<url>https?://[^\s]+)", full_text) is not None:  #: URL
                url_from_text = re.search("(?P<url>https?://[^\s]+)", full_text).group("url")
                parsed_token_list = self.parse_url_text(url_from_text)
        else:
            url_split = url[2:-2]
            url_split_list = re.sub('(":")+', " ", url_split)
            url_split_list = url_split_list.split(" ")
            parsed_token_list.extend(url_split_list)

        if retweet_url is not None:
            url_split = retweet_url[2:-2]
            url_split_list = re.sub('(":")+', " ", url_split)
            url_split_list = url_split_list.split(" ")
            parsed_token_list.extend(url_split_list)

        if quote_url is not None:
            url_split = quote_url[2:-2]
            url_split_list = re.sub('(":")+', " ", url_split)
            url_split_list = url_split_list.split(" ")
            parsed_token_list.extend(url_split_list)

        return parsed_token_list

    def tes_func(self):

        ###### checking urls ######
        # print(self.parse_url_text("https://www.facebook.com/some-details-330002341216/"))
        # print(self.parse_url_text("http://ftp://random.vib.slx/"))
        # print(self.parse_url_text(" https://en.wikipedia.org/wiki/Internet#Terminology"))
        # print(self.parse_url_text(""))
        # print(self.parse_url_text(""))

        ###### checking capitals######
        # self.check_if_capital("FirSt")
        # self.check_if_capital("FirSt")
        # self.check_if_capital("FirSt")
        # self.check_if_capital("FirSt")
        # self.check_if_capital("firST")
        # self.check_if_capital("FIRST")
        # self.check_if_capital("FIRST")
        # self.check_if_capital("NBA")
        # self.check_if_capital("NBA")
        # self.check_if_capital("gsw")
        # self.check_if_capital("GsW")


        ###### checking numbers ######
        # s1 = "-50.564564545"
        # s2 = "50,466.55565656"
        s3 = r"3\5"
        # s4 = "53.55"
        # s5 = "percent"
        # s6 = "PerCentage"
        # s7 = "%"
        # s8 = "$"
        # s9 = "5.23/4"
        # s10 = "1500"
        # s11 = "500k"
        num3 = re.findall("[-+]?[\d]+(?:\.\d+)?/[-+]?[\d]+(?:\.\d+)?"
                          "|[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?", s3)[0]
        print(num3)
        # print(self.parse_numbers(s1, "$", None))

        # print(self.parse_numbers(s2, None, "million"))
        # print(self.parse_numbers(s2, None, "m"))
        # print(self.parse_numbers(s2, "m", "m"))
        # print(self.parse_numbers(num3, "$", "%"))
        # print(self.parse_numbers(num3, "$", "million"))
        # print(self.parse_numbers(s4, None, s5))
        # print(self.parse_numbers(s4, s6, None))
        # print(self.parse_numbers(s9, None, s7))
        # print(self.parse_numbers(s2, None, None))
        # print(self.parse_numbers(s10, "BiliOn", None))
        # print(self.parse_numbers(s11, "BiliOn", None))

        x = 9

