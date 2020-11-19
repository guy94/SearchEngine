import re
from urllib.parse import urlparse
import spacy
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from document import Document

nlp = spacy.load("en_core_web_sm")


class Parse:
    capital_letter_dict = {}
    idx = 0

    def __init__(self):
        self.stop_words = stopwords.words('english')
        self.tokens = None
        self.is_num_after_num = False

    def parse_sentence(self, text):
        """
        This function tokenize, remove stop words and apply lower case for every word within the text
        :param text:
        :return:
        """
        text_tokens = word_tokenize(text)
        self.tokens = text_tokens
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
        urls = doc_as_list[3]
        indices = doc_as_list[4]
        retweet_text = doc_as_list[5]
        retweet_urls = doc_as_list[6]
        retweet_indices = doc_as_list[7]
        quoted_text = doc_as_list[8]
        quote_urls = doc_as_list[9]
        quoted_indices = doc_as_list[10]
        retweet_quoted_text = doc_as_list[11]
        retweet_quoted_urls = doc_as_list[12]
        retweet_quoted_indices = doc_as_list[13]

        concatenated_text = self.concatenate_tweets(full_text, retweet_quoted_text, quoted_text)
        tokenized_text = self.parse_sentence(concatenated_text)
        term_dict = {}

        ########################################
        # TODO: check if indices needed
        # indices_as_list = self.indices_as_list(indices)
        # indices_retweet_as_list = self.indices_as_list(retweet_indices)
        # indices_quoted_as_list = self.indices_as_list(quoted_indices)
        # indices_retweet_quoted_as_list = self.indices_as_list(retweet_quoted_indices)
        # self.tes_func()
        # entities = self.parse_entities(full_text)
        # entities = []

        broken_urls = self.parse_raw_url(urls, retweet_urls, quote_urls, retweet_quoted_urls, full_text)
        broken_urls = self.parse_url_text(broken_urls)

        # for term in broken_urls:
        #     if term not in term_dict.keys():
        #         term_dict[term] = 1
        #     else:
        #         term_dict[term] += 1

        last_number_parsed = None
        count_num_in_a_row = 0
        test_text = "#covid-19 5 thousands in his pocket #rakBibi @bibinetanyahu call 555-888 if you have 50$"
        test_tokens = word_tokenize(test_text)
        self.tokens = test_tokens
        entity_counter = 1
        for i, token in enumerate(self.tokens):
            parsed_token_list = []
            number_as_list = re.findall("[-+]?[\d]+(?:\.\d+)?/[-+]?[\d]+(?:\.\d+)?\w?[k|K|m|M|b|B]?"
                                        "|[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?[k|K|m|M|b|B]?", token)

            is_date = self.parse_date(token)

            if token.isalpha():  #: capital letters
                while i + entity_counter < len(self.tokens) and self.tokens[i + entity_counter].istitle():
                    token += self.tokens[i + entity_counter]
                entity_counter = 1
            self.check_if_capital(token)
            count_num_in_a_row = 0

            if token.startswith('@'):  #: @ sign
                if i < len(self.tokens) - 1:
                    parsed_token_list = [token + self.tokens[i + 1]]
                    count_num_in_a_row = 0

            elif token.startswith('#'):  #: # sign
                if i < len(self.tokens) - 1:
                    parsed_token_list = self.parse_hashtag(token + self.tokens[i + 1])
                    count_num_in_a_row = 0

            elif "-" in token and not token.startswith("-"):  # not a number starts with "-". example covid-19
                token_before = ""
                if i > 0:
                    token_before = self.tokens[i-1]
                hyphen_list = self.parse_hyphen(token, token_before)
                count_num_in_a_row = 0

            elif is_date:  # date format
                parsed_token_list = [token]

            elif len(number_as_list) != 0 and len(parsed_token_list) == 0:  #: numbers
                if len(number_as_list) > 1:
                    number_as_list = ["".join(number_as_list)]
                if '-' not in number_as_list[0]:  # if a representation of phone numbers, do nothing
                    count_num_in_a_row += 1
                    if i == 0 and i < len(self.tokens) - 1:
                        parsed_token_list = list(self.parse_numbers(number_as_list[0], None, self.tokens[i + 1]))
                    elif i < len(self.tokens) - 1:
                        parsed_token_list = list(
                            self.parse_numbers(number_as_list[0], self.tokens[i - 1], self.tokens[i + 1]))
                    else:
                        parsed_token_list = list(self.parse_numbers(number_as_list[0], self.tokens[i - 1], None))

                    if count_num_in_a_row == 2 and len(parsed_token_list) == 2:  # this block is for numbers like 25 3/4 that appear together
                        parsed_token_list = [last_number_parsed + " " + parsed_token_list[1]]
                        count_num_in_a_row = 0
                        del term_dict[last_number_parsed]
                    else:
                        last_number_parsed = parsed_token_list[0]
                else:
                    parsed_token_list = number_as_list

            if len(parsed_token_list) > 0:
                for term in parsed_token_list:
                    if term not in term_dict.keys():
                        term_dict[term] = 1
                    else:
                        term_dict[term] += 1
        #############################

        doc_length = len(tokenized_text)  # after text operations.

        # for term in tokenized_text:
        #     if term not in term_dict.keys():
        #         term_dict[term] = 1
        #     else:
        #         term_dict[term] += 1

        document = Document(tweet_id, tweet_date, full_text, urls, retweet_text, retweet_urls, quoted_text,
                            quote_urls, term_dict, doc_length)
        # print(term_dict)
        # print("--------------------")
        return document

    def parse_date(self, token):
        date_list = re.findall("\d{1,4}[-\.\/]\d{1,4}[-\.\/]\d{1,4}", token)
        if len(date_list) > 0:
            # print(date_list)
            return True

        return False

    def parse_hyphen(self, token, token_before):
        """
        :param token:
        :return: list of a parsed phrase split by a hyphen
        """
        to_return = []
        if token_before != "@" and token_before != "#":
            to_return = [token]
        split_hyphen = token.split("-")
        is_alpha = False

        for i in split_hyphen:
            if i != "":
                if i.isalpha():
                    is_alpha = True
            to_return.append(i.lower())

        if not is_alpha:
            return [token]

        return to_return

    def check_if_capital(self, token):  # counting may be unnescesary
        for ent in token:
            rest_of_token = ent[1:].upper()
            ent = ent[0] + rest_of_token
            if ent.isupper():
                if ent in Parse.capital_letter_dict:
                    Parse.capital_letter_dict[ent][1] += 1
                else:
                    Parse.capital_letter_dict[ent] = [True, 1]
            else:
                new_word = ent.upper()  # title
                if new_word in Parse.capital_letter_dict:
                    Parse.capital_letter_dict[new_word][0] = False
                    Parse.capital_letter_dict[new_word][1] += 1

    def parse_hashtag(self, token):
        """
        hashtags parsing
        :param token:
        :return: list of a decomposed hashtag
        """
        tokens_with_hashtag = [token.lower()]
        token = token.split("#")[1]
        # tokens_with_hashtag.extend(([a.lower() for a in re.findall("[A-Z]*[a-z]*", token) if a]))
        if "-" not in token:
            tokens_with_hashtag.extend(([a.lower() for a in re.split('([A-Z][a-z]+)''|^([a-z]+)', token) if a]))

        return tokens_with_hashtag

    def parse_url_text(self, urls):
        # domain = list(re.findall(r'(www\.)?(\w+[-?\w+]?)(\.\w+)', token))
        to_return = []
        for token in urls:
            domain = urlparse(token[1:-1]).netloc
            tokenize_url = re.split('[/=:?#]', token[1:-1])
            index = tokenize_url.index(domain)
            www_str = ''
            if "www." in domain:
                domain = domain[4:]
                www_str = "www"

            tokenize_url.pop(index)
            tokenize_url.insert(index, www_str)
            tokenize_url.insert(index + 1, domain)

            for i in range(len(tokenize_url)):
                if tokenize_url[i] != "":
                    to_return.append(tokenize_url[i])

        return to_return

    def parse_numbers(self, number_as_str, word_before, word_after):
        str_no_commas = re.sub("[^-?\d\./]", "", number_as_str)
        signs = {'usd': '$', 'aud': '$', 'eur': '€', '$': '$', '€': '€', '£': '£', 'percent': '%', 'percentage': '%',
                 '%': '%'}
        quantities = ["thousands", "thousand", "millions", "million", "billions", "billion"]
        quantity = ""
        result = ""
        alpha = ''
        division_as_is = ""
        is_division = False

        if number_as_str[-1].isalpha():
            alpha = number_as_str[-1]

        if word_before is not None:
            word_before = word_before.lower()

        if word_after is not None:
            word_after = word_after.lower()

        if "/" in str_no_commas:
            division_as_is = str_no_commas
            is_division = True
            num, denum = str_no_commas.split('/')
            as_number = float(num) / float(denum)
        elif "." in str_no_commas:
            amount_of_dots = str_no_commas.count(".")
            if amount_of_dots > 1:
                if not str_no_commas.startswith("."):
                    return [str_no_commas]
                str_no_commas = str_no_commas[1:]
            as_number = float("{:.3f}".format(float(str_no_commas)))
        else:
            as_number = int(str_no_commas)

        numbers_signs_list = [""] * 3

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
            if is_division:
                division_as_is = division_as_is + alpha

        if is_division:
            division_as_is = numbers_signs_list[0] + division_as_is + quantity + numbers_signs_list[2]
        numbers_signs_list[1] = numbers_signs_list[1] + quantity
        ret = result.join(numbers_signs_list)
        returnlist = [ret, division_as_is]

        #####

        # if word_after is not None and word_before is not None:
        #     print("before: " + word_before + number_as_str + word_after + ", after: " + ret)
        # elif word_after is not None:
        #     print("before: " + number_as_str + word_after + ", after: " + ret)
        # elif word_before is not None:
        #     print("before: " + word_before + number_as_str + ", after: " + ret)
        # print("---------------------------------")
        #####

        if returnlist[1] != "":
            return returnlist
        return [ret]

    def parse_entities(self, token_list):
        # doc = nlp(token)
        # entity_list = [i for i in doc.ents]

        return entity_list

    def parse_raw_url(self, url, retweet_url, quote_url, retweet_quoted_urls, full_text):
        parsed_token_list = set()
        input_list = [url, retweet_url, quote_url, retweet_quoted_urls]
        if url == "":
            if re.search("(?P<url>https?://[^\s]+)", full_text) is not None:  #: URL
                url_from_text = re.search("(?P<url>https?://[^\s]+)", full_text).group("url")
                raw_token_list = self.parse_url_text(url_from_text)
        for urls in input_list:
            if urls is not None and urls != "{}":
                url_str_as_list = urls[2:-2]
                url_str_as_list = url_str_as_list.replace("null", "\"null\"")
                dict_as_list = re.sub('(":")+''|(",")+', "\" \"", url_str_as_list)

                list_url = dict_as_list.split(" ")
                i = 0
                for i, key in enumerate(list_url):
                    if key != "\"null\"" and i % 2 != 0:
                        parsed_token_list.add(key)
        return parsed_token_list

    def indices_as_list(self, indices):
        indices_as_list = []
        if (indices is not None) and (indices != ""):
            indices_as_list = (list(filter(''.__ne__, re.findall("\d*", indices))))
        for i in range(len(indices_as_list)):
            indices_as_list[i] = int(indices_as_list[i])

        return indices_as_list

    # def add_to_tokens(self, text):
    #     if text != "":
    #         tokenized_quoted_text = self.parse_sentence(text)
    #         self.tokens = self.tokens + tokenized_quoted_text

    def concatenate_tweets(self, tweet, retweet_quoted_text, quoted_text):

        tweet_to_return = tweet
        if retweet_quoted_text is not None:
            tweet_to_return += " "
            tweet_to_return += retweet_quoted_text
        if quoted_text is not None and retweet_quoted_text != quoted_text:
            tweet_to_return += " "
            tweet_to_return += quoted_text

        return tweet_to_return

    def tes_func(self):

        ###### hashtags #######
        # s1 = "#IsAlpha123"
        # s2 = "#Covid19"
        # s3 = "#noWayYouKnow"
        # s4 = "#PlayTheGuitar2015"
        # print(self.parse_hashtag(s1))
        # print(self.parse_hashtag(s2))
        # print(self.parse_hashtag(s3))
        # print(self.parse_hashtag(s4))

        ###### checking hyphen ######
        # s1 = "covid-19"
        # s2 = "3434-585"
        # s3 = "-19"
        # print(self.parse_date(s1))
        # print(self.parse_date(s2))
        # print(self.parse_date(s3))


        ######### checking parse_date #########

        # s1 = "covid-19"
        # s2 = "1990-12-1"
        # s3 = "859.5.02"
        # s4 = "11.5.94"
        # s5 = "-56"
        # s6= "-26"
        # s6 = "16.5"
        # print(self.parse_date(s1))
        # print(self.parse_date(s2))
        # print(self.parse_date(s3))
        # print(self.parse_date(s4))
        # print(self.parse_date(s5))



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
        # s3 = r"3\5"
        # s4 = "53.55"
        # # s5 = "percent"
        # # s6 = "PerCentage"
        # s7 = "%"
        # # s8 = "$"
        # s9 = "5.23/4"
        # # s10 = "1500"
        # # s11 = "500k"
        # num3 = re.findall("[-+]?[\d]+(?:\.\d+)?/[-+]?[\d]+(?:\.\d+)?"
        #                   "|[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?", s3)[0]
        # print(num3)
        # print(self.parse_numbers(s1, "$", None))

        # print(self.parse_numbers(s2, None, "million"))
        # print(self.parse_numbers(s2, None, "m"))
        # print(self.parse_numbers(s2, "m", "m"))
        # print(self.parse_numbers(num3, "$", "%"))
        # print(self.parse_numbers(num3, "$", "million"))
        # print(self.parse_numbers(s4, None, s5))
        # print(self.parse_numbers(s4, s6, None))
        # print(self.parse_numbers(s4, None, s7))
        # print(self.parse_numbers(s2, None, None))
        # print(self.parse_numbers(s10, "BiliOn", None))
        # print(self.parse_numbers(s11, "BiliOn", None))

        x = 9
