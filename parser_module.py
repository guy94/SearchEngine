from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from document import Document
import re


class Parse:

    term_appearance_dict = {}

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
        # url_lst = self.parse_url_text("https://inst-agram.com/p/CD7fAPWs3WM/?igshid=o9kf0ugp1l8x")
        self.tes_func()
        for i, token in enumerate(self.tokens):
            url_from_text = ""
            parsed_token = ''
            if re.search("(?P<url>https?://[^\s]+)", token) is not None:
                url_from_text = re.search("(?P<url>https?://[^\s]+)", token).group("url")
            number_as_list = re.findall("[-+]?[\d]+[.[\d]+]?/[-+]?[-+]?[\d]+[.[\d]+]?", token)  # numbers like 3/5
            if len(number_as_list) == 0:
                number_as_list = re.findall("[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?", token)


            if token.startswith('@'):  #: @ sign
                parsed_token = token

            elif token.startswith('#'):  #: # sign
                parsed_token_list = self.parse_hashtag(token)

            elif url_from_text != "":  #: url
                parsed_token_list = self.parse_url_text(url_from_text)

            elif len(number_as_list) != 0:  #: numbers
                if i < len(self.tokens) - 1:
                    parsed_token = self.parse_numbers(number_as_list[0], self.tokens[i + 1])
                else:
                    parsed_token = self.parse_numbers(number_as_list[0])

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

    def parse_hashtag(self, token):
        tokens_with_hashtag = [token.lower()]
        token = token.split("#")[1]
        tokens_with_hashtag.extend(([a.lower() for a in re.split(r'([A-Z]*[a-z]*)', token) if a]))

        return tokens_with_hashtag

    def parse_url_text(self, token):
        domain = list(re.findall(r'(www\.)?(\w+-?\w+)(\.\w+)', token)[0])
        tokenize_url = re.split('[/=:?#]', token)
        domain_no_www = domain[1] + domain[2]

        index = tokenize_url.index(domain[0] + domain_no_www)
        tokenize_url.pop(index)
        tokenize_url.insert(index, domain[0].split(".")[0])
        tokenize_url.insert(index + 1, domain_no_www)

        to_return = []
        for i in range(len(tokenize_url)):
            if tokenize_url[i] != "":
                to_return.append(tokenize_url[i])

        return to_return

    def parse_percent(self, text):

        textAsAList = []
        only_decimal = re.findall('\d*\.?\d+', text)
        tokenized_url = word_tokenize(text)
        for d in only_decimal:
            j = tokenized_url.index(d)
            if tokenized_url[j + 1] == '%' or tokenized_url[j + 1] == 'percent' or tokenized_url[j + 1] == 'percentage':
                textAsAList.append(d+'%')

        return textAsAList

    def parse_numbers(self, number_as_str, word=None):
        str_no_commas = re.sub("[^\d\./]", "", number_as_str)

        strep = ''
        if word is not None:
            if word != '%' and word != '$':
                if "." or "/" in str_no_commas:
                    as_number = float("{:.3f}".format(float(str_no_commas)))

                else:
                    as_number = int(str_no_commas)

                word = word.lower()

                if word == "thousands" or word == "thousand":
                    if as_number < 1000:
                        strep = str(as_number) + 'K'
                    elif as_number < 1000000:
                        strep = str(as_number / 1000) + 'M'
                    else:
                        strep = str(as_number / 1000000) + 'B'
                elif word == "millions" or word == "million":
                    if as_number < 1000:
                        strep = str(as_number) + 'M'
                    elif as_number < 1000000:
                        strep = str(as_number / 1000) + 'B'
                elif word == "billions" or word == "billion":
                    strep = str(as_number) + 'B'

        else:
            if "." or "/" in str_no_commas:
                as_number = float("{:.3f}".format(float(str_no_commas)))

            else:
                as_number = int(str_no_commas)

            if as_number < 1000:
                strep = str(as_number)
            elif as_number < 1000000:
                strep = str(as_number / 1000) + 'K'
            elif 1000000 < as_number < 1000000000:
                strep = str(as_number / 1000000) + 'M'
            elif as_number > 1000000000:
                strep = str(as_number / 1000000000) + 'B'

        return strep


    def tes_func(self):

        s1 = "50.564564545"
        s2 = "50,466.55565656"
        s3 = "3/5"
        num3 = re.findall("[-+]?[\d]+[.[\d]+]?/[-+]?[-+]?[\d]+[.[\d]+]?", s3)
        if len(num3) == 0:
            num3 = re.findall("[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?", s2)[0]

        lst = [s1, s2, s3]

        # print(self.parse_numbers(s1))
        # print(self.parse_numbers(s2, "million"))
        print(self.parse_numbers(num3[0]))

