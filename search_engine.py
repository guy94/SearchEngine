from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import time
from tqdm import tqdm

try:
    import _pickle as pickle
except:
    import pickle


def run_engine(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):
    """
    :return:
    """
    config = ConfigClass(corpus_path, output_path, stemming)
    number_of_documents = 0
    r = ReadFile(corpus_path=config.get__corpusPath())
    p = Parse()
    indexer = Indexer(config)
    Parse.stemmer = stemming

    corpus_list = r.read_corpus()

    # documents_list = r.read_file(file_name=corpus_list[1])
    # for i in tqdm(range(len(documents_list))):
    #     parsed_document = p.parse_doc(documents_list[i])
    #     if (i == len(documents_list) - 1):
    #         indexer.is_last_doc = True
    #     indexer.add_new_doc(parsed_document)
    #     # amount_with_stemmer += len(parsed_document.term_doc_dictionary)
    #     number_of_documents += 1
    #
    # indexer.merge_files()

    ##################
    # for doc in next(r.read_file(corpus_list[0])):
    #     print(number_of_documents)
    #     parsed_document = p.parse_doc(doc)
    #     number_of_documents += 1
    #     if number_of_documents == 10000:
    #         break
    ##################

    # Iterate over every document in the file
    # for idx, document in enumerate(documents_list):
    #     # parse the document
    #     print(documents_list[idx])
    #     parsed_document = p.parse_doc(document)
    #     number_of_documents += 1
    #     print(str(number_of_documents))
    #     # index the document data
    #     indexer.add_new_doc(parsed_document)

    print('Finished parsing and indexing. Starting to export files')
    print("number of docs: {}".format(number_of_documents))

    # pickle_out = open("inverted_index", "wb")
    # pickle.dump(indexer.inverted_idx, pickle_out)
    # pickle.dump(number_of_documents, pickle_out)
    # pickle.dump(indexer.docs_dict, pickle_out) #TODO::USE THAT SHIT!!!!!!
    # pickle_out.close()


def load_index():
    print('Load inverted index')
    pickle_in = open("inverted_index", "rb")
    inverted_index = pickle.load(pickle_in)
    number_of_docs = pickle.load(pickle_in)
    tweet_id_dict = pickle.load(pickle_in)
    return inverted_index, number_of_docs


def search_and_rank_query(query, inverted_index, k, number_of_documents):
    p = Parse()
    query_object = p.parse_query(query)
    searcher = Searcher(inverted_index, number_of_documents)
    relevant_docs = searcher.relevant_docs_from_posting(query_object)
    normalized_query = searcher.normalized_query(query_object)
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs, normalized_query)
    return searcher.ranker.retrieve_top_k(ranked_docs, k)


def read_queries_file(queries):
    queries_list = []
    file_in = open(queries, encoding="utf8")

    while True:
        try:
            query = file_in.readline()
            if query != '\n':
                query = query.split(".", 1)[1]
                query = query.split("\n", 1)[0]
                queries_list.append(query)
        except:
            break
    file_in.close()
    return queries_list


def main(corpus_path, output_path, stemming, queries, num_docs_to_retrieve):
    run_engine(corpus_path, output_path, stemming, queries, num_docs_to_retrieve)
    inverted_index, number_of_documents = load_index()

    #TODO: check min/max value for k. what if there is no query?

    if type(queries) is list:
        queries_as_list = queries
    else:
        queries_as_list = read_queries_file(queries)

    for query in queries_as_list:
        for doc_tuple in search_and_rank_query(query, inverted_index, num_docs_to_retrieve, number_of_documents):
            x = 5
            print('tweet id: {}, score (cosine similarity with tf-idf rank): {}'.format(doc_tuple[0], doc_tuple[1]))
