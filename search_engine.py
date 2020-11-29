from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
import time
import _pickle as pickle
from query import query_object


def run_engine():
    """
    :return:
    """
    number_of_documents = 0

    config = ConfigClass()
    r = ReadFile(corpus_path=config.get__corpusPath())
    p = Parse()
    indexer = Indexer(config)


    corpus_list = r.read_corpus()
    start = time.time()
    Parse.stemmer = False

    documents_list = r.read_file(file_name=corpus_list[1])
    for i in range(len(documents_list)):
        print(str(number_of_documents))
        parsed_document = p.parse_doc(documents_list[i])
        if(i == len(documents_list) - 1):
            indexer.is_last_doc = True
        indexer.add_new_doc(parsed_document)
        # amount_with_stemmer += len(parsed_document.term_doc_dictionary)
        number_of_documents += 1

    indexer.merge_files()

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

    end = time.time()
    print(end - start)
    print('Finished parsing and indexing. Starting to export files')
    print("number of docs: {}".format(number_of_documents))


    # pickle_out = open("inverted_index", "wb")
    # pickle.dump(indexer.inverted_idx, pickle_out)
    # pickle.dump(number_of_documents, pickle_out)
    # pickle_out.close()


def load_index():
    print('Load inverted index')
    pickle_in = open("inverted_index", "rb")
    inverted_index = pickle.load(pickle_in)
    number_of_docs = pickle.load(pickle_in)
    return inverted_index, number_of_docs


def search_and_rank_query(query, inverted_index, k, number_of_documents):
    p = Parse()
    query_object = p.parse_query(query)
    searcher = Searcher(inverted_index, number_of_documents)
    relevant_docs = searcher.relevant_docs_from_posting(query_object)
    normalized_query = searcher.normalized_query(query_object)
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs, normalized_query)
    return searcher.ranker.retrieve_top_k(ranked_docs, k)


def main():
    run_engine()
    query = input("Please enter a query: ")
    k = int(input("Please enter number of docs to retrieve: "))
    start = time.time()
    inverted_index, number_of_documents = load_index()
    for doc_tuple in search_and_rank_query(query, inverted_index, k, number_of_documents):
        print('tweet id: {}, score (cosine similarity with tf-idf rank): {}'.format(doc_tuple[0], doc_tuple[1]))
    end = time.time()
    print(end - start)