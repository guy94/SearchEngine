from reader import ReadFile
from configuration import ConfigClass
from parser_module import Parse
from indexer import Indexer
from searcher import Searcher
import utils
import time


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
    # for j in range(len(corpus_list)):
    # documents_list = r.read_file(file_name='sample3.parquet')

    start = time.time()
    # documents_list = r.read_file(file_name=corpus_list[0])
    # parsed_document = p.parse_doc(documents_list[249863])
    # parsed_document = p.parse_doc(documents_list[2792])
    # parsed_document = p.parse_doc(documents_list[15000])
    Parse.stemmer = False
    amount_with_stemmer = 0
    amount_with_out_stemmer = 0

    # for i in range(10):
    documents_list = r.read_file(file_name=corpus_list[0])
    # parsed_document = p.parse_doc(documents_list[163322])
    for i in range(20):
        # print(str(number_of_documents))
        parsed_document = p.parse_doc(documents_list[i])
        if(i == 19):
            indexer.is_last_doc = True
        indexer.add_new_doc(parsed_document)
        amount_with_stemmer += len(parsed_document.term_doc_dictionary)
        number_of_documents += 1

    indexer.merge_files()

    # print("avg time with a stop words dict: {}".format(total_time / 30))
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

    utils.save_obj(indexer.inverted_idx, "inverted_idx")
    # utils.save_obj(indexer.postingDict, "posting")


def load_index():
    print('Load inverted index')
    inverted_index = utils.load_obj("inverted_idx")
    return inverted_index


def search_and_rank_query(query, inverted_index, k):
    p = Parse()
    query_as_list = p.parse_sentence(query)
    searcher = Searcher(inverted_index)
    relevant_docs = searcher.relevant_docs_from_posting(query_as_list)
    ranked_docs = searcher.ranker.rank_relevant_doc(relevant_docs)
    return searcher.ranker.retrieve_top_k(ranked_docs, k)


def main():
    run_engine()
    query = input("Please enter a query: ")
    k = int(input("Please enter number of docs to retrieve: "))
    inverted_index = load_index()
    for doc_tuple in search_and_rank_query(query, inverted_index, k):
        print('tweet id: {}, score (unique common words with query): {}'.format(doc_tuple[0], doc_tuple[1]))
