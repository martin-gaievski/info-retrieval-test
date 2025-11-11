from beir import util, LoggingHandler
from beir.datasets.data_loader import GenericDataLoader
from beir.hybrid.evaluation import EvaluateRetrieval
from beir.hybrid.search import RetrievalOpenSearch
from beir.hybrid.data_ingestor import OpenSearchDataIngestor

import logging
import pathlib, os, getopt, sys


def main(argv):
    opts, args = getopt.getopt(argv, "d:u:h:p:i:m:o:s:q:r:",
                               ["dataset=", "dataset_url=", "os_host=", "os_port=", "os_index=", "os_model_id=",
                                "operation=", "subset=", "qty=", "resume_from="])
    dataset = 'nfcorpus' # use nfcorpus by default
    url = ''
    endpoint = ''
    port = ''
    index = ''
    model_id = ''
    operation = 'both' # default value
    subset = None
    qty = sys.maxsize # max number of queries we want to process, everything by default
    resume_from = 1  # default: start from first document (1-indexed)
    for opt, arg in opts:
        if opt in ("-d", "-dataset"):
            dataset = arg
        elif opt in ("-u", "-dataset_url"):
            url = arg
        elif opt in ("-h", "-os_host"):
            endpoint = arg
        elif opt in ("-p", "-os_port"):
            port = arg
        elif opt in ("-i", "-os_index"):
            index = arg
        elif opt in ("-m", "-os_model_id"):
            model_id = arg
        elif opt in ("-o", "-operation"):
            operation = arg
        elif opt in ("-s", "-subset"):
            subset = arg
        elif opt in ("-q", "-qty"):
            qty = int(arg)
        elif opt in ("-r", "-resume_from"):
            resume_from = int(arg)

    #### Just some code to print debug information to stdout
    logging.basicConfig(format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO,
                        handlers=[LoggingHandler()])

    #### /print debug information to stdout
    #### Download scifact.zip dataset and unzip the dataset
    # dataset = "nfcorpus"
    # dataset = "trec-covid"
    # dataset = "arguana"
    # dataset = 'fiqa'
    # url = url.format(dataset)
    url = "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip".format(dataset)
    out_dir = os.path.join(pathlib.Path(__file__).parent.absolute(), "datasets")
    data_path = util.download_and_unzip(url, out_dir)

    data_folder = data_path
    if subset:
        data_folder = data_path + '/' + subset

    #### Provide the data_path where dataset has been downloaded and unzipped
    corpus, queries, qrels = GenericDataLoader(data_folder=data_folder).load(split="test")

    if operation in ['ingest', 'both']:
        print('endpoint:' + endpoint + '; port:' + port)
        if resume_from > 1:
            print(f'Resuming ingestion from document #{resume_from}')
        ingest_data(corpus, endpoint, index, port, resume_from)

    if operation in ['search', 'both']:
        evaluate(corpus, endpoint, index, model_id, port, qrels, queries, qty)


def ingest_data(corpus, endpoint, index, port, resume_from=1):
    OpenSearchDataIngestor(endpoint, port).ingest(corpus, index=index, start_position=resume_from)


def evaluate(corpus, endpoint, index, model_id, port, qrels, queries, qty):
    # This k values are being used for BM25 search
    # bm25_k_values = [1, 3, 5, 10, 100, min(9999, len(corpus))]
    bm25_k_values = [1, 3, 5, 10, 25, 50, 100, 200]
    # This K values are being used for dense model search
    model_k_values = [1, 3, 5, 10, 25, 50, 100, 200]
    # this k values are being used for scoring
    k_values = [1, 3, 5, 10, 25, 50, 100, 200]
    # for method in ['bm25', 'neural', 'hybrid']:
    # for method in ['hybrid']:
    # for method in ['neural']:

    '''for method in ['bm25']:
        print('starting search method ' + method)
        os_retrival = RetrievalOpenSearch(endpoint, port,
                                          index_name=index,
                                          model_id=model_id,
                                          search_method=method,
                                          pipeline_name='norm-pipeline')
        retriever = EvaluateRetrieval(os_retrival, bm25_k_values)
        result_size = max(bm25_k_values)
        results = os_retrival.search_bm25(corpus, queries, top_k=result_size)
        ndcg, _map, recall, precision = retriever.evaluate(qrels, results, k_values)
        print('--- end of results for ' + method)'''

    # for method in ['neural', 'hybrid']:
    for method in ['hybrid']:
        print('starting search method ' + method)
        os_retrival = RetrievalOpenSearch(endpoint, port,
                                          index_name=index,
                                          model_id=model_id,
                                          search_method=method,
                                          pipeline_name='norm-pipeline')
        retriever = EvaluateRetrieval(os_retrival, model_k_values)  # or "cos_sim" for cosine similarity
        top_k = max(model_k_values)
        result_size = max(bm25_k_values)
        # results = retriever.retrieve(corpus, queries)
        results = os_retrival.search_vector(corpus, queries, top_k=top_k, result_size=result_size, query_limit=qty, skip_warmups=True)

        # Get the raw search results
        # raw_results = os_retrival.search_vector(corpus, queries, top_k=top_k, result_size=result_size)

        # Convert tuple results to the expected dictionary format
        '''formatted_results = {}
        # Process each query individually
        for query_id, query_text in queries.items():
            raw_results = os_retrival.search_vector(corpus, {query_id: query_text}, top_k=top_k, result_size=result_size)

            # Initialize the query results
            formatted_results[query_id] = {}

            # Ensure we have results for this query
            if raw_results and len(raw_results) >= 2:
                doc_ids = raw_results[0]
                scores = raw_results[1]
                print(str(doc_ids) + str(scores))

                # Create dictionary of doc_id -> score for this query
                for doc_id, score in zip(doc_ids, scores):
                    if isinstance(doc_id, str) and isinstance(score, (int, float)):
                        formatted_results[query_id][doc_id] = float(score)

            else:
                print("Query return no results")

        print(f"Results quantity:" + str(len(formatted_results)))
        print(f"Results format: {type(formatted_results)}")
        print(f"Sample result structure: {list(formatted_results.items())[:1]}")

        ndcg, _map, recall, precision = retriever.evaluate(qrels, formatted_results, k_values)
        '''

        ndcg, _map, recall, precision = retriever.evaluate(qrels, results, k_values, queries=queries)
        print('--- end of results for ' + method)

    # method = 'hybrid'
    # print('starting search method ' + method)
    # os_retrival = RetrievalOpenSearch(endpoint, port,
    # index_name=index,
    # model_id=model_id,
    # search_method=method,
    # pipeline_name='norm-pipeline')
    # retriever = EvaluateRetrieval(os_retrival, bm25_k_values, model_k_values)  # or "cos_sim" for cosine similarity
    # results = retriever.retrieve(corpus, queries)
    # results = retriever.search(corpus, queries, top_k)
    # ndcg, _map, recall, precision = retriever.evaluate(qrels, results, k_values)
    # print('--- end of results for ' + method)


if __name__ == "__main__":
    main(sys.argv[1:])
