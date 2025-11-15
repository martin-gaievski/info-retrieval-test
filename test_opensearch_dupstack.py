from beir import util, LoggingHandler
from beir.datasets.data_loader import GenericDataLoader
from beir.hybrid.evaluation import EvaluateRetrieval
from beir.hybrid.search import RetrievalOpenSearch
from beir.hybrid.data_ingestor import OpenSearchDataIngestor

import logging
import pathlib, os, getopt, sys


def main(argv):
    opts, args = getopt.getopt(argv, "u:h:p:i:m:n:o:l:e:f:r:",
                               ["dataset_url=", "os_host=", "os_port=", "os_index=", "os_model_id=", "num_of_runs=", "operation=", "pipelines=", "method=", "subset=", "resume_from="])
    dataset = 'cqadupstack'
    url = ''
    endpoint = ''
    port = ''
    index = ''
    model_id = ''
    num_of_runs = 2
    operation = "evaluate"
    pipelines = 'norm-pipeline'
    mmethod = 'hybrid'
    subset = ''
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
        elif opt in ("-n", "-num_of_runs"):
            num_of_runs = int(arg)
        elif opt in ("-o", "-operation"):
            operation = arg
        elif opt in ("-l", "-pipelines"):
            pipelines = arg
        elif opt in ("-e", "-method"):
            mmethod = arg
        elif opt in ("-f", "-subset"):
            subset = arg
        elif opt in ("-r", "-resume_from"):
            resume_from = int(arg)


    #### Just some code to print debug information to stdout
    logging.basicConfig(format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=logging.INFO,
                        handlers=[LoggingHandler()])

    #### /print debug information to stdout
    #### Download scifact.zip dataset and unzip the dataset
    url = url.format(dataset)
    out_dir = os.path.join(pathlib.Path(__file__).parent.absolute(), "datasets")
    data_path = util.download_and_unzip(url, out_dir)

    # Get list of available subsets
    available_subsets = [d for d in os.listdir(data_path) 
                        if os.path.isdir(os.path.join(data_path, d)) and not d.startswith('.')]
    available_subsets.sort()
    
    # Validate subset parameter for ingest operation
    if operation in ['ingest', 'both']:
        if not subset:
            print("ERROR: The -f/--subset parameter is required for ingest operation!")
            print(f"Available subsets: {', '.join(available_subsets)}")
            print("Use 'all' to ingest all subsets")
            sys.exit(1)
        elif subset != 'all' and subset not in available_subsets:
            print(f"ERROR: Invalid subset '{subset}'!")
            print(f"Available subsets: {', '.join(available_subsets)}")
            print("Use 'all' to ingest all subsets")
            sys.exit(1)
    
    # Determine which subsets to load
    if subset == 'all':
        subsets_to_load = available_subsets
        print(f"Will process ALL {len(subsets_to_load)} subsets: {', '.join(subsets_to_load)}")
    elif subset:
        subsets_to_load = [subset]
        print(f"Will process subset: {subset}")
    else:
        # For evaluate operation without subset specified, load all
        subsets_to_load = available_subsets
        print(f"Loading all {len(subsets_to_load)} subsets for evaluation")
    
    # Load only the required subsets
    mega_corpus = []
    mega_queries = []
    mega_qrels = []
    data_name = []
    
    for subset_name in subsets_to_load:
        subset_path = os.path.join(data_path, subset_name) + "/"
        print(f"Loading subset: {subset_name} from {subset_path}")
        try:
            corpus, queries, qrels = GenericDataLoader(data_folder=subset_path).load(split="test")
            mega_corpus.append(corpus)
            mega_queries.append(queries)
            mega_qrels.append(qrels)
            data_name.append(subset_name)
            print(f"  - Loaded {len(corpus)} documents from {subset_name}")
        except Exception as e:
            print(f"  - ERROR loading {subset_name}: {e}")
            continue

    if not mega_corpus:
        print("ERROR: No data was loaded!")
        sys.exit(1)

    if operation == 'ingest' or operation == 'both':
        ingest_data(mega_corpus, data_name, endpoint, index, port, subset, resume_from)

    if operation == 'evaluate' or operation == 'both':
        evaluate(mega_corpus, data_name, endpoint, index, model_id, port, mega_qrels, mega_queries, num_of_runs, pipelines, mmethod, subset)


def ingest_data(mega_corpus, data_name, endpoint, index, port, subset, resume_from=1):
    # Determine index naming strategy
    use_separate_indices = "-" in index  # If index contains dash, assume subset-specific naming
    
    if subset == 'all':
        # Ingest all subsets
        total_ingested = 0
        for i in range(len(mega_corpus)):
            # Use subset-specific index name if pattern detected
            if use_separate_indices:
                subset_index = f"{index.split('-')[0]}-{data_name[i]}"
            else:
                subset_index = index
                
            print(f"\n=== Ingesting subset: {data_name[i]} ({i+1}/{len(data_name)}) ===")
            print(f"    Target index: {subset_index}")
            print(f"    Documents to ingest: {len(mega_corpus[i])}")
            
            if resume_from > 1 and i == 0:
                print(f"    Resuming from document #{resume_from}")
                OpenSearchDataIngestor(endpoint, port).ingest(mega_corpus[i], index=subset_index, start_position=resume_from)
            else:
                OpenSearchDataIngestor(endpoint, port).ingest(mega_corpus[i], index=subset_index)
            total_ingested += len(mega_corpus[i])
            print(f"    Completed {data_name[i]}. Total documents ingested so far: {total_ingested}")
    else:
        # Ingest specific subset
        for i in range(len(mega_corpus)):
            if data_name[i] == subset:
                # Use subset-specific index name if pattern detected
                if use_separate_indices:
                    subset_index = f"{index.split('-')[0]}-{data_name[i]}"
                else:
                    subset_index = index
                    
                print(f"\n=== Ingesting subset: {data_name[i]} ===")
                print(f"    Target index: {subset_index}")
                print(f"    Documents to ingest: {len(mega_corpus[i])}")
                
                if resume_from > 1:
                    print(f"    Resuming from document #{resume_from}")
                OpenSearchDataIngestor(endpoint, port).ingest(mega_corpus[i], index=subset_index, start_position=resume_from)
                break


def evaluate(mega_corpus, data_name, endpoint, index, model_id, port, mega_qrels, mega_queries, num_of_runs, pipelines, mmethod, subset):
    # This k values are being used for BM25 search
    # bm25_k_values = [1, 3, 5, 10, 100, min(9999, len(corpus))]
    bm25_k_values = [1, 3, 5, 10, 100]
    # This K values are being used for dense model search
    model_k_values = [1, 3, 5, 10, 100]
    # this k values are being used for scoring
    k_values = [5, 10, 100]

    mm = mmethod.split(',')
    
    # Determine index naming strategy
    use_separate_indices = "-" in index  # If index contains dash, assume subset-specific naming

    for i in range(len(mega_corpus)):
        # print(i)
        if subset and subset != 'all' and subset != data_name[i]:
            continue
        # Use subset-specific index name if pattern detected
        if use_separate_indices:
            subset_index = f"{index.split('-')[0]}-{data_name[i]}"
        else:
            subset_index = index
            
        print(f"\n=== Evaluating subset: {data_name[i]} ({i+1}/{len(data_name)}) ===")
        print(f"    Using index: {subset_index}")
        corpus = mega_corpus[i]
        queries = mega_queries[i]
        qrels = mega_qrels[i]

        if 'bm25' in mm:
            method = 'bm25'
            print('starting search method ' + method)
            os_retrival = RetrievalOpenSearch(endpoint, port,
                                              index_name=subset_index,
                                              model_id=model_id,
                                              search_method=method,
                                              pipeline_name=pipelines.split(',')[0])
            retriever = EvaluateRetrieval(os_retrival, bm25_k_values)
            result_size = max(bm25_k_values)
            results = os_retrival.search_bm25(corpus, queries, top_k=result_size)
            ndcg, _map, recall, precision = retriever.evaluate(qrels, results, k_values)
            print('--- end of results for ' + method)

        #for method in ['neural', 'hybrid']:
        for method in get_vector_methods(mm):
            #for method in ['hybrid', 'bool']:
                for pipeline in pipelines.split(','):
                    print('starting search method ' + method + " for pipeline " + pipeline)
                    os_retrival = RetrievalOpenSearch(endpoint, port,
                                                      index_name=subset_index,
                                                      model_id=model_id,
                                                      search_method=method,
                                                      pipeline_name=pipeline)
                    retriever = EvaluateRetrieval(os_retrival, model_k_values)  # or "cos_sim" for cosine similarity
                    top_k = max(model_k_values)
                    result_size = max(bm25_k_values)
                    # results = retriever.retrieve(corpus, queries)
                    all_experiments_took_time = []
                    for run in range(0, num_of_runs) :
                        results, took_time = os_retrival.search_vector(corpus, queries, top_k=top_k, result_size=result_size)
                        all_experiments_took_time.append(took_time)
                        ndcg, _map, recall, precision = retriever.evaluate(qrels, results, k_values)
                    # print('Total time: ' + str(total_time))
                    retriever.evaluate_time(all_experiments_took_time)
                    print('--- end of results for ' + method + " and pipeline " + pipeline)


def get_vector_methods(mm):
    vector_methods = []
    if 'neural' in mm:
        vector_methods.append('neural')
    if 'hybrid' in mm:
        vector_methods.append('hybrid')
    if 'bool' in mm:
        vector_methods.append('bool')
    return vector_methods


if __name__ == "__main__":
    main(sys.argv[1:])
