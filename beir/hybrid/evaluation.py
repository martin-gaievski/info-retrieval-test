import pytrec_eval
import logging
from typing import List, Dict, Tuple, Any
from math import floor
import numpy as np

logger = logging.getLogger(__name__)


class EvaluateRetrieval:

    def __init__(self, retriever,
                 k_values: List[int] = [1, 3, 5, 10, 100, 1000]):
        self.k_values = k_values
        self.top_k = max(k_values)
        self.retriever = retriever
    
    def calculate_ndcg(self, results: Dict[str, float], qrels: Dict[str, int], k: int = 10) -> float:
        """Calculate NDCG@k for search results."""
        if not results or not qrels:
            return 0.0
        
        # Get top-k results
        sorted_results = sorted(results.items(), key=lambda x: x[1], reverse=True)[:k]
        
        # Calculate DCG
        dcg = 0.0
        for i, (doc_id, score) in enumerate(sorted_results):
            relevance = qrels.get(doc_id, 0)
            if relevance > 0:
                dcg += (2**relevance - 1) / np.log2(i + 2)
        
        # Calculate IDCG (ideal DCG)
        ideal_relevances = sorted(qrels.values(), reverse=True)[:k]
        idcg = 0.0
        for i, relevance in enumerate(ideal_relevances):
            if relevance > 0:
                idcg += (2**relevance - 1) / np.log2(i + 2)
        
        # Return NDCG
        return dcg / idcg if idcg > 0 else 0.0

    @staticmethod
    def evaluate(qrels: Dict[str, Dict[str, int]],
                results: Dict[str, Dict[str, float]],
                k_values: List[int],
                ignore_identical_ids: bool = True) -> Tuple[
        Dict[str, float], Dict[str, float], Dict[str, float], Dict[str, float]]:
        """
        Evaluate search results using multiple metrics.
        
        Args:
            qrels: Dictionary of query relevance scores {query_id: {doc_id: relevance_score}}
            results: Dictionary of search results {query_id: {doc_id: score}}
            k_values: List of k values for @k metrics
            ignore_identical_ids: Whether to ignore documents with same ID as query
        """
        
        if ignore_identical_ids:
            logging.info(
                'For evaluation, we ignore identical query and document ids (default), please explicitly set ``ignore_identical_ids=False`` to ignore this.')
            popped = []
            for qid, rels in results.items():
                for pid in list(rels):
                    if qid == pid:
                        results[qid].pop(pid)
                        popped.append(pid)

        ndcg = {}
        _map = {}
        recall = {}
        precision = {}

        # Initialize metric dictionaries
        for k in k_values:
            ndcg[f"NDCG@{k}"] = 0.0
            _map[f"MAP@{k}"] = 0.0
            recall[f"Recall@{k}"] = 0.0
            precision[f"P@{k}"] = 0.0

        # Prepare evaluation strings
        map_string = "map_cut." + ",".join([str(k) for k in k_values])
        ndcg_string = "ndcg_cut." + ",".join([str(k) for k in k_values])
        recall_string = "recall." + ",".join([str(k) for k in k_values])
        precision_string = "P." + ",".join([str(k) for k in k_values])
        
        evaluator = pytrec_eval.RelevanceEvaluator(qrels, {map_string, ndcg_string, recall_string, precision_string})
        scores = evaluator.evaluate(results)

        # Calculate average metrics for each k
        k_metrics = {
            'ndcg': {k: [] for k in k_values},
            'map': {k: [] for k in k_values},
            'recall': {k: [] for k in k_values},
            'precision': {k: [] for k in k_values}
        }

        # Accumulate scores per k value
        for query_id in scores.keys():
            for k in k_values:
                k_metrics['ndcg'][k].append(scores[query_id]["ndcg_cut_" + str(k)])
                k_metrics['map'][k].append(scores[query_id]["map_cut_" + str(k)])
                k_metrics['recall'][k].append(scores[query_id]["recall_" + str(k)])
                k_metrics['precision'][k].append(scores[query_id]["P_" + str(k)])

        # Calculate average scores for each k
        for k in k_values:
            ndcg[f"NDCG@{k}"] = round(np.mean(k_metrics['ndcg'][k]), 5)
            _map[f"MAP@{k}"] = round(np.mean(k_metrics['map'][k]), 5)
            recall[f"Recall@{k}"] = round(np.mean(k_metrics['recall'][k]), 5)
            precision[f"P@{k}"] = round(np.mean(k_metrics['precision'][k]), 5)

        # Calculate mean and median using the @k values
        for metric_name, metric_dict, scores_key in [
            ('NDCG', ndcg, 'ndcg'),
            ('MAP', _map, 'map'),
            ('Recall', recall, 'recall'),
            ('P', precision, 'precision')
        ]:
            # Calculate mean and median across k values
            k_averages = [metric_dict[f"{metric_name}@{k}"] for k in k_values]
            metric_dict[f"{metric_name}_mean"] = round(np.mean(k_averages), 5)
            metric_dict[f"{metric_name}_median"] = round(np.median(k_averages), 5)

        # Log results
        for eval_metric in [ndcg, _map, recall, precision]:
            logging.info("\n")
            for k in eval_metric.keys():
                logging.info("{}: {:.4f}".format(k, eval_metric[k]))

        return ndcg, _map, recall, precision

    def _pxx(self, values: List[Any], p: float):
        """Calculates the pXX statistics for a given list.

        Args:
            values: List of values.
            p: Percentile (between 0 and 1).

        Returns:
            The corresponding pXX metric.
        """
        lowest_percentile = 1 / len(values)
        highest_percentile = (len(values) - 1) / len(values)

        # return -1 if p is out of range or if the list doesn't have enough elements
        # to support the specified percentile
        if p < 0 or p > 1:
            return -1.0
        elif p < lowest_percentile or p > highest_percentile:
            if p == 1.0 and len(values) > 1:
                return float(values[len(values) - 1])
            return -1.0
        else:
            return float(values[floor(len(values) * p)])

    def evaluate_time(self, took_time_all_measures):
        p50 = []
        p90 = []
        p99 = []

        for took_time in took_time_all_measures:
            times = list(took_time.values())

            p50.append(np.percentile(np.array(times), 50))
            p90.append(np.percentile(np.array(times), 90))
            p99.append(np.percentile(np.array(times), 99))

        print('p50: ' + str(np.average(p50)))
        print('p90: ' + str(np.average(p90)))
        print('p99: ' + str(np.average(p99)))

        return p50, p90, p99
