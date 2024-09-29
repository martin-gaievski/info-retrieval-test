import itertools
import textwrap
from typing import Type, List, Dict, Union, Tuple
from opensearchpy import OpenSearch, RequestsHttpConnection


class OpenSearchDataIngestor:

    def __init__(self, endpoint: str, port: str, timeout: int = 30, language: str = "english"):
        self.opensearch = OpenSearch(
            hosts=[{
                'host': endpoint,
                'port': port
            }],
            use_ssl=False,
            verify_certs=False,
            connection_class=RequestsHttpConnection,
            timeout=timeout
        )
        self.bulk_size = 200
        self.max_tokens = 512
        self.language = language

    def ingest(self, corpus: Dict[str, Dict[str, str]], index: str, pipeline_name: str = 'nlp-ingest-pipeline'):
        index_settings='{"mappings":{"properties":{"passage_embedding":{"type":"knn_vector","dimension":768,"method":{"engine":"lucene","space_type":"l2","name":"hnsw","parameters":{}}},"passage_text":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"text_key":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}},"title_key":{"type":"text","fields":{"keyword":{"type":"keyword","ignore_above":256}}}}},"settings":{"index":{"knn":true,"default_pipeline":"nlp-ingest-pipeline","replication":{"type":"DOCUMENT"},"number_of_shards":"3","number_of_replicas":"1"}}}'
        self.opensearch.indices.create(index=index, body=index_settings)

        '''for i in range(0, 200, self.bulk_size):'''
        for i in range(0, len(corpus), self.bulk_size):
            key_list = itertools.islice(corpus.keys(), i, i + self.bulk_size)

            def get_doc_text(full_string: str):
                str_as_list = textwrap.wrap(full_string, self.max_tokens, break_long_words=False,
                                            break_on_hyphens=False)
                return full_string if len(str_as_list) == 0 else str_as_list[0]
                # return ' '.join(full_string.split()[:500])

            def cleanup(s):
                '''cleaned = s.replace('"', '')
                cleaned = cleaned.replace("\'", "")
                return cleaned
                '''
                return s

            def get_content(corpus_doc):
                if 'title' in corpus_doc.keys():
                    return {
                        'passage_text': cleanup(get_doc_text((corpus_doc["title"] + ' ' + corpus_doc["text"]).strip())), 'text_key': cleanup(corpus_doc['text']), 'title_key': cleanup(corpus_doc['title'])}
                else:
                    return {'passage_text': cleanup(get_doc_text((corpus_doc["text"]).strip())), 'text_key': cleanup(corpus_doc['text'])}

            actions = []
            _ = [
                actions.extend(
                    [{'index': {'_index': index, '_id': key_id}},
                     get_content(corpus[key_id])])
                for key_id in key_list
            ]
            # actions[1::2] = [{'passage_text': corpus[key_id]['text']} for key_id in key_list]
            self.opensearch.bulk(
                index=index,
                body=actions,
                pipeline=pipeline_name)

            if i % 1000 == 0:
                print("Ingested " + str(i) + " documents")
