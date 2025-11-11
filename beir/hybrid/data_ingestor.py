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
        self.bulk_size = 400
        self.max_tokens = 512
        self.language = language

    def ingest(self, corpus: Dict[str, Dict[str, str]], index: str, start_position: int = 1):
        '''
        Ingest corpus documents into OpenSearch index.
        
        Args:
            corpus: Dictionary of documents to ingest
            index: OpenSearch index name
            start_position: Document position to start from (1-indexed, default=1)
        '''
        # Convert 1-indexed start_position to 0-indexed
        start_index = max(0, start_position - 1)
        
        # If starting from a position other than the beginning, log it
        if start_index > 0:
            print(f"Starting ingestion from document position {start_position} (skipping first {start_index} documents)")
        
        # Calculate the starting batch position
        start_batch = (start_index // self.bulk_size) * self.bulk_size
        
        '''for i in range(0, 200, self.bulk_size):'''
        for i in range(start_batch, len(corpus), self.bulk_size):
            # Skip documents before start_index within the first batch
            batch_start = max(i, start_index)
            batch_end = min(i + self.bulk_size, len(corpus))
            
            # If this batch is entirely before start_index, skip it
            if batch_end <= start_index:
                continue
                
            key_list = itertools.islice(corpus.keys(), batch_start, batch_end)

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
                    '''try:
                        pubmed_id = int(corpus_doc['metadata']['pubmed_id']) if corpus_doc['metadata']['pubmed_id'] else None
                    except (KeyError, ValueError, TypeError):
                        pubmed_id = None
                    return {
                        'passage_text': cleanup(get_doc_text((corpus_doc["title"] + ' ' + corpus_doc["text"]).strip())), 
                        'text_key': cleanup(corpus_doc['text']), 
                        'title_key': cleanup(corpus_doc['title']),
                        'url': corpus_doc['metadata']['url'],
                        'pubmed_id': pubmed_id
                        }
                    '''
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
                body=actions)

            # Adjust progress message to account for skipped documents
            total_ingested = max(0, i - start_index)
            if i % 1000 == 0 and i > 0:
                print(f"Ingested {total_ingested} documents (total processed: {i})")
