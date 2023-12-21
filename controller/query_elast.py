from fastapi import HTTPException

import os, re

from controller.elastic import Elastic

from dotenv import load_dotenv
load_dotenv()

class QueryElast(Elastic):
    def __init__(self):
        super().__init__()
        
        
        self.exclude_search_product_field = ['search', 'original_search', 'brand_id']
        self.search_product_field = {
            "search_field": "search",
        }
        
    def analyze_text(self, text):
        # analyze text
        analyzed_text = self.client.indices.analyze(
            index=os.getenv('ES_INDEX_SEARCH_PRODUCT'), 
            body={"analyzer": os.getenv('ES_ANALYZER_SEARCH_PRODUCT'), "text": text}
            )['tokens']
        # auto complete suggest
        body_suggest = {'suggest': {}}
        for n, text in enumerate(analyzed_text):
            body_suggest['suggest'][str(n)+'_search'] = {
                'text' : text['token'],
                'term' : {
                    "field": self.search_product_field['search_field'],
                    "size": 1,
                    "min_word_length" : 2,
                }
            }
        return self.client.search(index=os.getenv('ES_INDEX_SEARCH_PRODUCT'), body=body_suggest)['suggest']

    def search_product(self, ls_suggest, n_result=5):
        ls_text = []
        for text in ls_suggest:
            if len(ls_suggest[text][0]['options']) > 0:
                ls_text.append(ls_suggest[text][0]['options'][0]['text'])
            else:
                ls_text.append(ls_suggest[text][0]['text'])
                
        body_search_product = {
            "query": {
                "match": {
                    self.search_product_field['search_field']: { 
                        "query": ' '.join(ls_text),
                        "analyzer": os.getenv('ES_ANALYZER_SEARCH_PRODUCT')
                    }
                }
            },
            
            "_source": {
                "excludes": self.exclude_search_product_field 
            },
            
            "size": n_result
        }
        ls_product = self.client.search(index=os.getenv('ES_INDEX_SEARCH_PRODUCT'), body=body_search_product)['hits']['hits']
        return [res['_source'] for res in ls_product]
        
        
    def search_product_review(self, text, n_result=5):
        try:
            ls_suggest = self.analyze_text(text)
            result = self.search_product(ls_suggest, n_result=n_result)
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail="Internal Server Error") from e
    
    def search_review(self, ls_product):
        try:
            ls_product_id = [product['product_id'] for product in ls_product]
            ls_review_product = {}
            for product in ls_product_id:
                ls_review_product[product] = ''
            body_review_product = {
                "query": {
                    "terms": {'product_id': ls_product_id},
                },
                'size': 1000
            }
            
            reviews = self.client.search(index=os.getenv('ES_INDEX_REVIEW_PRODUCT'), body=body_review_product)['hits']['hits']
            for review in reviews:
                text = re.sub('[\r\n\t]+|<.*?>', ' ', review['_source']['comment'])
                ls_review_product[review['_source']['product_id']] += f"Point {review['_source']['point']} : {text}\n\n"
            return ls_review_product
        except Exception as e:
            raise HTTPException(status_code=500, detail="Internal Server Error") from e