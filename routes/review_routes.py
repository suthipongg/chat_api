from fastapi import APIRouter, HTTPException

import sys, os, re

from controller.query_elast import QueryElast
from models.review_model import ReviewModel

es = QueryElast()

review_route = APIRouter()

@review_route.post("/review/list_product")
async def ls_product(body:ReviewModel):
    try:
        text = es.analyze_text(body.text_product)
        product = es.search_product(text, body.n_result)
        return {
            "analyzed_text": text,
            'product': product,
        }
    except HTTPException as http_exception:
        raise http_exception
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(e)
        raise HTTPException(status_code=500, detail="Internal Server Error") from e


@review_route.post("/review/conclusion")
async def review(body:ReviewModel):
    try:
        product = es.search_product_review(body.text_product, body.n_result)
        review = es.search_review(product)
        for p in product:
            review[p['brand_name'] + ' > ' + p['product_name']] = review[p['product_id']]
            del review[p['product_id']]
        return {
            "review": review,
        }
    except HTTPException as http_exception:
        raise http_exception
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(e)
        raise HTTPException(status_code=500, detail="Internal Server Error") from e