from models.review_model import ReviewModel
from fastapi import APIRouter, HTTPException
import sys, os

review_route = APIRouter()

@review_route.post("/review/conclusion")
async def review(body:ReviewModel):
    try:
        return {
            "message": body.text_product,
        }
    except HTTPException as http_exception:
        raise http_exception
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(e)
        raise HTTPException(status_code=500, detail="Internal Server Error") from e