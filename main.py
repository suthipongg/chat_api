from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.requests import Request
import uvicorn

from datetime import datetime
from time import time
import os
from dotenv import load_dotenv
load_dotenv('configs/.env')
env = os.environ

from configs.middleware import log_request_middleware
from routes.review_routes import review_route
from utils.logging import configure_logging

ALLOWED_ORIGINS = ['*']

app = FastAPI(
    title=env["TITLE"], 
    description=f"Created by Music \n TNT Media and Network Co., Ltd. \n Started at {datetime.now().strftime('%c')}",
    docs_url="/",
    version="1.0.0",
    )

app.add_middleware(  
    CORSMiddleware,  
    allow_origins=ALLOWED_ORIGINS,  # Allows CORS for this specific origin  
    allow_credentials=True,  
    allow_methods=["*"],  # Allows all methods  
    allow_headers=["*"],  # Allows all headers  
)  

app.middleware("http")(log_request_middleware)
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    start_time = time()
    response = await call_next(request)
    process_time = (time() - start_time) * 1000
    response.headers["X-Process-Time"] = "{0:.2f}ms".format(process_time)
    return response


if int(env["LOGGING"]):
    configure_logging()
    
app.include_router(review_route, prefix="/chat", tags=["Review"], responses={404: {"description": "Not found"}})

if __name__ == "__main__":
    uvicorn.run("main:app", 
                host=env["HOST"], 
                port=int(env["PORT"]), 
                log_level="info", 
                reload=True
                )