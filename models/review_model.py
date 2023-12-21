from pydantic import BaseModel, Field
from datetime import datetime
from models.utils import ManageBody

class ReviewModel(BaseModel, ManageBody):
    text_product : str = Field(..., description="text_product (**required)")
    n_result : int = Field(5, description="n_result (**optional)")
    created_at : str = ""
    
    def __init__(self, **data):
        super().__init__(**data)
        if not self.created_at:
            self.created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        self.clean_model(["text_product"])

    class Config:
        json_schema_extra = {
            "example": {
                "text_product": "test",
                'n_result': 5,
            }
        }