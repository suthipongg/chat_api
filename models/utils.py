from fastapi import HTTPException

class ManageBody:
    def strip_str(self, value:str, key) -> str:
        if type(value) == str:
            self.__dict__[key] = value.strip()
    
    def check_is_empty(self, value:str, key, keys) -> str:
        if type(value) == str and value == "" and key in keys:
            raise HTTPException(status_code=400, detail=f"{key} not provided.")
    
    def clean_model(self, keys:list = []):
        for key, value in self.__dict__.items():
            self.strip_str(value, key)
            self.check_is_empty(value, key, keys)