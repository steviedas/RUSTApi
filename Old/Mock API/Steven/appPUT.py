from fastapi import FastAPI
from pydantic import BaseModel

class Input(BaseModel):
    age: int
    sex: str

app = FastAPI()
@app.put("/predict")

def predict_model(d:Input):
    age = d.age
    sex = d.sex.upper()

    if age <= 17 and sex == "MALE":
        return {"Boy"}
    elif age >= 18 and sex == "MALE":
        return {"Man"}
    elif age <= 17 and sex == "FEMALE":
        return {"Girl"}
    elif age >= 18 and sex == "FEMALE":
        return {"Woman"}
    else:
        return {"Please provide the parameters age and sex"}
    
#python3 -m uvicorn appPUT:app --reload