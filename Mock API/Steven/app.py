from fastapi import FastAPI

app = FastAPI()

@app.get("/predict")

def predict_model (age: int, sex:str):

    if age <= 17 and sex.upper() == "MALE":
        return {"Boy"}
    elif age >= 18 and sex.upper() == "MALE":
        return {"Man"}
    elif age <= 17 and sex.upper() == "FEMALE":
        return {"Girl"}
    elif age >= 18 and sex.upper() == "FEMALE":
        return {"Woman"}
    else:
        return {"Please provide the parameters age and sex"}
    
#python3 -m uvicorn app:app --reload