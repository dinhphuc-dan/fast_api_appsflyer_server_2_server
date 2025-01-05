from typing import Annotated
from fastapi import Depends, FastAPI, status, Body, Header, Cookie, Path, Query
from pydantic import BaseModel

app = FastAPI()

@app.post("/data", status_code=status.HTTP_200_OK)
async def receive_realtime_message(message: Annotated[dict, Body()]):
    print(message)
    return "OK"
