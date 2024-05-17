from fastapi import FastAPI
import asyncio
from kafka_configuration import start_consumers  # Only import the necessary function

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    # Start only the image processing task, which will handle fetching metadata internally
    print("Starting the FastAPI application...")
    loop = asyncio.get_event_loop()
    loop.create_task(start_consumers())
    print("FastAPI application started.")

@app.get("/")
def read_root():
    return {"message": "Metadata extraction and DB writing service is running"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
