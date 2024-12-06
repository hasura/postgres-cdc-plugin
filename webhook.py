from fastapi import FastAPI, Request, HTTPException, Security
from fastapi.security import APIKeyHeader
import json

app = FastAPI()

# In production, use environment variables
API_KEY = "your-secret-key-here"
api_key_header = APIKeyHeader(name="X-API-Key")


async def get_api_key(api_key: str = Security(api_key_header)):
    if api_key != API_KEY:
        raise HTTPException(
            status_code=403,
            detail="Invalid API key"
        )
    return api_key


@app.post("/webhook/")
async def webhook_endpoint(
        request: Request,
        api_key: str = Security(get_api_key)
):
    payload = await request.json()
    print("Received payload:", json.dumps(payload, indent=4))
    return {"message": "Webhook received successfully"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
