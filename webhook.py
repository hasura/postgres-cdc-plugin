from fastapi import FastAPI, Request
import json
import time
app = FastAPI()

@app.post("/webhook/")
async def webhook_endpoint(request: Request):
    payload = await request.json()
    print("Received payload:", json.dumps(payload, indent=4))
    # You can add more logic here to process the payload
    return {"message": "Webhook received successfully"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
