import fastapi
import uvicorn


app = fastapi.FastAPI()

@app.get('/')
def home():
    return "welcome to this app sujaaata!!"

@app.get('/health')
def health():
    return None

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=3000, reload=True)