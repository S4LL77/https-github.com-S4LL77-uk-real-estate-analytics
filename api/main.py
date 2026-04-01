from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Future-proofing: We structure the API logically using routers 
# rather than throwing everything in one file.
from .routers import analytics, predict

app = FastAPI(
    title="UK Real Estate Analytics API",
    description="A demonstration FastAPI layer sitting on top of a highly optimized Snowflake Medallion data warehouse.",
    version="1.0.0",
)

# CORS ensures that web applications running on different ports (e.g., React on 3000)
# or external Tableau dashboards can query this API.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(analytics.router, prefix="/analytics", tags=["analytics"])
app.include_router(predict.router, prefix="/predict", tags=["prediction"])

@app.get("/")
def health_check():
    """Simple check to ensure the server is responding."""
    return {"status": "ok", "message": "UK Real Estate API is running."}
