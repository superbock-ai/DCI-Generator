"""
FastAPI Broker for DCI Generator
REST API interface for submitting and monitoring analysis jobs.
"""

import os
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import jwt
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI(
    title="DCI Generator API",
    description="REST API for submitting and monitoring insurance document analysis jobs",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security scheme
security = HTTPBearer()

# Configuration
DIRECTUS_SECRET = os.getenv("DIRECTUS_SECRET")
if not DIRECTUS_SECRET:
    raise ValueError("DIRECTUS_SECRET environment variable is required")


# Pydantic models for request/response
class AnalysisJobRequest(BaseModel):
    """Request model for analysis job submission."""
    product_id: str = Field(..., description="Product ID from Directus to analyze")
    export: bool = Field(False, description="Export results to JSON file")
    detailed: bool = Field(False, description="Show detailed results")
    no_cache: bool = Field(False, description="Disable caching for this run")
    segment_chunks: int = Field(8, ge=1, le=20, description="Number of segments to process per chunk")
    benefit_chunks: int = Field(8, ge=1, le=20, description="Number of benefits to process per chunk")
    detail_chunks: int = Field(8, ge=1, le=10, description="Number of details to process per chunk")
    debug: bool = Field(False, description="Enable debug mode")
    debug_clean: bool = Field(False, description="Clean debug files before running")
    debug_from: Optional[str] = Field(None, description="Force re-run from specific tier")
    seed_directus: bool = Field(False, description="Seed results to Directus")
    dry_run_directus: bool = Field(False, description="Dry run mode for Directus seeding")


class CleanupJobRequest(BaseModel):
    """Request model for cleanup job submission."""
    product_id: str = Field(..., description="Product ID to clean up")


class JobResponse(BaseModel):
    """Response model for job submission."""
    job_id: str = Field(..., description="Unique job identifier")
    status: str = Field(..., description="Job status")
    message: str = Field(..., description="Status message")


class JobStatusResponse(BaseModel):
    """Response model for job status queries."""
    job_id: str = Field(..., description="Job identifier")
    status: str = Field(..., description="Current job status")
    result: Optional[Dict[str, Any]] = Field(None, description="Job results if completed")
    error: Optional[str] = Field(None, description="Error message if failed")
    progress: Optional[Dict[str, Any]] = Field(None, description="Progress information")


# JWT Authentication
def verify_jwt_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    """
    Verify JWT token using Directus secret.
    """
    try:
        token = credentials.credentials

        # Decode JWT token
        payload = jwt.decode(token, DIRECTUS_SECRET, algorithms=["HS256"])

        return payload

    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired"
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )


# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "dci-generator-api",
        "version": "1.0.0"
    }


# Connect to Celery
def get_celery_app():
    """Get Celery app instance for task submission."""
    import sys

    # Add worker directory to Python path
    worker_path = os.path.join(os.path.dirname(__file__), 'worker')
    if worker_path not in sys.path:
        sys.path.insert(0, worker_path)

    from celery_app import app as celery_app
    return celery_app


# Job submission endpoints
@app.post("/jobs/analysis", response_model=JobResponse)
async def submit_analysis_job(
    job_request: AnalysisJobRequest,
    user: Dict[str, Any] = Depends(verify_jwt_token)
):
    """
    Submit an analysis job for processing.
    """
    try:
        # Import tasks dynamically to avoid circular imports
        import sys

        # Add worker directory to Python path
        worker_path = os.path.join(os.path.dirname(__file__), 'worker')
        if worker_path not in sys.path:
            sys.path.insert(0, worker_path)

        from tasks import analyze_document_task

        # Submit task to Celery
        result = analyze_document_task.delay(**job_request.model_dump())

        return JobResponse(
            job_id=result.id,
            status="submitted",
            message=f"Analysis job submitted for product {job_request.product_id}"
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to submit analysis job: {str(e)}"
        )


@app.post("/jobs/cleanup", response_model=JobResponse)
async def submit_cleanup_job(
    job_request: CleanupJobRequest,
    user: Dict[str, Any] = Depends(verify_jwt_token)
):
    """
    Submit a cleanup job for processing.
    """
    try:
        # Import tasks dynamically to avoid circular imports
        import sys

        # Add worker directory to Python path
        worker_path = os.path.join(os.path.dirname(__file__), 'worker')
        if worker_path not in sys.path:
            sys.path.insert(0, worker_path)

        from tasks import cleanup_product_task

        # Submit task to Celery
        result = cleanup_product_task.delay(job_request.product_id)

        return JobResponse(
            job_id=result.id,
            status="submitted",
            message=f"Cleanup job submitted for product {job_request.product_id}"
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to submit cleanup job: {str(e)}"
        )


@app.get("/jobs/{job_id}/status", response_model=JobStatusResponse)
async def get_job_status(
    job_id: str,
    user: Dict[str, Any] = Depends(verify_jwt_token)
):
    """
    Get the status of a submitted job.
    """
    try:
        # Import Celery app and result
        celery_app = get_celery_app()
        result = celery_app.AsyncResult(job_id)

        # Get job status
        status = result.status
        job_result = None
        error = None
        progress = None

        if status == "SUCCESS":
            job_result = result.result
        elif status == "FAILURE":
            error = str(result.info) if result.info else "Job failed with unknown error"
        elif status == "PROCESSING":
            # Try to get progress information
            try:
                progress = result.info
            except:
                progress = {"status": "Processing"}

        return JobStatusResponse(
            job_id=job_id,
            status=status.lower(),
            result=job_result,
            error=error,
            progress=progress
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get job status: {str(e)}"
        )


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "message": "DCI Generator API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }