"""
FastAPI application for Flink Resource Estimator.

This module contains the web application setup, API endpoints,
and HTML page serving logic.
"""

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from typing import Annotated
import json
import os
from datetime import datetime

# Import from our custom modules
from .models import EstimationInput, SavedEstimation
from .estimation import (
    calculate_flink_estimation,
    save_estimation_to_json,
    get_saved_estimations_directory
)

app = FastAPI(
    title="Flink Resource Estimator", 
    description="Tool to estimate Flink cluster resources based on workload parameters"
)

# Mount static files and templates
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Serve the main estimation form."""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/saved", response_class=HTMLResponse)
async def saved_estimations_page(request: Request):
    """Serve the saved estimations page."""
    return templates.TemplateResponse("saved.html", {"request": request})


@app.post("/estimate", response_class=HTMLResponse)
async def estimate_resources(
    request: Request,
    project_name: Annotated[str, Form()],
    messages_per_second: Annotated[int, Form()],
    avg_record_size_bytes: Annotated[int, Form()],
    num_distinct_keys: Annotated[int, Form()] = 100000,
    data_skew_risk: Annotated[str, Form()] = "medium",
    bandwidth_capacity_mbps: Annotated[int, Form()] = 1000,
    simple_statements: Annotated[int, Form()] = 0,
    medium_statements: Annotated[int, Form()] = 0,
    complex_statements: Annotated[int, Form()] = 0
):
    """Process the estimation request and return results."""
    
    try:
        # Create EstimationInput from form data
        input_params = EstimationInput(
            project_name=project_name,
            messages_per_second=messages_per_second,
            avg_record_size_bytes=avg_record_size_bytes,
            num_distinct_keys=num_distinct_keys,
            data_skew_risk=data_skew_risk,
            bandwidth_capacity_mbps=bandwidth_capacity_mbps,
            simple_statements=simple_statements,
            medium_statements=medium_statements,
            complex_statements=complex_statements
        )
        
        # Calculate estimation using Pydantic models
        estimation_result = calculate_flink_estimation(input_params)
        
        return templates.TemplateResponse(
            "results.html", 
            {
                "request": request,
                "project_name": project_name,
                "estimation": estimation_result,
                "success": True
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "results.html",
            {
                "request": request,
                "project_name": project_name if 'project_name' in locals() else "Unknown",
                "error": str(e),
                "success": False
            }
        )


@app.get("/api/estimate")
async def api_estimate(
    project_name: str,
    messages_per_second: int,
    avg_record_size_bytes: int,
    num_distinct_keys: int = 100000,
    data_skew_risk: str = "medium",
    bandwidth_capacity_mbps: int = 1000,
    simple_statements: int = 0,
    medium_statements: int = 0,
    complex_statements: int = 0
):
    """API endpoint for programmatic access to estimation via query parameters."""
    try:
        input_params = EstimationInput(
            project_name=project_name,
            messages_per_second=messages_per_second,
            avg_record_size_bytes=avg_record_size_bytes,
            num_distinct_keys=num_distinct_keys,
            data_skew_risk=data_skew_risk,
            bandwidth_capacity_mbps=bandwidth_capacity_mbps,
            simple_statements=simple_statements,
            medium_statements=medium_statements,
            complex_statements=complex_statements
        )
        return calculate_flink_estimation(input_params)
    except Exception as e:
        return JSONResponse({
            "error": str(e),
            "message": "Invalid input parameters"
        }, status_code=400)


@app.post("/api/estimate")
async def api_estimate_post(input_params: EstimationInput):
    """API endpoint for programmatic access to estimation via JSON."""
    try:
        return calculate_flink_estimation(input_params)
    except Exception as e:
        return JSONResponse({
            "error": str(e),
            "message": "Failed to calculate estimation"
        }, status_code=500)


@app.post("/save-estimation")
async def save_estimation(
    project_name: Annotated[str, Form()],
    messages_per_second: Annotated[int, Form()],
    avg_record_size_bytes: Annotated[int, Form()],
    num_distinct_keys: Annotated[int, Form()] = 100000,
    data_skew_risk: Annotated[str, Form()] = "medium",
    bandwidth_capacity_mbps: Annotated[int, Form()] = 1000,
    simple_statements: Annotated[int, Form()] = 0,
    medium_statements: Annotated[int, Form()] = 0,
    complex_statements: Annotated[int, Form()] = 0
):
    """Save estimation results to JSON file."""
    try:
        # Create EstimationInput from form data
        input_params = EstimationInput(
            project_name=project_name,
            messages_per_second=messages_per_second,
            avg_record_size_bytes=avg_record_size_bytes,
            num_distinct_keys=num_distinct_keys,
            data_skew_risk=data_skew_risk,
            bandwidth_capacity_mbps=bandwidth_capacity_mbps,
            simple_statements=simple_statements,
            medium_statements=medium_statements,
            complex_statements=complex_statements
        )
        
        # Calculate estimation using Pydantic models
        estimation_result = calculate_flink_estimation(input_params)
        
        # Save to JSON using Pydantic models
        filename = save_estimation_to_json(input_params, estimation_result)
        
        return JSONResponse({
            "success": True,
            "message": f"Estimation saved successfully as {filename}",
            "filename": filename
        })
        
    except Exception as e:
        return JSONResponse({
            "success": False,
            "message": f"Error saving estimation: {str(e)}"
        }, status_code=500)


@app.post("/api/save-estimation")
async def api_save_estimation(input_params: EstimationInput):
    """API endpoint to save estimation via JSON."""
    try:
        # Calculate estimation using Pydantic models
        estimation_result = calculate_flink_estimation(input_params)
        
        # Save to JSON using Pydantic models
        filename = save_estimation_to_json(input_params, estimation_result)
        
        return JSONResponse({
            "success": True,
            "message": f"Estimation saved successfully as {filename}",
            "filename": filename
        })
        
    except Exception as e:
        return JSONResponse({
            "success": False,
            "message": f"Error saving estimation: {str(e)}"
        }, status_code=500)


@app.get("/download/{filename}")
async def download_estimation(filename: str):
    """Download a saved estimation JSON file."""
    saved_dir = get_saved_estimations_directory()
    filepath = os.path.join(saved_dir, filename)
    
    if not os.path.exists(filepath):
        return JSONResponse({
            "error": "File not found"
        }, status_code=404)
    
    return FileResponse(
        filepath,
        media_type="application/json",
        filename=filename
    )


@app.get("/reload/{filename}", response_class=HTMLResponse)
async def reload_estimation(request: Request, filename: str):
    """Reload a saved estimation and display the results page."""
    saved_dir = get_saved_estimations_directory()
    filepath = os.path.join(saved_dir, filename)
    
    if not os.path.exists(filepath):
        return templates.TemplateResponse(
            "results.html",
            {
                "request": request,
                "project_name": "Unknown",
                "error": f"Estimation file '{filename}' not found",
                "success": False
            }
        )
    
    try:
        # Load the saved estimation file
        with open(filepath, 'r', encoding='utf-8') as f:
            saved_data = json.load(f)
        
        # Parse the saved estimation using Pydantic models
        saved_estimation = SavedEstimation(**saved_data)
        
        return templates.TemplateResponse(
            "results.html",
            {
                "request": request,
                "project_name": saved_estimation.input_parameters.project_name,
                "estimation": saved_estimation.estimation_results,
                "success": True,
                "is_reloaded": True,
                "saved_filename": filename,
                "saved_at": saved_estimation.metadata.saved_at
            }
        )
        
    except Exception as e:
        return templates.TemplateResponse(
            "results.html",
            {
                "request": request,
                "project_name": "Unknown",
                "error": f"Error loading estimation: {str(e)}",
                "success": False
            }
        )


@app.get("/saved-estimations")
async def list_saved_estimations():
    """List all saved estimation files."""
    try:
        saved_dir = get_saved_estimations_directory()
        files = []
        for filename in os.listdir(saved_dir):
            if filename.endswith('.json'):
                filepath = os.path.join(saved_dir, filename)
                stat = os.stat(filepath)
                
                # Try to read metadata from file
                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        project_name = data.get('metadata', {}).get('project_name', 'Unknown')
                        saved_at = data.get('metadata', {}).get('saved_at', 'Unknown')
                except:
                    project_name = 'Unknown'
                    saved_at = 'Unknown'
                
                files.append({
                    "filename": filename,
                    "project_name": project_name,
                    "saved_at": saved_at,
                    "size_bytes": stat.st_size,
                    "modified_time": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
                })
        
        # Sort by modification time (newest first)
        files.sort(key=lambda x: x['modified_time'], reverse=True)
        
        return JSONResponse({
            "success": True,
            "files": files,
            "count": len(files)
        })
        
    except Exception as e:
        return JSONResponse({
            "success": False,
            "message": f"Error listing files: {str(e)}"
        }, status_code=500)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 