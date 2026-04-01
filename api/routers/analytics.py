from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from ..database import execute_query

router = APIRouter()

@router.get("/median-price")
def get_median_price(
    county: str = Query(..., description="The county to filter by (e.g., GREATER LONDON)"),
    property_type: Optional[str] = Query(None, description="Filter by D, S, T, F, or O"),
    year: int = Query(2024, ge=1995, le=2025, description="The year to analyze")
):
    """
    Retrieve the exact median house price for a specific region and time period.
    Demonstrates analytical querying directly from the Snowflake Gold Mart.
    """
    
    # In a real app we'd use SQLAlchemy or parameter binding strictly.
    # The snowflake-connector-python parameterized query style is %s (or qmark based on config).
    # It safely escapes parameters to prevent SQL injection.
    
    base_query = """
        SELECT 
            MEDIAN(t.price_paid) as median_price,
            COUNT(1) as total_sales
        FROM STG_MARTS.fct_transactions t
        JOIN STG_MARTS.dim_location l ON t.location_sk = l.location_sk
        JOIN MARTS.dim_property_scd2 p ON t.property_nk = p.property_nk
        WHERE l.county ILIKE %s
          AND YEAR(t.date_of_transfer) = %s
    """
    params = [county, year]
    
    if property_type:
        base_query += " AND p.property_type ILIKE %s"
        params.append(property_type)
        
    try:
        results = execute_query(base_query, tuple(params))
        
        if not results or results[0].get('median_price') is None:
            return {"median_price": None, "message": "No data found for this segment."}
            
        # Format the decimal to a clean integer since we're talking about house prices
        return {
            "county": county,
            "year": year,
            "property_type": property_type or "All",
            "median_price_gbp": int(results[0]['median_price']),
            "total_sales": results[0]['total_sales']
        }
    except Exception as e:
        # In production this would be logged, not returned to the client
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
