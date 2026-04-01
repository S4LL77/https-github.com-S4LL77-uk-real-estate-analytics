from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from ..database import execute_query

router = APIRouter()

class PropertyRequest(BaseModel):
    postcode: str  # e.g. SW1A
    property_type: str  # D, S, T, F, O
    is_new_build: bool

@router.post("/estimate")
def estimate_price(req: PropertyRequest):
    """
    A heuristic price estimator (baseline model).
    
    Instead of full Machine Learning, this queries Snowflake for identical properties
    in the same postcode sector, calculates the moving average, and adjusts based
    on the current BOE interest rate dynamically.
    
    This shows how an external service (API) can seamlessly leverage Snowflake's
    analytical compute power for real-time inference without duplicating ML pipelines.
    """
    
    # 1. Fetch exactly matching properties in the outcode
    query = """
    WITH property_history AS (
        SELECT 
            t.price_paid,
            t.date_of_transfer,
            t.boe_rate_at_sale_decimal
        FROM STG_MARTS.fct_transactions t
        JOIN STG_MARTS.dim_location l ON t.location_sk = l.location_sk
        JOIN MARTS.dim_property_scd2 p ON t.property_nk = p.property_nk
        WHERE l.outward_code ILIKE %s
          AND p.property_type ILIKE %s
          AND p.is_new_build = %s
        ORDER BY t.date_of_transfer DESC
        LIMIT 50
    )
    SELECT 
        AVG(price_paid) as avg_recent_price,
        AVG(boe_rate_at_sale_decimal) as avg_past_boe_rate
    FROM property_history
    """
    
    # Using only the first part of the postcode for regional context
    outcode = req.postcode.split()[0]
    
    try:
        results = execute_query(query, (outcode, req.property_type, req.is_new_build))
        
        if not results or not results[0]['avg_recent_price']:
            return {"error": "Not enough historical data in this postcode sector to estimate."}
            
        base_price = results[0]['avg_recent_price']
        past_rate = results[0]['avg_past_boe_rate'] or 0.05
        
        # 2. Heuristic Adjustment (Mocking ML Logic):
        # The latest Bank of England rate is retrieved to adjust the price.
        # Rule of thumb for the demonstration: if current rates are 2% higher
        # than when these houses sold, the estimated value drops by 5% because
        # borrowing is more expensive.
        
        current_rate_query = "SELECT rate_value_decimal FROM STG_STG.stg_boe_rates ORDER BY rate_date DESC LIMIT 1"
        current_rate_res = execute_query(current_rate_query)
        current_rate = current_rate_res[0]['rate_value_decimal']
        
        rate_diff = current_rate - past_rate
        
        # Adjust base price (if rate went up by 1% (0.01), drop price by 2.5%)
        adjustment_factor = 1.0 - (rate_diff * 2.5)
        estimated_price = base_price * adjustment_factor
        
        return {
            "postcode_sector": outcode,
            "property_type": req.property_type,
            "is_new_build": req.is_new_build,
            "base_recent_average": int(base_price),
            "rate_adjustment_factor": f"{100*(adjustment_factor-1):.1f}%",
            "estimated_value_gbp": int(estimated_price)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
