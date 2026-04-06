from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery
from pydantic import BaseModel
from typing import Optional
from datetime import date


app = FastAPI()

PROJECT_ID = "kyabarsi-purdue-devops"
DATASET = "property_mgmt"

# ---------------------------------------------------------------------------
# POST Request Pydantic Models
# ---------------------------------------------------------------------------
class CreateIncome(BaseModel):
    amount: float
    date: date
    notes: Optional[str] = None



# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties in the database.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    properties = [dict(row) for row in results]
    return properties

@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns a specific property by its ID.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = {property_id}
    """
    try:
        results = list(bq.query(query).result())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )
    
    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )
    
    return dict(results[0])

# ---------------------------------------------------------------------------
# Income
# ---------------------------------------------------------------------------
@app.get("/income/{property_id}")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):")
    """
    Returns the income for a specific property by its ID.
    """
    query = f"""
        SELECT
            income_id,
            property_id,
            amount,
            date,
            notes
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = {property_id}
        ORDER BY date DESC
    """

    try:
        results = list(bq.query(query).result())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Income for property with ID {property_id} not found"
        )

    return [dict(row) for row in results]

@app.post("/income/{property_id}", status_code=201)
def create_income(property_id: int, body: CreateIncome, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Create a new income record for a property.
    """
    property_check = list(bq.query(f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}").result())
    if not property_check:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )
    new_income_id = list(bq.query(f"SELECT COALESCEE(MAX(income_id), 0) as new_id FROM `{PROJECT_ID}.{DATASET}.income`").result())[0]["new_id"] + 1

    notes = f'"{body.notes}"' if body.notes else "NULL"

    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.income` (income_id, property_id, amount, date, notes)
        VALUES ({new_income_id}, {property_id}, {body.amount}, "{body.date}", {notes})
    """

    try:
        bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return {
        "income_id": new_income_id,
        "property_id": property_id,
        "amount": body.amount,
        "date": body.date,
        "notes": body.notes"
    }

