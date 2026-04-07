from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import bigquery
from pydantic import BaseModel
from typing import Optional
from datetime import date


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

PROJECT_ID = "kyabarsi-purdue-devops"
DATASET = "property_mgmt"

# ---------------------------------------------------------------------------
# POST Request Pydantic Models
# ---------------------------------------------------------------------------
class CreateIncome(BaseModel):
    amount: float
    date: date
    notes: Optional[str] = None

class CreateExpense(BaseModel):
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
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
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

    new_income_id = list(bq.query(f"SELECT COALESCE(MAX(income_id), 0) as new_id FROM `{PROJECT_ID}.{DATASET}.income`").result())[0]["new_id"] + 1

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
        "notes": body.notes
    }


# ---------------------------------------------------------------------------
# Expenses
# ---------------------------------------------------------------------------

@app.get("/expenses/{property_id}")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns the expenses for a specific property by its ID.
    """
    query = f"""
        SELECT
            expense_id,
            property_id,
            amount,
            date,
            notes
        FROM `{PROJECT_ID}.{DATASET}.expenses`
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
            detail=f"Expenses for property with ID {property_id} not found"
        )

    return [dict(row) for row in results]

@app.post("/expenses/{property_id}", status_code=201)
def create_expense(property_id: int, body: CreateExpense, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Create a new expense record for a property.
    """
    property_check = list(bq.query(f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}").result())
    if not property_check:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )

    new_expense_id = list(bq.query(f"SELECT COALESCE(MAX(expense_id), 0) as new_id FROM `{PROJECT_ID}.{DATASET}.expenses`").result())[0]["new_id"] + 1

    notes = f'"{body.notes}"' if body.notes else "NULL"

    query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.expenses` (expense_id, property_id, amount, date, notes)
        VALUES ({new_expense_id}, {property_id}, {body.amount}, "{body.date}", {notes})
    """

    try:
        bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return {
        "expense_id": new_expense_id,
        "property_id": property_id,
        "amount": body.amount,
        "date": body.date,
        "notes": body.notes
    }


# ---------------------------------------------------------------------------
# Additional Endpoints
# ---------------------------------------------------------------------------

@app.get("/properties/{property_id}/summary")
def get_property_summary(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns total income, total expenses, and net cashflow for a property.
    """
    property_check = list(bq.query(f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}").result())
    if not property_check:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )

    query = f"""
        SELECT
            COALESCE((SELECT SUM(amount) FROM `{PROJECT_ID}.{DATASET}.income` WHERE property_id = {property_id}), 0) AS total_income,
            COALESCE((SELECT SUM(amount) FROM `{PROJECT_ID}.{DATASET}.expenses` WHERE property_id = {property_id}), 0) AS total_expenses
    """

    try:
        result = list(bq.query(query).result())[0]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    total_income = result["total_income"]
    total_expenses = result["total_expenses"]

    return {
        "property_id": property_id,
        "total_income": total_income,
        "total_expenses": total_expenses,
        "net_cashflow": total_income - total_expenses
    }

@app.get("/properties/{property_id}/cashflow")
def get_cashflow(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns a month-by-month cashflow breakdown for a property.
    """
    property_check = list(bq.query(f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}").result())
    if not property_check:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )

    query = f"""
        WITH monthly_income AS (
            SELECT FORMAT_DATE('%Y-%m', date) AS month, SUM(amount) AS income
            FROM `{PROJECT_ID}.{DATASET}.income`
            WHERE property_id = {property_id}
            GROUP BY month
        ),
        monthly_expenses AS (
            SELECT FORMAT_DATE('%Y-%m', date) AS month, SUM(amount) AS expenses
            FROM `{PROJECT_ID}.{DATASET}.expenses`
            WHERE property_id = {property_id}
            GROUP BY month
        )
        SELECT
            COALESCE(i.month, e.month) AS month,
            COALESCE(i.income, 0) AS income,
            COALESCE(e.expenses, 0) AS expenses,
            COALESCE(i.income, 0) - COALESCE(e.expenses, 0) AS net
        FROM monthly_income i
        FULL OUTER JOIN monthly_expenses e ON i.month = e.month
        ORDER BY month DESC
    """

    try:
        results = list(bq.query(query).result())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row) for row in results]

@app.get("/income/{property_id}/total")
def get_income_total(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns the total income amount for a property.
    """
    property_check = list(bq.query(f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}").result())
    if not property_check:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )

    query = f"""
        SELECT COALESCE(SUM(amount), 0) AS total_income
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = {property_id}
    """

    try:
        result = list(bq.query(query).result())[0]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return {"property_id": property_id, "total_income": result["total_income"]}

@app.get("/expenses/{property_id}/total")
def get_expenses_total(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns the total expenses amount for a property.
    """
    property_check = list(bq.query(f"SELECT property_id FROM `{PROJECT_ID}.{DATASET}.properties` WHERE property_id = {property_id}").result())
    if not property_check:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Property with ID {property_id} not found"
        )

    query = f"""
        SELECT COALESCE(SUM(amount), 0) AS total_expenses
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = {property_id}
    """

    try:
        result = list(bq.query(query).result())[0]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return {"property_id": property_id, "total_expenses": result["total_expenses"]}