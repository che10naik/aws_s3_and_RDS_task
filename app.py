from datetime import date
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from rds_handler import RDSTableHandler

app = FastAPI(title="Employees API")

# Pydantic Models
class EmployeeIn(BaseModel):
    """Input model for creating/updating an employee"""
    name: str
    salary: float
    salary_date: date

class EmployeeOut(EmployeeIn):
    """Output model for employee with ID"""
    id: int

# Database Setup
handler = RDSTableHandler()
handler.create_table_if_not_exists()  # Ensure table exists

# FastAPI Endpoints (CRUD)

@app.get("/items", response_model=list[EmployeeOut])
def list_items():
    """Retrieve all employee records"""
    return handler.get_all()

@app.get("/items/{item_id}", response_model=EmployeeOut)
def get_item(item_id: int):
    """Retrieve a single employee by ID"""
    emp = handler.get_by_id(item_id)
    if not emp:
        raise HTTPException(status_code=404, detail="Employee not found")
    return emp

@app.post("/items", response_model=EmployeeOut)
def create_item(payload: EmployeeIn):
    """Create a new employee record"""
    return handler.create(payload.dict())

@app.put("/items/{item_id}", response_model=EmployeeOut)
def update_item(item_id: int, payload: EmployeeIn):
    """Update an existing employee record"""
    updated = handler.update(item_id, payload.dict())
    if not updated:
        raise HTTPException(status_code=404, detail="Employee not found")
    return updated

@app.delete("/items/{item_id}")
def delete_item(item_id: int):
    """Delete an employee record"""
    success = handler.delete(item_id)
    if not success:
        raise HTTPException(status_code=404, detail="Employee not found")
    return {"deleted": success}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)
