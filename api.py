from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBearer

from RBAC.rbac import Permission, Resource, rbac_system

app = FastAPI()
security = HTTPBearer()

def get_current_user_id(token = Depends(security)) -> str:
    # Your existing JWT/token validation logic
    # Return user_id from token
    pass

def check_permission_dependency(permission: Permission, resource: Resource = None):
    def permission_checker(user_id: str = Depends(get_current_user_id)):
        if not rbac_system.check_permission(user_id, permission, resource):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied: Insufficient permissions"
            )
        return user_id
    return permission_checker

@app.get("/api/documents")
def get_documents(user_id: str = Depends(check_permission_dependency(Permission.READ, Resource.DOCUMENTS))):
    # Your existing code
    return {"documents": ["doc1", "doc2"]}

@app.post("/api/users")
def create_user(user_id: str = Depends(check_permission_dependency(Permission.MANAGE_USERS, Resource.USERS))):
    # Your existing code
    return {"message": "User created"}
