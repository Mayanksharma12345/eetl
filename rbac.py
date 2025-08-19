from enum import Enum
from typing import Dict, Optional
from dataclasses import dataclass, field
import functools


class Permission(Enum):
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    ADMIN = "admin"
    PROCESS_FILES = "process_files"
    VIEW_REPORTS = "view_reports"
    MANAGE_ETL = "manage_etl"


class Resource(Enum):
    FILES = "files"
    REPORTS = "reports"
    DATABASE = "database"
    SYSTEM = "system"


@dataclass
class Role:
    name: str
    permissions: set = field(default_factory=set)

    def has_permission(self, permission: Permission) -> bool:
        return permission in self.permissions


@dataclass
class User:
    user_id: str
    username: str
    role: Role
    is_active: bool = True

    def has_permission(self, permission: Permission) -> bool:
        return self.is_active and self.role.has_permission(permission)


class ETL_RBAC:
    def __init__(self):
        self.users: Dict[str, User] = {}
        self.roles: Dict[str, Role] = {}
        self._setup_roles()

    def _setup_roles(self):
        """Setup three roles for ETL system"""

        # ADMIN - Can do everything
        admin_role = Role("admin", {
            Permission.READ, Permission.WRITE, Permission.DELETE,
            Permission.ADMIN, Permission.PROCESS_FILES,
            Permission.VIEW_REPORTS, Permission.MANAGE_ETL
        })

        # MANAGER - Can process files and view reports
        manager_role = Role("manager", {
            Permission.READ, Permission.WRITE, Permission.PROCESS_FILES,
            Permission.VIEW_REPORTS
        })

        # USER - Can only view reports
        user_role = Role("user", {
            Permission.READ, Permission.VIEW_REPORTS
        })

        self.roles = {
            "admin": admin_role,
            "manager": manager_role,
            "user": user_role
        }

    def create_user(self, user_id: str, username: str, role_name: str) -> User:
        if role_name not in self.roles:
            raise ValueError(f"Invalid role: {role_name}")

        user = User(user_id, username, self.roles[role_name])
        self.users[user_id] = user
        return user

    def check_permission(self, user_id: str, permission: Permission) -> bool:
        user = self.users.get(user_id)
        return user.has_permission(permission) if user else False


# Create global RBAC instance
rbac = ETL_RBAC()