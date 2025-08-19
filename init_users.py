
# init_users.py
import os, sqlite3
from passlib.context import CryptContext

DB_PATH = os.path.join(os.path.dirname(__file__), "etl_kpis.db")
pwd = CryptContext(schemes=["bcrypt"], deprecated="auto")

def ensure_users_table():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL CHECK (role IN ('admin','manager','user')),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        conn.commit()

def seed_admin(username="admin", email="admin@example.com", password="Admin@123"):
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        # upsert-like behavior: insert if not exists
        c.execute("SELECT id FROM users WHERE username=?", (username,))
        row = c.fetchone()
        if row:
            print("Admin user already exists.")
            return
        c.execute(
            "INSERT INTO users (username, email, password_hash, role) VALUES (?, ?, ?, 'admin')",
            (username, email, pwd.hash(password))
        )
        conn.commit()
        print(f"Seeded admin user -> username={username}  password={password}")

if __name__ == "__main__":
    ensure_users_table()
    seed_admin()
