import os
import pathlib
import secrets
import base64

def setup_project():
    # Create directories
    directories = [
        "data/raw", "data/processed", "data/output",
        "logs/airflow", "logs/spark", "logs/pipeline",
        "notebooks",
        "tests/unit", "tests/integration"
    ]
    
    for dir_path in directories:
        path = pathlib.Path(dir_path)
        path.mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {dir_path}")

    # Create .env file
    env_file = pathlib.Path(".env")
    if not env_file.exists():
        # Generate a Fernet-like key (32 bytes base64 encoded)
        fernet_key = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()
        env_content = f"""# API Configuration
TMDB_API_KEY=your_tmdb_api_key

# Database
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow

# Redis
REDIS_PASSWORD=redis_secret

# Airflow
AIRFLOW_UID=50000
AIRFLOW__CORE__FERNET_KEY={fernet_key}
"""
        env_file.write_text(env_content)
        print("Created .env file")
    else:
        print(".env file already exists")

if __name__ == "__main__":
    setup_project()
