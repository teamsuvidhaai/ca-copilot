from typing import Any, Dict, List, Optional, Union

from pydantic import AnyHttpUrl, PostgresDsn, validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    PROJECT_NAME: str = "CA-Copilot Backend"
    API_V1_STR: str = "/api/v1"
    
    # In production, these must be set
    SECRET_KEY: str
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24 * 8 

    DATABASE_URL: str
    OPENAI_API_KEY: Optional[str] = None
    
    # Supabase Storage
    SUPABASE_URL: Optional[str] = None
    SUPABASE_KEY: Optional[str] = None
    SUPABASE_BUCKET: str = "knowledge-kits"

    # Google Drive Storage
    STORAGE_PROVIDER: str = "google_drive" # 'supabase' or 'google_drive'
    GOOGLE_CREDENTIALS_PATH: str = "google-credentials.json"
    GOOGLE_DRIVE_FOLDER_ID: Optional[str] = None
    
    # Google OAuth (for Google Sign-In)
    GOOGLE_OAUTH_CLIENT_ID: Optional[str] = None

    # GST Services
    APPYFLOW_API_KEY: Optional[str] = None

    # LlamaParse (document parsing)
    LLAMA_CLOUD_API_KEY: Optional[str] = None


    BACKEND_CORS_ORIGINS: List[str] = []

    @validator("DATABASE_URL", pre=True)
    def fix_database_url(cls, v: str) -> str:
        if isinstance(v, str):
            # Fix protocol for asyncpg
            if v.startswith("postgres://"):
                v = v.replace("postgres://", "postgresql+asyncpg://", 1)
            elif v.startswith("postgresql://") and "+asyncpg" not in v:
                v = v.replace("postgresql://", "postgresql+asyncpg://", 1)
            
            # Strip ALL query parameters — asyncpg doesn't support them in the URL.
            # We handle ssl, pooling, timeouts explicitly via connect_args in session.py.
            if "?" in v:
                v = v.split("?")[0]
                
        return v

    @validator("BACKEND_CORS_ORIGINS", pre=True)
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Union[List[str], str]:
        if isinstance(v, str) and not v.startswith("["):
            return [i.strip() for i in v.split(",")]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)

    model_config = SettingsConfigDict(case_sensitive=True, env_file=".env", extra="ignore")


settings = Settings()
