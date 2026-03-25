from typing import Optional, List, Any
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, model_serializer

from app.schemas.service import Service

class ClientBase(BaseModel):
    name: str
    email: Optional[str] = None
    gstins: Optional[List[str]] = []
    pan: Optional[str] = None
    cin: Optional[str] = None
    tan: Optional[str] = None
    iec: Optional[str] = None

class ClientCreate(ClientBase):
    service_ids: Optional[List[UUID]] = []

class ClientUpdate(ClientBase):
    pass

class ClientServicesUpdate(BaseModel):
    service_ids: List[UUID]

class ClientInDBBase(ClientBase):
    id: UUID
    firm_id: UUID
    created_at: datetime

    class Config:
        from_attributes = True

class Client(ClientInDBBase):
    services: List[Service] = []
    
    @model_serializer(mode='wrap')
    def _serialize(self, serializer: Any) -> dict:
        data = serializer(self)
        # Add client_id as a computed field
        data['client_id'] = f"CLT-{str(self.id)[:8].upper()}"
        # Explicitly ensure pan/cin are included (debugging missing fields)
        if self.pan:
            data['pan'] = self.pan
        if self.cin:
            data['cin'] = self.cin
        return data
