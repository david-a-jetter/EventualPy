from typing import Any
from pydantic import BaseModel


class Annotation(BaseModel):
    id: int
    data: Any
    acknowledged: bool
