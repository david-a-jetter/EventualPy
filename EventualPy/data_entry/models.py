from typing import  Optional
from pydantic import BaseModel
from EventualPy.annotation.models import Annotation


class DataEntryField(BaseModel):
    id: int
    annotation: Optional[Annotation]

