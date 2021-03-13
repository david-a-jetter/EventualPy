from typing import List, Optional

from pydantic import BaseModel

from EventualPy.annotation.models import Annotation


class DataEntryField(BaseModel):
    id: int
    annotations: List[Annotation]

    @property
    def active_annotation(self) -> Optional[Annotation]:
        if len(self.annotations) > 0:
            return self.annotations[-1]
        else:
            return None
