from dataclasses import dataclass, field
from typing import List

@dataclass
class DeleteRecord:
    student_id: str
    admission_ids: List[str] = field(default_factory=list)
    following_ids: List[str] = field(default_factory=list)