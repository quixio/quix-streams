from .apply import ApplyFunction
from typing import Optional


class IdentityFunction(ApplyFunction):
    def __init__(self, name: Optional[str] = "IDENTITY"):
        super().__init__(func=lambda x: x, name=name)
