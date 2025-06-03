from typing import Literal, get_args

__all__ = ("JoinHow", "JoinHow_choices")

JoinHow = Literal["inner", "left"]
JoinHow_choices = get_args(JoinHow)
