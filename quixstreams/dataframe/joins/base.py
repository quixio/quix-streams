from typing import Literal, get_args

__all__ = ("JoinHow", "JoinHow_choices", "OnOverlap", "OnOverlap_choices")

JoinHow = Literal["inner", "left"]
JoinHow_choices = get_args(JoinHow)

OnOverlap = Literal["keep-left", "keep-right", "raise"]
OnOverlap_choices = get_args(OnOverlap)
