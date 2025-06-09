from datetime import timedelta
from typing import Any, Callable, Literal, Optional, Union, get_args

from quixstreams.dataframe.utils import ensure_milliseconds

from .utils import keep_left_merger, keep_right_merger, raise_merger

__all__ = ("Join", "JoinHow", "JoinHow_choices", "OnOverlap", "OnOverlap_choices")

JoinHow = Literal["inner", "left"]
JoinHow_choices = get_args(JoinHow)

OnOverlap = Literal["keep-left", "keep-right", "raise"]
OnOverlap_choices = get_args(OnOverlap)


class Join:
    def __init__(
        self,
        how: JoinHow,
        on_merge: Union[OnOverlap, Callable[[Any, Any], Any]],
        grace_ms: Union[int, timedelta],
        store_name: Optional[str] = None,
    ) -> None:
        if how not in JoinHow_choices:
            raise ValueError(
                f'Invalid "how" value: {how}. '
                f"Valid choices are: {', '.join(JoinHow_choices)}."
            )
        self._how = how

        if callable(on_merge):
            self._merger = on_merge
        elif on_merge == "keep-left":
            self._merger = keep_left_merger
        elif on_merge == "keep-right":
            self._merger = keep_right_merger
        elif on_merge == "raise":
            self._merger = raise_merger
        else:
            raise ValueError(
                f'Invalid "on_merge" value: {on_merge}. '
                f"Provide either one of {', '.join(OnOverlap_choices)} or "
                f"a callable to merge records manually."
            )

        self._grace_ms = ensure_milliseconds(grace_ms)
        self._store_name = store_name or "join"
