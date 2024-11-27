from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .manager import SourceProcess

__all__ = ("SourceException",)


class SourceException(Exception):
    """
    Raised in the parent process when a source finish with an exception
    """

    def __init__(self, process: "SourceProcess") -> None:
        self.pid: Optional[int] = process.pid
        self.process: SourceProcess = process
        self.exitcode = self.process.exitcode

    def __str__(self) -> str:
        msg = f"{self.process.source} with PID {self.pid} failed"
        if self.exitcode == 0:
            return msg
        return f"{msg} with exitcode {self.exitcode}"
