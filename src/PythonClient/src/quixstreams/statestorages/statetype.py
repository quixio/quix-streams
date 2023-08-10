from enum import Enum
from typing import TypeVar


class StateType(Enum):
    Binary = 'a'
    Object = 'o'
    Bool = 'b'
    Double = 'd'
    Long = 'l'
    String = 's'


StreamStateType = TypeVar('StreamStateType')
