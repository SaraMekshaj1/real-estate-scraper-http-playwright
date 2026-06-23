from enum import Enum, auto

class RunOutcome(Enum):
    COMPLETED       = auto()   # all pages visited naturally
    EARLY_STOP      = auto()   # daily run hit consecutive-known threshold
    INTERRUPTED     = auto()   # crash, rate-limit kill, network drop