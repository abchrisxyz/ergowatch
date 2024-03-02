from enum import Enum

from pydantic import constr

Address = constr(pattern="^[a-zA-Z0-9]+$")
Digest32 = constr(pattern="^[a-zA-Z0-9]{64}$")
TokenID = Digest32


class TimeWindow(str, Enum):
    m1 = "1m"
    m3 = "3m"
