from dataclasses import dataclass
from enum import Enum


# Colors from: https://catppuccin.com/palette
class Colors(Enum):
    ERROR = (243, 139, 168)
    WARNING = (249, 226, 175)
    SUCCESS = (166, 227, 161)


@dataclass
class Colors:
    error = (243, 139, 168)
    warning = (249, 226, 175)
    success = (166, 227, 161)

# Colors from: https://catppuccin.com/palette
