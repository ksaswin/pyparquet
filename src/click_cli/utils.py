from dataclasses import dataclass
from enum import Enum


# Colors from: https://catppuccin.com/palette
class Colors(Enum):
    ERROR = (243, 139, 168)
    WARNING = (249, 226, 175)
    SUCCESS = (166, 227, 161)


@dataclass
class FileFormat:
    filetype: str
    extension: str


class TransformFileFormat(Enum):
    CSV = FileFormat("csv", ".csv")
    EXCEL = FileFormat("excel", ".xlsx")
    JSON = FileFormat("json", ".json")

    @property
    def filetype(self):
        return self.value.filetype

    @property
    def file_extension(self):
        return self.value.extension


def get_file_extension(filetype: str) -> str | None:
    for en in TransformFileFormat:
        if en.filetype == filetype:
            return en.file_extension

    return None

