from typing import List, Sequence, NamedTuple
from itertools import groupby
from pathlib import Path

from dlt.common.storages.file_storage import FileStorage
from dlt.common.configuration.specs import NormalizeVolumeConfiguration
from dlt.common.storages.versioned_storage import VersionedStorage


class TParsedNormalizeFileName(NamedTuple):
    schema_name: str
    table_name: str
    file_id: str


class NormalizeStorage(VersionedStorage):

    STORAGE_VERSION = "1.0.0"
    EXTRACTED_FOLDER: str = "extracted"  # folder within the volume where extracted files to be normalized are stored

    def __init__(self, is_owner: bool, C: NormalizeVolumeConfiguration) -> None:
        super().__init__(NormalizeStorage.STORAGE_VERSION, is_owner, FileStorage(C.normalize_volume_path, "t", makedirs=is_owner))
        self.CONFIG = C
        if is_owner:
            self.initialize_storage()

    def initialize_storage(self) -> None:
        self.storage.create_folder(NormalizeStorage.EXTRACTED_FOLDER, exists_ok=True)

    def list_files_to_normalize_sorted(self) -> Sequence[str]:
        return sorted(self.storage.list_folder_files(NormalizeStorage.EXTRACTED_FOLDER))

    def get_grouped_iterator(self, files: Sequence[str]) -> "groupby[str, str]":
        return groupby(files, NormalizeStorage.get_schema_name)

    @staticmethod
    def get_schema_name(file_name: str) -> str:
        return NormalizeStorage.parse_normalize_file_name(file_name).schema_name

    @staticmethod
    def build_extracted_file_stem(schema_name: str, table_name: str, file_id: str) -> str:
        # builds file name with the extracted data to be passed to normalize
        return f"{schema_name}.{table_name}.{file_id}"

    @staticmethod
    def parse_normalize_file_name(file_name: str) -> TParsedNormalizeFileName:
        # parse extracted file name and returns (events found, load id, schema_name)
        if not file_name.endswith("jsonl"):
            raise ValueError(file_name)

        parts = Path(file_name).stem.split(".")
        if len(parts) != 3:
            raise ValueError(file_name)
        return TParsedNormalizeFileName(*parts)