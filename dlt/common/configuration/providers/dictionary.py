from contextlib import contextmanager
from typing import Any, Iterator, Optional, Type, Tuple

from dlt.common.typing import StrAny

from .provider import Provider


class DictionaryProvider(Provider):

    def __init__(self) -> None:
        self._values: StrAny = {}
        pass

    @property
    def name(self) -> str:
        return "Dictionary Provider"

    def get_value(self, key: str, hint: Type[Any], *namespaces: str) -> Tuple[Optional[Any], str]:
        full_path = namespaces + (key,)
        full_key = "__".join(full_path)
        node = self._values
        try:
            for k in  full_path:
                node = node[k]
            return node, full_key
        except KeyError:
            return None, full_key

    @property
    def supports_secrets(self) -> bool:
        return True

    @property
    def supports_namespaces(self) -> bool:
        return True


    @contextmanager
    def values(self, v: StrAny) -> Iterator[None]:
        p_values = self._values
        self._values = v
        yield
        self._values = p_values