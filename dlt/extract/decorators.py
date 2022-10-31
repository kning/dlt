import inspect
from types import ModuleType
from makefun import wraps
from typing import Any, Callable, Dict, Iterator, List, NamedTuple, Optional, Tuple, Type, TypeVar, Union, overload

from dlt.common.configuration import with_config, get_fun_spec
from dlt.common.configuration.specs import BaseConfiguration
from dlt.common.exceptions import ArgumentsOverloadException
from dlt.common.schema.schema import Schema
from dlt.common.schema.typing import TTableSchemaColumns, TWriteDisposition
from dlt.common.typing import AnyFun, ParamSpec, TDataItems
from dlt.common.utils import is_inner_function
from dlt.extract.exceptions import InvalidResourceDataTypeFunctionNotAGenerator

from dlt.extract.typing import TTableHintTemplate, TFunHintTemplate
from dlt.extract.source import DltResource, DltSource


class SourceInfo(NamedTuple):
    SPEC: Type[BaseConfiguration]
    f: AnyFun
    module: ModuleType


_SOURCES: Dict[str, SourceInfo] = {}

TSourceFunParams = ParamSpec("TSourceFunParams")
TResourceFunParams = ParamSpec("TResourceFunParams")


@overload
def source(func: Callable[TSourceFunParams, Any], /, name: str = None, schema: Schema = None, spec: Type[BaseConfiguration] = None) -> Callable[TSourceFunParams, DltSource]:
    ...

@overload
def source(func: None = ..., /, name: str = None, schema: Schema = None, spec: Type[BaseConfiguration] = None) -> Callable[[Callable[TSourceFunParams, Any]], Callable[TSourceFunParams, DltSource]]:
    ...

def source(func: Optional[AnyFun] = None, /, name: str = None, schema: Schema = None, spec: Type[BaseConfiguration] = None) -> Any:

    # if name and schema:
    #     raise ArgumentsOverloadException(
    #         "source name cannot be set if schema is present",
    #         "source",
    #         "You can provide either the Schema instance directly in `schema` argument or the name of ")

    def decorator(f: Callable[TSourceFunParams, Any]) -> Callable[TSourceFunParams, DltSource]:
        nonlocal schema, name

        # source name is passed directly or taken from decorated function name
        name = name or f.__name__

        if not schema:
            # create or load default schema
            # TODO: we need a convention to load ie. load the schema from file with name_schema.yaml
            schema = Schema(name)

        # wrap source extraction function in configuration with namespace
        conf_f = with_config(f, spec=spec, namespaces=("source", name))

        @wraps(conf_f, func_name=name)
        def _wrap(*args: Any, **kwargs: Any) -> DltSource:
            rv = conf_f(*args, **kwargs)

            # if generator, consume it immediately
            if inspect.isgenerator(rv):
                rv = list(rv)

            # def check_rv_type(rv: Any) -> None:
            #     pass

            # # check if return type is list or tuple
            # if isinstance(rv, (list, tuple)):
            #     # check all returned elements
            #     for v in rv:
            #         check_rv_type(v)
            # else:
            #     check_rv_type(rv)

            # convert to source
            return DltSource.from_data(schema, rv)

        # get spec for wrapped function
        SPEC = get_fun_spec(conf_f)
        # store the source information
        _SOURCES[_wrap.__qualname__] = SourceInfo(SPEC, _wrap, inspect.getmodule(f))

        # the typing is right, but makefun.wraps does not preserve signatures
        return _wrap  # type: ignore

    if func is None:
        # we're called with parens.
        return decorator

    if not callable(func):
        raise ValueError("First parameter to the source must be a callable.")

    # we're called as @source without parens.
    return decorator(func)


# @source
# def reveal_1() -> None:
#     pass

# @source(name="revel")
# def reveal_2() -> None:
#     pass


# def revel_3(v) -> int:
#     pass


# reveal_type(reveal_1)
# reveal_type(reveal_1())

# reveal_type(reveal_2)
# reveal_type(reveal_2())

# reveal_type(source(revel_3))
# reveal_type(source(revel_3)("s"))

@overload
def resource(
    data: Callable[TResourceFunParams, Any],
    /,
    name: str = None,
    table_name_fun: TFunHintTemplate[str] = None,
    write_disposition: TTableHintTemplate[TWriteDisposition] = None,
    columns: TTableHintTemplate[TTableSchemaColumns] = None,
    selected: bool = True,
    depends_on: DltResource = None,
    spec: Type[BaseConfiguration] = None
) -> Callable[TResourceFunParams, DltResource]:
    ...

@overload
def resource(
    data: None = ...,
    /,
    name: str = None,
    table_name_fun: TFunHintTemplate[str] = None,
    write_disposition: TTableHintTemplate[TWriteDisposition] = None,
    columns: TTableHintTemplate[TTableSchemaColumns] = None,
    selected: bool = True,
    depends_on: DltResource = None,
    spec: Type[BaseConfiguration] = None
) -> Callable[[Callable[TResourceFunParams, Any]], Callable[TResourceFunParams, DltResource]]:
    ...


# @overload
# def resource(
#     data: Union[DltSource, DltResource, Sequence[DltSource], Sequence[DltResource]],
#     /
# ) -> DltResource:
#     ...


@overload
def resource(
    data: Union[List[Any], Tuple[Any], Iterator[Any]],
    /,
    name: str = None,
    table_name_fun: TFunHintTemplate[str] = None,
    write_disposition: TTableHintTemplate[TWriteDisposition] = None,
    columns: TTableHintTemplate[TTableSchemaColumns] = None,
    selected: bool = True,
    depends_on: DltResource = None,
    spec: Type[BaseConfiguration] = None
) -> DltResource:
    ...


def resource(
    data: Optional[Any] = None,
    /,
    name: str = None,
    table_name_fun: TFunHintTemplate[str] = None,
    write_disposition: TTableHintTemplate[TWriteDisposition] = None,
    columns: TTableHintTemplate[TTableSchemaColumns] = None,
    selected: bool = True,
    depends_on: DltResource = None,
    spec: Type[BaseConfiguration] = None
) -> Any:

    def make_resource(_name: str, _data: Any) -> DltResource:
        table_template = DltResource.new_table_template(table_name_fun or _name, write_disposition=write_disposition, columns=columns)
        return DltResource.from_data(_data, _name, table_template, selected, depends_on)


    def decorator(f: Callable[TResourceFunParams, Any]) -> Callable[TResourceFunParams, DltResource]:
        resource_name = name or f.__name__

        # if f is not a generator (does not yield) raise Exception
        if not inspect.isgeneratorfunction(inspect.unwrap(f)):
            raise InvalidResourceDataTypeFunctionNotAGenerator(resource_name, f, type(f))

        # do not inject config values for inner functions, we assume that they are part of the source
        SPEC: Type[BaseConfiguration] = None
        if is_inner_function(f):
            conf_f = f
        else:
            # wrap source extraction function in configuration with namespace
            conf_f = with_config(f, spec=spec, namespaces=("resource", resource_name))
            # get spec for wrapped function
            SPEC = get_fun_spec(conf_f)

        # @wraps(conf_f, func_name=resource_name)
        # def _wrap(*args: Any, **kwargs: Any) -> DltResource:
        #     return make_resource(resource_name, f(*args, **kwargs))

        # store the standalone resource information
        if SPEC:
            _SOURCES[f.__qualname__] = SourceInfo(SPEC, f, inspect.getmodule(f))

        # the typing is right, but makefun.wraps does not preserve signatures
        return make_resource(resource_name, f)

    # if data is callable or none use decorator
    if data is None:
        # we're called with parens.
        return decorator

    if callable(data):
        return decorator(data)
    else:
        return make_resource(name, data)


def _get_source_for_inner_function(f: AnyFun) -> Optional[SourceInfo]:
    # find source function
    parts = f.__qualname__.split(".")
    parent_fun = ".".join(parts[:-2])
    return _SOURCES.get(parent_fun)


# @resource
# def reveal_1() -> None:
#     pass

# @resource(name="revel")
# def reveal_2() -> None:
#     pass


# def revel_3(v) -> int:
#     pass


# reveal_type(reveal_1)
# reveal_type(reveal_1())

# reveal_type(reveal_2)
# reveal_type(reveal_2())

# reveal_type(resource(revel_3))
# reveal_type(resource(revel_3)("s"))


# reveal_type(resource([], name="aaaa"))
# reveal_type(resource("aaaaa", name="aaaa"))

# name of dlt metadata as part of the item
# DLT_METADATA_FIELD = "_dlt_meta"


# class TEventDLTMeta(TypedDict, total=False):
#     table_name: str  # a root table in which store the event


# def append_dlt_meta(item: TBoundItem, name: str, value: Any) -> TBoundItem:
#     if isinstance(item, abc.Sequence):
#         for i in item:
#             i.setdefault(DLT_METADATA_FIELD, {})[name] = value
#     elif isinstance(item, dict):
#         item.setdefault(DLT_METADATA_FIELD, {})[name] = value

#     return item


# def with_table_name(item: TBoundItem, table_name: str) -> TBoundItem:
#     # normalize table name before adding
#     return append_dlt_meta(item, "table_name", table_name)


# def get_table_name(item: StrAny) -> Optional[str]:
#     if DLT_METADATA_FIELD in item:
#         meta: TEventDLTMeta = item[DLT_METADATA_FIELD]
#         return meta.get("table_name", None)
#     return None


# def with_retry(max_retries: int = 3, retry_sleep: float = 1.0) -> Callable[[Callable[_TFunParams, TBoundItem]], Callable[_TFunParams, TBoundItem]]:

#     def decorator(f: Callable[_TFunParams, TBoundItem]) -> Callable[_TFunParams, TBoundItem]:

#         def _wrap(*args: Any, **kwargs: Any) -> TBoundItem:
#             attempts = 0
#             while True:
#                 try:
#                     return f(*args, **kwargs)
#                 except Exception as exc:
#                     if attempts == max_retries:
#                         raise
#                     attempts += 1
#                     logger.warning(f"Exception {exc} in iterator, retrying {attempts} / {max_retries}")
#                     sleep(retry_sleep)

#         return _wrap

#     return decorator


TBoundItems = TypeVar("TBoundItems", bound=TDataItems)
TDeferred = Callable[[], TBoundItems]
TDeferredFunParams = ParamSpec("TDeferredFunParams")


def defer(f: Callable[TDeferredFunParams, TBoundItems]) -> Callable[TDeferredFunParams, TDeferred[TBoundItems]]:

    @wraps(f)
    def _wrap(*args: Any, **kwargs: Any) -> TDeferred[TBoundItems]:
        def _curry() -> TBoundItems:
            return f(*args, **kwargs)
        return _curry

    return _wrap  # type: ignore