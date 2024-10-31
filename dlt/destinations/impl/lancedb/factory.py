import typing as t
import os

from dlt.common.destination import Destination, DestinationCapabilitiesContext
from dlt.common.destination.capabilities import DataTypeMapper
from dlt.common.exceptions import MissingDependencyException
from dlt.destinations.impl.lancedb.configuration import (
    LanceDBCredentials,
    LanceDBClientConfiguration,
)

LanceDBTypeMapper: t.Type[DataTypeMapper]
try:
    # lancedb type mapper cannot be used without pyarrow installed
    from dlt.destinations.impl.lancedb.type_mapper import LanceDBTypeMapper
except MissingDependencyException:
    # assign mock type mapper if no arrow
    from dlt.common.destination.capabilities import UnsupportedTypeMapper as LanceDBTypeMapper


if t.TYPE_CHECKING:
    from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient


# Define function to get embedding model provider and host
def get_embedding_model_provider():
    provider = os.getenv("DESTINATION__LANCEDB__EMBEDDING_MODEL_PROVIDER")
    provider_host = os.getenv("DESTINATION__LANCEDB__EMBEDDING_MODEL_PROVIDER_HOST")
    
    if provider == "ollama" and provider_host:
        model_host = provider_host
    else:
        model_host = "localhost:11434"
    
    return provider, model_host


class lancedb(Destination[LanceDBClientConfiguration, "LanceDBClient"]):
    spec = LanceDBClientConfiguration

    def _raw_capabilities(self) -> DestinationCapabilitiesContext:
        caps = DestinationCapabilitiesContext()
        caps.preferred_loader_file_format = "jsonl"
        caps.supported_loader_file_formats = ["jsonl"]
        caps.type_mapper = LanceDBTypeMapper

        caps.max_identifier_length = 200
        caps.max_column_identifier_length = 1024
        caps.max_query_length = 8 * 1024 * 1024
        caps.is_max_query_length_in_bytes = False
        caps.max_text_data_type_length = 8 * 1024 * 1024
        caps.is_max_text_data_type_length_in_bytes = False
        caps.supports_ddl_transactions = False

        caps.decimal_precision = (38, 18)
        caps.timestamp_precision = 6
        caps.supported_replace_strategies = ["truncate-and-insert"]

        return caps

    @property
    def client_class(self) -> t.Type["LanceDBClient"]:
        from dlt.destinations.impl.lancedb.lancedb_client import LanceDBClient

        return LanceDBClient

    def __init__(
        self,
        credentials: t.Union[LanceDBCredentials, t.Dict[str, t.Any]] = None,
        destination_name: t.Optional[str] = None,
        environment: t.Optional[str] = None,
        provider: t.Optional[str] = None,
        embedding_host: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> None:
        super().__init__(
            credentials=credentials,
            destination_name=destination_name,
            environment=environment,
            provider = provider,
            embedding_host = embedding_host,
            **kwargs,
        )
        # Retrieve and use the embedding host based on the provider
        provider, embedding_host = get_embedding_model_provider()
        print(f"Using embedding provider: {provider} with host: {embedding_host}")

