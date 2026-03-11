#!/usr/bin/env python3
import json
import os
import singer
from singer import metadata, utils
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_xero import streams as streams_
from tap_xero.client import XeroClient
from tap_xero.context import Context
from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.tap_base import Tap

# REQUIRED_CONFIG_KEYS = [
#     "start_date",
#     "client_id",
#     "client_secret",
#     "tenant_id",
#     "refresh_token",

# ]

LOGGER = singer.get_logger()

BAD_CREDS_MESSAGE = (
    "Failed to refresh OAuth token using the credentials from both the config and S3. "
    "The token might need to be reauthorized from the integration's properties "
    "or there could be another authentication issue. Please attempt to reauthorize "
    "the integration."
)


class BadCredsException(Exception):
    pass


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(tap_stream_id):
    path = "schemas/{}.json".format(tap_stream_id)
    schema = utils.load_json(get_abs_path(path))
    dependencies = schema.pop("tap_schema_dependencies", [])
    refs = {}
    for sub_stream_id in dependencies:
        refs[sub_stream_id] = load_schema(sub_stream_id)
    if refs:
        singer.resolve_schema_references(schema, refs)
    return schema

def load_metadata(stream, schema):
    mdata = metadata.new()

    mdata = metadata.write(mdata, (), 'table-key-properties', stream.pk_fields)
    mdata = metadata.write(mdata, (), 'forced-replication-method', stream.replication_method)

    if stream.bookmark_key:
        mdata = metadata.write(mdata, (), 'valid-replication-keys', [stream.bookmark_key])

    for field_name in schema['properties'].keys():
        if field_name in stream.pk_fields or field_name == stream.bookmark_key:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)


def ensure_credentials_are_valid(config):
    XeroClient(config).filter("currencies")

def discover(ctx):
    ctx.check_platform_access()
    catalog = Catalog([])
    for stream in streams_.all_streams:
        schema_dict = load_schema(stream.tap_stream_id)
        mdata = load_metadata(stream, schema_dict)

        schema = Schema.from_dict(schema_dict)
        catalog.streams.append(CatalogEntry(
            stream=stream.tap_stream_id,
            tap_stream_id=stream.tap_stream_id,
            key_properties=stream.pk_fields,
            schema=schema,
            metadata=mdata
        ))
    return catalog


def load_and_write_schema(stream):
    singer.write_schema(
        stream.tap_stream_id,
        load_schema(stream.tap_stream_id),
        stream.pk_fields,
    )


def sync(ctx):
    ctx.refresh_credentials()
    currently_syncing = ctx.state.get("currently_syncing")
    start_idx = streams_.all_stream_ids.index(currently_syncing) \
        if currently_syncing else 0
    stream_ids_to_sync = [cs.tap_stream_id for cs in ctx.catalog.streams
                          if cs.is_selected()]
    streams = [s for s in streams_.all_streams[start_idx:]
               if s.tap_stream_id in stream_ids_to_sync]
    for stream in streams:
        ctx.state["currently_syncing"] = stream.tap_stream_id
        ctx.write_state()
        load_and_write_schema(stream)
        LOGGER.info("Syncing stream: %s", stream.tap_stream_id)
        stream.sync(ctx)
    ctx.state["currently_syncing"] = None
    ctx.write_state()


def _sdk_catalog_to_singer(sdk_catalog):
    """Convert Hotglue SDK Catalog to singer Catalog for use with existing sync/streams."""
    singer_streams = []
    for entry in sdk_catalog.streams:
        singer_streams.append(
            CatalogEntry(
                stream=entry.tap_stream_id,
                tap_stream_id=entry.tap_stream_id,
                key_properties=entry.key_properties or [],
                schema=Schema.from_dict(entry.schema.to_dict()),
                metadata=entry.metadata.to_list(),
            )
        )
    return Catalog(singer_streams)


class TapXero(Tap):
    """Marketo Engage tap."""

    name = "tap-xero"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "start_date",
            th.DateTimeType,
            description="Earliest updatedAt timestamp to sync from.",
        ),
        th.Property("client_id", th.StringType, required=True),
        th.Property("client_secret", th.StringType, required=True),
        th.Property("tenant_id", th.StringType, required=True),
        th.Property("refresh_token", th.StringType, required=True),
    ).to_dict()

    def run_discovery(self) -> str:
        """Write the catalog json to STDOUT and return as a string.

        Returns:
            The catalog as a string of JSON.
        """
        config_path = str(self.config_file) if self.config_file else ""
        catalog = discover(Context(dict(self.config), {}, {}, config_path))
        catalog_dict = {"streams": [s.to_dict() for s in catalog.streams]}
        catalog_text = json.dumps(catalog_dict, indent=2)
        print(catalog_text)
        return catalog_text

    def sync_all(self) -> None:
        """Sync all streams.

        Returns:
            None
        """
        config_path = str(self.config_file) if self.config_file else ""
        catalog = _sdk_catalog_to_singer(self.catalog)
        sync(Context(dict(self.config), self.state, catalog, config_path))

    def discover_streams(self):
        ''' Just to avoid raise NotImplementedError'''
        return []

if __name__ == "__main__":
    TapXero.cli()
