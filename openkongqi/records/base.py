# -*- coding: utf-8 -*-
from datetime import datetime
import json
import pytz

from ..utils import load_backend, get_uuid

_CACHE_KEY = 'okq:{uuid}:latest'


class BaseRecordsWrapper(object):
    """Base wrapper class to get database records. This class is to be used
    as parent of any database records wrapper class.

    The children wrapper class will have to define custom methods,
    for creating the database connection.
    """

    # key_ctx will be overwritten with dict when initializing source
    key_ctx = None
    ts_fmt = "%Y-%m-%dT%H:%M:%SZ"

    def __init__(self, settings, cache, *args, **kwargs):
        self._cnx = self.create_cnx(settings)
        self._cache = cache
        # NOTE: can't apply key context here
        # because it hasn't been set when this is initialized
        self._cache_key = settings.get('CACHE_KEY', _CACHE_KEY)

    def create_cnx(self, settings):
        """Create a connection to the database

        .. warning:: This method has be overwritten
        """
        raise NotImplementedError

    def db_init(self):
        """Initialize database.

        .. warning:: This method has be overwritten
        """
        raise NotImplementedError

    def is_duplicate(self, record):
        """Check for duplicated records.

        Check if the timestamp, uuid, key already exists in the db.
        Note that these are all the primary keys.

        .. warning:: This method has be overwritten
        """
        raise NotImplementedError

    def write_records(self, records):
        """Save the records

        See data extraction format on what to expect as input.

        .. warning:: This method has to be overwritten
        """
        raise NotImplementedError

    def get_records(self, uuid, start, end, fields=None):
        """Get records between two dates and for certain fields.

        If fields is ``None``, then select all the fields available.

        Returns a generator for all the records found.

        .. warning:: This method has to be overwritten

        :param uuid: unique id
        :type uuid: str
        :param start: the start date (lower boundary)
        :type start: datetime.datetime
        :param end: the end date (upper boundary)
        :type end: datetime.datetime
        :param fields: fields to filter on
        :type fields: list
        """
        raise NotImplementedError

    def _apply_key_ctx(self, key):
        if self.key_ctx is not None:
            modname_prefix = self.key_ctx.get('modname')
            if modname_prefix:
                return get_uuid(modname_prefix, key)
        else:
            return key

    def get_cache_key(self, uuid, contextualized=True):
        if contextualized:
            return self._cache_key.format(uuid=self._apply_key_ctx(uuid))
        else:
            return self._cache_key.format(uuid=uuid)

    def set_latest(self, uuid, record):
        """Set record as the latest entry in cache database.

        :param uuid: unique id
        :type uuid: str
        """
        latest_record = {
            'ts': self._ts_to_string(record['ts']),
            'fields': record['fields'],
        }
        latest_json = json.dumps(latest_record)
        key = self.get_cache_key(uuid=uuid)
        self._cache.set(key, latest_json)

    def get_latest(self, uuid):
        """Get latest record entry from cache database.

        :param uuid: unique id
        :type uuid: str
        """
        latest = self._cache.get(
            self.get_cache_key(uuid, contextualized=False)
        )
        if latest is None:
            return None
        return json.loads(latest)

    def _ts_to_string(self, ts):
        """Convert a datetime.datetime object to a string.

        This is done for compatibility in serialization.

        Format:    %Y-%m-%dT%H:%M:%SZ
        Example: 2016-07-13T10:09:56Z

        :param ts: timestamp
        :type ts: datetime.datetime
        """
        # remove the microseconds
        ts = ts.replace(microsecond=0)
        # convert to UTC or keep naive
        if ts.tzinfo is not None:
            ts = ts.astimezone(pytz.utc)
        return datetime.strftime(ts, self.ts_fmt)

    def _string_to_ts(self, ts_string):
        """Convert a timestamp string to datetime.datetime object.

        The input format has to be the same
        from the output of ``_ts_to_string``.

        :param ts_string: timestamp string
        :type ts_string: str
        """
        return datetime \
            .strptime(ts_string, self.ts_fmt) \
            .replace(tzinfo=pytz.utc)


def create_recsdb(settings, cache):
    mod = load_backend(settings['ENGINE'])
    return mod.RecordsWrapper(settings, cache)
