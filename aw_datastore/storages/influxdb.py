import logging
from typing import List, Dict, Optional
from datetime import datetime, timezone, timedelta
import json

from aw_core.models import Event

from influxdb import InfluxDBClient

from . import logger
from .abstract import AbstractStorage


# Time combined with measure (bucket_id) and tag style make it unique this times series database
# The time as it is stored in the database becomes key to updates

# JSON structure for InfluxDB for events
# app/status in tags because it will allow easy filtering and grouping
# Rules of thumb: Tags are parameters you run WHERE against, Fields are
# what you SELECT but if you SELECT * you get both but you can't use WHERE
# on field all because performance first
"""
[
    {
        "mesasurement": "bucket_id_window",
        "tags": {
            "app": "app_name"
        },
        "time": "2009-11-10T23:00:00Z",
        "fields": {
            "id": 0
            "duration": 20
            "title": "webstie - chrome" 
        }
    },
    {
        "measurement": "bucket_id_afk",
        "tags": {
            "status": "not-afk" // status as a tag there are only two groups
        }
        "time": "2009-11-10T23:00:00Z",
        "fields": {
            "id": 0
            "duration": 20
        }
    }
]
"""

class InfluxDBStorage(AbstractStorage):
    """Uses a InfluxDB server as backend"""
    sid = "influxdb"

    def __init__(self, testing):
        # TODO put config in correct place
        # hardcoded for testing
        host = "192.168.1.116" # defaults localhost
        port = 8086 # defaults 8086
        user = "root" # defaults root
        password = "root" # defaults root
        dbname = "activitywatch" + ("-testing" if testing else "")
        self.client = InfluxDBClient(host, port, user, password, dbname)
        
        self.db = InfluxDBClient(host, port, user, password, dbname)
        # Tags are parameters that can be used with a where or a group by
        # FIXME any parameter for an event can become a tag except time and bucket_id
        # This will be set across all bucket_ids so may need a way to set custom tags by bucket id
        # but this is very database specific
        # Made tags duplicate of fields for now
        self.tags = ("app", "status")
        # Appears if a database already exists it does not cause issues
        self.db.create_database(dbname)
        # FIXME: changing the value below you will have to change _event_parser_from for the timedelta to whatever 
        # precision you set it to, not sure how to make it easier to change
        self.time_precision = "ms"

    def buckets(self) -> Dict[str, dict]:
        # Should not return metadata table as part of the buckets
        raw = self.db.get_list_measurements()
        bucketnames = [item["name"] for item in raw]
        buckets = dict()
        for bucket_id in bucketnames:
            if bucket_id != "metadata":
                buckets[bucket_id] = self.get_metadata(bucket_id)
        return buckets

    def create_bucket(self, bucket_id: str, type_id: str, client: str,
                      hostname: str, created: str, name: Optional[str] = None) -> None:
        # Orginally pumped the meta data into the events table but it created fields
        # that were not used and made the data look messy
        # This creates a seperate metadata table
        metadata = [{
            "measurement": "metadata",
            "time": created,
            "tags": {
                "id": bucket_id,
                "_id": "metadata",
            },
            "fields": {  
                "name": name,
                "type": type_id,
                "client": client,
                "hostname": hostname,
                "created": created,
            }
        }]
        self.db.write_points(points=metadata, time_precision=self.time_precision)
        # Need a dummy event otherwise it returns an error when looking for a "table"
        # Makes sense with standard databases but this is a schema-less style so you can
        # create tables by simply calling the database with a write call and there is no 
        # explicit create table method
        dummyevent = [{
             "measurement": bucket_id,
             "time": 0, # time zero because if it is set to created it messes with queued events
             "tags": {},
             "fields": {"duration": 0.0}
             }]        
        self.db.write_points(points=dummyevent,time_precision=self.time_precision)
        # 1 Because dummy event is 0
        #self.id_cache[bucket_id] = 1

    def delete_bucket(self, bucket_id: str) -> None:
        self.db.delete_series(measurement=bucket_id)

    def _event_parser_from(self, influxdb_result) -> list:
        """
        Maps the results from infuxdb into an event
        You can apply additional filters inside get_points
        like: (measurement=None, tags=None)
        Returns a list of all events (handles multiple)
        """
        event_generator = influxdb_result.get_points()
        results = []
        for event in event_generator:
            epoch = int(event["time"]) # set to milliseconds but can be tweaked with self.time_precision up top
            final_datetime = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=epoch)
            mapped = Event(event["time"], final_datetime, event["duration"], dict())
            for key in event:
                if key not in  ("time", "duration") and key[0] != "_": #ignore tags
                    if event[key]:
                        mapped.data[key] = event[key]    
            results.append(mapped)
        return results #if len(results) > 0 else None

    def _event_parser_to(self, bucket_id, event):
        data = {
            "measurement": bucket_id,
            "tags": {},
            "time": event.timestamp,
            "fields": {
                "id": event.id
            }
        }
        data["fields"]["duration"] = event.duration.total_seconds()
        for key in event.data:
            if key in self.tags:
                data["tags"]["_" + key] = event.data[key]
            #else:
            data["fields"][key] = event.data[key]
        #data["fields"]["id"] = iso_timestamp
        # event id is not set and is expected to be generated from database
        #event.id = iso_timestamp
        return data

    def get_metadata(self, bucket_id: str) -> dict:
        # TAGS need single quotes to be referenced correctly, everything else double quotes....
        # https://www.docs.influxdata.com/influxdb/v1.5/query_language/data_exploration/#common-issues-with-the-where-clause
        query = ''' SELECT * FROM "metadata" WHERE "id" = '{}' '''.format(bucket_id)
        generator = self.db.query(query=query,epoch=self.time_precision).get_points()
        # Does return an extra parameter compared to other databases
        metadata = next(generator) # only should be one 
        return metadata

    def get_events(self, bucket_id: str, limit: int,
                   starttime: Optional[datetime]=None, endtime: Optional[datetime]=None) -> List[Event]:
        _starttime = starttime.isoformat() if starttime else starttime
        _endtime = endtime.isoformat() if endtime else endtime
        # single quotes for date time string or it will break due to influxdb specs
        query = 'SELECT * FROM "{}" '.format(bucket_id)
        if endtime or starttime:
            query += ' WHERE '
            if starttime:
                query += "time >= '{}' ".format(_starttime)
            if starttime and endtime:
                query += "AND "
            if endtime:
                query += "time <= '{}' ".format(_endtime)
        query += 'ORDER BY time DESC '
        if limit and limit > -1:
            query += 'LIMIT {} '.format(limit)
        result = self.db.query(query=query, epoch=self.time_precision)
        events = self._event_parser_from(result)
        return events

    def get_eventcount(self, bucket_id: str,
                   starttime: Optional[datetime]=None, endtime: Optional[datetime]=None) -> int:
        query = 'SELECT COUNT (id) FROM "{}"'.format(bucket_id)
        generator = self.db.query(query=query, epoch=self.time_precision).get_points()
        result = 0
        for item in generator:
            result = item["count"]
            # TODO possibly log error if item is greater than 1
        return result

    def insert_one(self, bucket_id: str, event: Event) -> Event:
        data = self._event_parser_to(bucket_id ,event)
        self.db.write_points(points=[data], time_precision=self.time_precision)
        # I don't think setting the event.id is needed since insert_many with other databases will break
        # However it will break replace() if an event had not been hit by replace_last() at least once
        event.id = self._get_last_event_id(bucket_id)
        return event


    def insert_many(self, bucket_id: str, events: List[Event]) -> None:
        bulk = []
        for event in events:
            data, event = self._event_parser_to(bucket_id, event)
            bulk.append(data)
        self.db.write_points(points=bulk, time_precision=self.time_precision)

    def delete(self, bucket_id: str, event_id: int) -> bool:
        query = 'DELETE FROM "{}" WHERE "id" = {} LIMIT 1'.format(bucket_id, event_id)
        self.db.query(query=query, epoch=self.time_precision)

    

    def _get_last_event_id(self, bucket_id):
        """ Gets the last id based on the "time" field in the database """
        query = 'SELECT last(id) FROM "{}"'.format(bucket_id)
        generator = self.db.query(query=query, epoch=self.time_precision).get_points()
        result = None
        for item in generator:
            result = item["time"]
        # TODO log error if result is empty
        return result

    def replace(self, bucket_id: str, event_id: int, event: Event) -> Event:
        # Influx automatically handles duplicates by replacing them with the latest
        # This is based on the default database retention policy as long as the time + bucket_id is the same (exactly)
        event.id = event_id
        return self.insert_one(bucket_id, event)
    
    def replace_last(self, bucket_id: str, event: Event) -> None:
        event.id = self._get_last_event_id(bucket_id)
        return self.insert_one(bucket_id, event)
    