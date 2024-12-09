import datetime
import re
import time
from pathlib import Path
from typing import Callable, TYPE_CHECKING, Optional

from wattson.cosimulation.control.messages.wattson_notification import WattsonNotification
from wattson.cosimulation.control.messages.wattson_notification_topic import WattsonNotificationTopic
from wattson.time.wattson_time_type import WattsonTimeType

if TYPE_CHECKING:
    from wattson.cosimulation.control.interface.wattson_client import WattsonClient


class WattsonTime:
    """
    A representation for time in both, the wall-clock perspective and the simulated time.
    Simulated time is defined at a different speed and with a given offset to wall-clock time, i.e.,
    the offset is derived from a representation of a wall-clock timestamp that corresponds to a
    simulation clock timestamp.
    """
    def __init__(self,
                 wall_clock_reference: float = None,
                 sim_clock_reference: float = None,
                 speed: float = 1):
        """
        Creates a new WattsonTime object.
        If no parameters are given, this instance represents the concurrent execution of both, the wall-clock and
        the simulation clock.
        @param wall_clock_reference: The start time of the simulation as wall-clock timestamp
        @param sim_clock_reference: The start time of the simulation in simulated time, i.e., the simulated timestamp
            that corresponds to the wall-clock timestamp
        @param speed: The speed factor between wall-clock and simulation clock. A value of 2 indicates that for each
            wall-clock second, two simulated seconds pass.
        """
        self._reference_wall: float = time.time() if wall_clock_reference is None else wall_clock_reference
        self._reference_sim: float = self._reference_wall if sim_clock_reference is None else sim_clock_reference
        self._speed = speed
        self._wall_clock_function = time.time
        self._sync_push: bool = False
        self._sync_pull: bool = False
        self._wattson_client: Optional['WattsonClient'] = None

    def __repr__(self):
        return f"W: {self.to_local_datetime(WattsonTimeType.WALL)} | S: {self.to_local_datetime(WattsonTimeType.SIM)} | SPEED: {self.speed}"

    @property
    def speed(self):
        return self._speed

    @property
    def reference_wall(self) -> float:
        return self._reference_wall

    @property
    def reference_sim(self) -> float:
        return self._reference_sim

    def set_speed(self, speed: float) -> bool:
        if not isinstance(speed, float) or speed <= 0:
            return False
        self._speed = speed
        self.push()
        return True

    def set_wall_clock_reference(self, wall_clock_reference: float) -> bool:
        if not isinstance(wall_clock_reference, float) or wall_clock_reference < 0:
            return False
        self._reference_wall = wall_clock_reference
        self.push()
        return True

    def set_sim_clock_reference(self, sim_clock_reference: float) -> bool:
        if not isinstance(sim_clock_reference, float) or sim_clock_reference < 0:
            return False
        self._reference_sim = sim_clock_reference
        self.push()
        return True

    def copy(self, safe: bool = True) -> 'WattsonTime':
        copied_time = WattsonTime(
            wall_clock_reference=self.reference_wall,
            sim_clock_reference=self.reference_sim,
            speed=self.speed
        )
        copied_time._wall_clock_function = self._wall_clock_function
        if not safe:
            copied_time._wattson_client = self._wattson_client
            copied_time._sync_pull = self._sync_pull
            copied_time._sync_push = self._sync_push
        return copied_time

    def enable_synchronization(
            self,
            wattson_client: 'WattsonClient',
            enable_pull: bool = True,
            enable_push: bool = True,
            prefer_local: bool = False
        ):
        """
        Synchronize this time instance with other participants in the simulation.
        @param wattson_client: The WattsonClient to use for simulation
        @param enable_pull: Whether to apply received time updates locally
        @param enable_push: Whether to submit local time to the WattsonServer
        @param prefer_local: Whether to initially submit the local time to the WattsonServer
        @return:
        """
        self._wattson_client = wattson_client
        self._wattson_client.subscribe(topic=WattsonNotificationTopic.WATTSON_TIME, callback=self._update_from_server)
        self._sync_pull = enable_pull
        self._sync_push = enable_push
        if prefer_local:
            self.push()
        else:
            self.pull()

    def _update_from_server(self, notification: WattsonNotification):
        wattson_time = notification.notification_data.get("wattson_time")
        if not isinstance(wattson_time, WattsonTime):
            return
        self.sync_from(wattson_time=wattson_time)

    def sync_from(self, wattson_time: 'WattsonTime', auto_push: bool = False):
        self._reference_wall = wattson_time.reference_wall
        self._reference_sim = wattson_time.reference_sim
        self._speed = wattson_time.speed
        self._wall_clock_function = wattson_time._wall_clock_function
        if auto_push:
            self.push()

    def pull(self):
        if not self._sync_pull:
            return False
        if self._wattson_client is None:
            return False
        self.sync_from(self._wattson_client.get_wattson_time())
        return True

    def push(self):
        if not self._sync_push:
            return False
        if self._wattson_client is None:
            return False
        self._wattson_client.set_wattson_time(self)

    def set_wallclock_function(self, wall_clock_function: Callable[[], float]):
        self._wall_clock_function = wall_clock_function
        self.push()

    def time(self, time_type: WattsonTimeType = WattsonTimeType.WALL) -> float:
        """
        Returns the current timestamp for the selected time type
        @param time_type: The type to return (wall or simulated)
        @return: The current timestamp
        """
        if time_type == WattsonTimeType.WALL:
            return self.wall_clock_time()
        return self.sim_clock_time()

    def wall_clock_time(self) -> float:
        """
        Returns the current (wall-clock) timestamp
        """
        return self._wall_clock_function()

    def sim_clock_time(self) -> float:
        """
        Returns the current simulation clock timestamp
        """
        sim_time_passed = self.passed_sim_clock_seconds()
        return self._reference_sim + sim_time_passed

    def start_timestamp(self, time_type: WattsonTimeType) -> float:
        """
        The timestamp the simulation started at in wall-clock time
        @param time_type: The time type to return (wall or simulation)
        @return: The wall-clock timestamp that the simulation started
        """
        if time_type == WattsonTimeType.WALL:
            return self._reference_wall
        return self._reference_sim

    def sim_start_time(self) -> float:
        """
        Returns the timestamp the simulation started at in sim-clock time
        @return:
        """
        return self.start_timestamp(time_type=WattsonTimeType.SIM)

    def wall_start_time(self) -> float:
        """
        Returns the timestamp the simulation started at in wall-clock time
        @return:
        """
        return self.start_timestamp(time_type=WattsonTimeType.WALL)

    def passed_seconds(self, time_type: WattsonTimeType) -> float:
        """
        Returns the number of seconds passed since the start of the simulation in wall clock time
        @param time_type: The time type to return (wall or simulation)
        @return The number of seconds passed since the start of the simulation
        """
        if time_type == WattsonTimeType.WALL:
            return self.passed_wall_clock_seconds()
        return self.passed_sim_clock_seconds()

    def passed_wall_clock_seconds(self) -> float:
        return self.wall_clock_time() - self._reference_wall

    def passed_sim_clock_seconds(self) -> float:
        """
        Returns the number of seconds passed since the start of the simulation in simulation clock time
        """
        return self.passed_wall_clock_seconds() * self.speed

    def iso_format(self, time_type: WattsonTimeType, timezone: datetime.tzinfo = datetime.timezone.utc) -> str:
        return self.to_datetime(time_type=time_type, timezone=timezone).isoformat()

    def start_datetime(self, time_type: WattsonTimeType, timezone: datetime.tzinfo = datetime.timezone.utc) -> datetime.datetime:
        """
        Returns a datetime object representing the simulation start time of the given time type
        @param time_type: The time type to return (wall or simulation)
        @param timezone: The timezone (tzinfo) to use.
        @return: The datetime object representing the simulation's start time
        """
        return datetime.datetime.fromtimestamp(self.start_timestamp(time_type=time_type), tz=timezone)

    def start_datetime_local(self, time_type: WattsonTimeType) -> datetime.datetime:
        return self.start_datetime(time_type=time_type).astimezone()

    def to_datetime(self,
                    time_type: WattsonTimeType,
                    timezone: datetime.tzinfo = datetime.timezone.utc) -> datetime.datetime:
        """
        Returns the represented time as a datetime.datetime object.
        @param time_type: The time type to return (wall or simulation)
        @param timezone: The timezone (tzinfo) to use.
        @return: The datetime object representing the current time
        """
        return datetime.datetime.fromtimestamp(self.time(time_type=time_type), tz=timezone)

    def to_utc_datetime(self, time_type: WattsonTimeType) -> datetime.datetime:
        return self.to_datetime(time_type=time_type, timezone=datetime.timezone.utc)

    def to_local_datetime(self, time_type: WattsonTimeType) -> datetime.datetime:
        return self.to_utc_datetime(time_type=time_type).astimezone()

    def file_name(self,
                  time_type: WattsonTimeType,
                  as_local: bool = False,
                  with_time: bool = True,
                  with_milliseconds: bool = False,
                  with_timestamp: bool = False,
                  force_dashes: bool = True) -> str:
        """
        Returns the current wall time as a datetime string for usage as a file name in ISO 8601.
        @param time_type: The time type to return (wall or simulation)
        @param as_local: Whether to use the local timezone or UTC.
        @param with_time: Whether to include the time besides the date in the string
        @param with_milliseconds: Whether to add milliseconds to the string (requires with_time = True)
        @param with_timestamp: Whether to add the timestamp to the string
        @param force_dashes: Replace dots and colons with dashes for better file system compatibility
        @return: A String representation of the current time to be used as a filename
        """
        dt = self.to_local_datetime(time_type=time_type) if as_local else self.to_utc_datetime(time_type=time_type)
        ts = self.time(time_type=time_type)
        date_format = "%Y-%m-%d"
        if with_time:
            date_format = f"{date_format}T%H:%M:%S"
            if with_milliseconds:
                date_format = f"{date_format}.%f"
            date_format = f"{date_format}%z"
        file_name = dt.strftime(date_format)
        if "%z" in date_format:
            file_name = file_name[:-2] + ":" + file_name[-2:]
        if with_timestamp:
            file_name = f"{file_name}TS{ts}"
        if force_dashes:
            file_name = file_name.replace(":", "-").replace(".", "-")
        return file_name

    @staticmethod
    def from_offset(timestamp: float, simulation_offset: float, speed: float = 1):
        reference_sim = timestamp + simulation_offset
        return WattsonTime(wall_clock_reference=timestamp, sim_clock_reference=reference_sim, speed=speed)

    @staticmethod
    def extract_timestamp_from_file(file: Path) -> Optional[float]:
        name = file.stem
        if "TS" not in name:
            return None
        timestamp_split = name.split("TS")[1]
        pattern = re.compile("[0-9]+[.-][0-9]+")
        timestamp_string = pattern.search(timestamp_split)
        if timestamp_string is None:
            return None
        try:
            timestamp = float(timestamp_string.string.replace("-", "."))
            return timestamp
        except ValueError:
            return None
