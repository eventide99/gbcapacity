# -*- coding: utf-8 -*-
"""
UK Settlement Period utilities.

A Period object representing a UK electricity settlement period (half-hour).
Can be instantiated with a period ID ('YYYY-MM-DDPXX') or UTC datetime.
"""

import re
import pandas as pd


class PeriodError(Exception):
    pass


class Period:

    def __init__(self, pid: str = None, utc_datetime: pd.Timestamp = None):
        if pid and utc_datetime:
            raise PeriodError("Provide either pid or utc_datetime, not both.")
        if not pid and not utc_datetime:
            self._initialize_from_utc(pd.Timestamp.utcnow())
        elif pid:
            self.pid = pid
        elif utc_datetime:
            self._initialize_from_utc(utc_datetime)
        else:
            raise ValueError("Either pid or utc_datetime must be provided.")

    def _initialize_from_utc(self, utc_datetime):
        if isinstance(utc_datetime, str):
            utc_datetime = pd.to_datetime(utc_datetime, utc=True)

        if utc_datetime.tzinfo is None:
            utc_datetime = utc_datetime.tz_localize('UTC')
        else:
            utc_datetime = utc_datetime.tz_convert('UTC')

        london_time = utc_datetime.tz_convert('Europe/London')
        settlement_date = london_time.date()
        settlement_date_midnight = pd.Timestamp(settlement_date).tz_localize('Europe/London')
        start_time_local = london_time
        time_difference = start_time_local - settlement_date_midnight
        settlement_period = (time_difference // pd.Timedelta(minutes=30)) + 1
        self.pid = f"{settlement_date}P{settlement_period:02d}"

    @property
    def settlement_date(self):
        return pd.to_datetime(self.pid[:10]).tz_localize('UTC').tz_convert('Europe/London').date()

    @property
    def settlement_period(self):
        return int(self.pid[-2:])

    @property
    def start_time_local(self):
        settlement_date_midnight = pd.Timestamp(self.settlement_date).tz_localize('Europe/London')
        return settlement_date_midnight + pd.Timedelta(minutes=30 * (self.settlement_period - 1))

    @property
    def end_time_local(self):
        return self.start_time_local + pd.Timedelta(minutes=30)

    @property
    def start_time_utc(self):
        return self.start_time_local.tz_convert('UTC')

    @property
    def end_time_utc(self):
        return self.end_time_local.tz_convert('UTC')

    @property
    def pid(self):
        return self._pid

    @pid.setter
    def pid(self, pid):
        pattern = r'^\d{4}-\d{2}-\d{2}P(0[1-9]|[1-4]\d|50)$'
        if not re.match(pattern, pid):
            raise PeriodError("Period must be in format 'YYYY-MM-DDPXX', where XX is 01-50.")

        date_str = pid[:10]
        period = int(pid[-2:])
        date = pd.to_datetime(date_str)
        year = date.year

        march_dates = pd.date_range(start=f"{year}-03-01", end=f"{year}-03-31")
        last_sunday_march = march_dates[march_dates.weekday == 6].max()

        october_dates = pd.date_range(start=f"{year}-10-01", end=f"{year}-10-31")
        last_sunday_october = october_dates[october_dates.weekday == 6].max()

        if date.date() == last_sunday_march.date() and period > 46:
            raise PeriodError("Short clock change day: period cannot exceed 46.")
        if date.date() == last_sunday_october.date() and period > 50:
            raise PeriodError("Long clock change day: period cannot exceed 50.")
        if date.date() != last_sunday_october.date() and period > 48:
            raise PeriodError("Regular days: period cannot exceed 48.")

        self._pid = pid

    def offset(self, periods: int):
        """Return a new Period offset by the given number of half-hours."""
        new_start_time_utc = self.start_time_utc + pd.Timedelta(minutes=30 * periods)
        return Period(utc_datetime=new_start_time_utc)
