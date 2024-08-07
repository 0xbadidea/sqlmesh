from datetime import datetime
import typing as t
import more_itertools
from sqlmesh.utils.date import TimeLike, to_datetime, to_timestamp, now
from sqlmesh.utils.cron import CroniterCache
from sqlmesh.core.snapshot.definition import Interval as PrimitiveInterval
from sqlmesh.core.node import IntervalUnit


class Interval:
    def __init__(self, start: datetime, end: datetime):
        self.start = start
        self.end = end
        self.missing = True

    def _range(self, what: t.Self | PrimitiveInterval) -> t.Tuple[datetime, datetime]:
        if isinstance(what, Interval):
            return what.start, what.end
        return to_datetime(what[0]), to_datetime(what[1])

    def __gt__(self, other: t.Self | PrimitiveInterval) -> bool:
        """An interval is greater than another interval if it starts after it"""
        other_start, _ = self._range(other)
        return self.start > other_start

    def __lt__(self, other: t.Self | PrimitiveInterval) -> bool:
        """An interval is less than another interval if it starts before it"""
        other_start, _ = self._range(other)
        return self.start < other_start

    def __eq__(self, other: object) -> bool:
        """An interval is equal to another interval if the start and end times are identical"""
        if not isinstance(other, Interval) and not isinstance(other, tuple):
            return False
        other_start, other_end = self._range(other)
        return self.start == other_start and self.end == other_end

    def __ge__(self, other: t.Self | PrimitiveInterval) -> bool:
        return self.__gt__(other) or self.__eq__(other)

    def __le__(self, other: t.Self | PrimitiveInterval) -> bool:
        return self.__lt__(other) or self.__eq__(other)

    def __str__(self) -> str:
        format_str = "%Y-%m-%d %H:%M:%S"
        return (
            f"Interval[start={self.start.strftime(format_str)},end={self.end.strftime(format_str)}]"
        )

    def __repr__(self) -> str:
        return self.__str__()

    def __hash__(self) -> int:  # so it can be used in sets
        return hash(self.to_primitive())

    def __contains__(self, other: t.Self | PrimitiveInterval | datetime) -> bool:
        return self.covers(other)

    def covers(self, other: t.Self | PrimitiveInterval | datetime) -> bool:
        start, end = (other, other) if isinstance(other, datetime) else self._range(other)
        return start >= self.start and end <= self.end

    def to_primitive(self) -> PrimitiveInterval:
        return (to_timestamp(self.start), to_timestamp(self.end))

    @classmethod
    def from_primitive(cls, interval: PrimitiveInterval) -> t.Self:
        s, e = interval
        return cls(to_datetime(s), to_datetime(e))


class Intervals:
    """
    Represents a contiguous set of intervals from a start date to an end date
    """

    def __init__(
        self,
        cron: str,
        start: datetime,
        end: datetime,
        interval_unit: t.Optional[IntervalUnit] = None,
    ):
        self.cron = cron
        self.interval_unit = interval_unit or IntervalUnit.from_cron(cron)
        self.start = start
        self.end = end
        self._cron_intervals: t.List[Interval] = []
        self._data_intervals: t.List[Interval] = []

    def ready(self, current_time: datetime) -> t.List[Interval]:
        """
        Return a list of data intervals that are ready for processing, at at the :current_time
        """
        cron_intervals = self.cron_intervals
        data_intervals = self.data_intervals

        # find the most recent cron interval before the cutoff
        most_recent_cron = None

        def _find(lst: t.List[Interval], cutoff: datetime) -> t.Optional[Interval]:
            lst_peekable = more_itertools.peekable(lst)
            for item in lst_peekable:
                if item.end > cutoff:
                    return None

                next_item = lst_peekable.peek(None)
                if not next_item or next_item.end > cutoff:
                    return item

            return None

        most_recent_cron = _find(cron_intervals, min(current_time, self.end))
        if not most_recent_cron:
            return []

        return [i for i in data_intervals if i <= most_recent_cron]

    def mark_present(
        self,
        present_intervals: t.List[t.Self] | t.Sequence[PrimitiveInterval],
    ) -> None:
        """
        Tag all our data intervals that match items in the :intervals list as present

        The ones that are not present will be considered missing by Intervals.missing()
        """
        if len(present_intervals) == 0:
            return

        # test the first item to know if we are dealing with an Intervals or a PrimitiveInterval
        first_item = present_intervals[0]
        if isinstance(first_item, tuple):
            intervals = [
                Intervals.from_primitive(cron=self.cron, start=start, end=end)
                for start, end in t.cast(t.Sequence[PrimitiveInterval], present_intervals)
            ]
        else:
            intervals = t.cast(t.List[Intervals], present_intervals)

        for group in intervals:
            for present_interval in group.data_intervals:
                for interval in self.data_intervals:
                    if present_interval.covers(interval):
                        interval.missing = False

    def missing(
        self,
        current_time: t.Optional[datetime] = None,
        lookback: int = 0,
        cutoff_time: t.Optional[datetime] = None,
    ) -> t.List[Interval]:
        """
        Calculate the missing intervals, optionally with lookback applied
        Note that you should call mark_present() with the list of intervals that have already been filled before calling missing()

        Args:
            current_time: Show the missing intervals "as at" this time
            lookback: For any missing intervals, flag this many prior intervals as also missing
            cutoff_time: Only used when calculating lookback. This indicates the timestamp after which missing intervals will
                never be marked as present (in practice, the model end time). We dont want to process intervals after
                the :cutoff_time but we may want to re-process intervals leading up to the :cutoff_time if we are within :lookback
                periods of it
        """
        current_time = current_time or now()
        cutoff_time = cutoff_time or current_time  # TODO: or current_time + max lookback intervals?
        expected = self.ready(current_time)
        missing = [i for i in expected if i.missing]

        if lookback > 0 and len(missing) > 0:
            # if all the missing intervals are greater than the cutoff time, then only consider the most recent when calculating lookback
            # the cutoff time is the time of the last valid interval that would be persisted to state. It typically represents the model end date
            if all((i.start >= cutoff_time for i in missing)):
                missing = missing[-1:]

            lookback_intervals = self._generate_lookback(missing[0], lookback)

            # lookback is viral, everything needs to be refreshed from the earliest lookback interval
            # because each interval depends on the previous one
            min_interval = min(lookback_intervals + missing)
            missing = [e for e in expected if e >= min_interval and e.end <= cutoff_time]

        return missing

    def match(self, maybe_interval: Interval) -> t.Optional[Interval]:
        return next((i for i in self.data_intervals if i.covers(maybe_interval)), None)

    def _generate_lookback(self, interval: Interval, lookback: int) -> t.List[Interval]:
        croniter = self.interval_unit.croniter(interval.start)
        end = interval.start
        lookback_intervals = []
        for _ in range(lookback):
            start = croniter.get_prev()
            if start < self.start:
                break
            match = self.match(Interval(start=start, end=end))
            if not match:
                raise ValueError(
                    f"_generate_lookback generated a range ({start} -> {end}) not covered by an expected interval. This is a bug"
                )
            lookback_intervals.append(match)
            end = start
        return lookback_intervals

    @property
    def cron_intervals(self) -> t.List[Interval]:
        if not self._cron_intervals:
            self._cron_intervals = _intervals_until(self.croniter, self.start, self.end)
        return self._cron_intervals

    @property
    def data_intervals(self) -> t.List[Interval]:
        if not self._data_intervals:
            self._data_intervals = _intervals_until(
                self.interval_unit.croniter(self.start), self.start, self.end
            )
        return self._data_intervals

    @property
    def croniter(self) -> CroniterCache:
        return CroniterCache(self.cron, self.start)

    def __iter__(self) -> t.Iterable[Interval]:
        return iter(self.data_intervals)

    @classmethod
    def from_primitive(cls, cron: str, start: TimeLike, end: TimeLike) -> t.Self:
        return cls(cron=cron, start=to_datetime(start), end=to_datetime(end))

    @classmethod
    def from_compacted(
        cls,
        cron: str,
        compacted_intervals: t.List[PrimitiveInterval],
        start: t.Optional[TimeLike] = None,
        end: t.Optional[TimeLike] = None,
    ) -> t.Self:
        """
        Generates an Interval covering the full range of input PrimitiveInterval's
        If there are gaps, the gaps are marked as missing
        """
        if not start:
            start = min([i[0] for i in compacted_intervals])
        if not end:
            end = max(i[1] for i in compacted_intervals)

        intervals = cls(cron=cron, start=to_datetime(start), end=to_datetime(end))
        intervals.mark_present(compacted_intervals)
        return intervals


def _intervals_until(croniter: CroniterCache, start: datetime, end: datetime) -> t.List[Interval]:
    intervals = []
    current = start

    while next := croniter.get_next():
        if next != current:
            # this effectively prunes part intervals that can arise eg if you have a start date of '2023-01-01 00:00:00' but a cron schedule of '0 12 * * *'
            generated_interval_seconds = (next - current).total_seconds()
            if generated_interval_seconds == croniter.interval_seconds:
                intervals.append(Interval(start=current, end=next))

        current = next

        if next >= end:
            break

    return intervals
