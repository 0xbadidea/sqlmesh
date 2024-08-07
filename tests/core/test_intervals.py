from sqlmesh.utils.date import to_datetime
from sqlmesh.core.snapshot.intervals import Interval, Intervals


def test_interval():
    i1 = Interval(to_datetime("2023-01-01"), to_datetime("2023-01-02"))
    i2 = Interval(to_datetime("2023-01-02"), to_datetime("2023-01-03"))
    i3 = Interval(to_datetime("2023-01-01"), to_datetime("2023-01-02"))

    assert str(i1) == "Interval[start=2023-01-01 00:00:00,end=2023-01-02 00:00:00]"

    assert i1 < i2
    assert i2 > i1
    assert i1 >= i1
    assert i1 <= i1
    assert i1 == i1
    assert not i1 >= i2
    assert not i2 <= i1
    assert i1 == i3
    assert i2 != i3

    assert i1 == (to_datetime("2023-01-01"), to_datetime("2023-01-02"))
    assert i1 != (to_datetime("2023-01-02"), to_datetime("2023-01-03"))
    assert i1 < (to_datetime("2023-01-02"), to_datetime("2023-01-03"))
    assert (to_datetime("2023-01-02"), to_datetime("2023-01-03")) > i1

    lst = [i1, i2]

    assert i1 in lst
    assert i2 in lst
    assert i3 in lst  # because i1 and i3 are equal

    i4 = Interval(to_datetime("2023-01-04"), to_datetime("2023-01-05"))
    assert i4 not in lst


def test_interval_covers():
    i1 = Interval(to_datetime("2023-01-01"), to_datetime("2023-01-02"))
    i2 = Interval(to_datetime("2023-01-02"), to_datetime("2023-01-03"))

    assert i1.covers(to_datetime("2023-01-01"))
    assert i1.covers(to_datetime("2023-01-02"))
    assert i1.covers(to_datetime("2023-01-01 05:00:00"))
    assert not i1.covers(to_datetime("2023-01-02 00:00:01"))

    assert i1.covers(i1)
    assert not i1.covers(i2)
    assert not i2.covers(i1)

    assert i1.covers((to_datetime("2023-01-01 00:00:00"), to_datetime("2023-01-01 01:00:00")))
    assert i1.covers((to_datetime("2023-01-01 01:00:00"), to_datetime("2023-01-01 02:00:00")))
    assert i1.covers((to_datetime("2023-01-01 23:59:00"), to_datetime("2023-01-02 00:00:00")))

    assert not i1.covers((to_datetime("2023-01-01 12:00:00"), to_datetime("2023-01-02 12:00:00")))


def test_interval_contains():
    i = Interval(to_datetime("2023-01-01"), to_datetime("2023-01-02"))

    assert to_datetime("2023-01-01") in i
    assert to_datetime("2023-01-02") in i
    assert Interval(to_datetime("2023-01-01"), to_datetime("2023-01-02")) in i

    assert to_datetime("2023-01-02 00:01:00") not in i
    assert Interval(to_datetime("2022-12-31"), to_datetime("2023-01-02")) not in i
    assert Interval(to_datetime("2023-01-01"), to_datetime("2023-01-03")) not in i
    assert Interval(to_datetime("2023-01-01 12:00:00"), to_datetime("2023-01-01 18:00:00")) in i


def test_intervals_start_of_day_aligned():
    i = Intervals(cron="@daily", start=to_datetime("2023-01-01"), end=to_datetime("2023-01-05"))

    iterated = [i for i in i]

    assert (
        i.cron_intervals
        == i.data_intervals
        == iterated
        == [
            (to_datetime("2023-01-01"), to_datetime("2023-01-02")),
            (to_datetime("2023-01-02"), to_datetime("2023-01-03")),
            (to_datetime("2023-01-03"), to_datetime("2023-01-04")),
            (to_datetime("2023-01-04"), to_datetime("2023-01-05")),
        ]
    )


def test_intervals_midday_aligned():
    i = Intervals(cron="0 12 * * *", start=to_datetime("2023-01-01"), end=to_datetime("2023-01-05"))

    assert i.cron_intervals == [
        (to_datetime("2023-01-01 12:00:00"), to_datetime("2023-01-02 12:00:00")),
        (to_datetime("2023-01-02 12:00:00"), to_datetime("2023-01-03 12:00:00")),
        (to_datetime("2023-01-03 12:00:00"), to_datetime("2023-01-04 12:00:00")),
        (to_datetime("2023-01-04 12:00:00"), to_datetime("2023-01-05 12:00:00")),
    ]

    iterated = [i for i in i]

    assert (
        iterated
        == i.data_intervals
        == [
            (to_datetime("2023-01-01"), to_datetime("2023-01-02")),
            (to_datetime("2023-01-02"), to_datetime("2023-01-03")),
            (to_datetime("2023-01-03"), to_datetime("2023-01-04")),
            (to_datetime("2023-01-04"), to_datetime("2023-01-05")),
        ]
    )


def test_intervals_hourly():
    i = Intervals(
        cron="@hourly", start=to_datetime("2023-01-01"), end=to_datetime("2023-01-01 05:00:00")
    )

    assert (
        i.cron_intervals
        == i.data_intervals
        == [
            (to_datetime("2023-01-01 00:00:00"), to_datetime("2023-01-01 01:00:00")),
            (to_datetime("2023-01-01 01:00:00"), to_datetime("2023-01-01 02:00:00")),
            (to_datetime("2023-01-01 02:00:00"), to_datetime("2023-01-01 03:00:00")),
            (to_datetime("2023-01-01 03:00:00"), to_datetime("2023-01-01 04:00:00")),
            (to_datetime("2023-01-01 04:00:00"), to_datetime("2023-01-01 05:00:00")),
        ]
    )


def test_ready_intervals_start_of_day_aligned():
    i = Intervals(cron="@daily", start=to_datetime("2023-01-01"), end=to_datetime("2023-01-05"))

    assert i.ready(to_datetime("2023-01-01 00:00:00")) == []
    assert i.ready(to_datetime("2023-01-02 00:00:00")) == [
        (to_datetime("2023-01-01 00:00:00"), to_datetime("2023-01-02 00:00:00"))
    ]
    assert i.ready(to_datetime("2023-01-02 04:00:00")) == [
        (to_datetime("2023-01-01 00:00:00"), to_datetime("2023-01-02 00:00:00"))
    ]
    assert i.ready(to_datetime("2023-01-02 16:00:00")) == [
        (to_datetime("2023-01-01 00:00:00"), to_datetime("2023-01-02 00:00:00"))
    ]
    assert i.ready(to_datetime("2023-01-03 01:00:00")) == [
        (to_datetime("2023-01-01 00:00:00"), to_datetime("2023-01-02 00:00:00")),
        (to_datetime("2023-01-02 00:00:00"), to_datetime("2023-01-03 00:00:00")),
    ]
    assert i.ready(to_datetime("2023-01-10 00:00:00")) == [
        (to_datetime("2023-01-01 00:00:00"), to_datetime("2023-01-02 00:00:00")),
        (to_datetime("2023-01-02 00:00:00"), to_datetime("2023-01-03 00:00:00")),
        (to_datetime("2023-01-03 00:00:00"), to_datetime("2023-01-04 00:00:00")),
        (to_datetime("2023-01-04 00:00:00"), to_datetime("2023-01-05 00:00:00")),
    ]


def test_ready_intervals_midday_aligned():
    i = Intervals(cron="0 12 * * *", start=to_datetime("2023-01-01"), end=to_datetime("2023-01-05"))

    assert i.ready(to_datetime("2023-01-01 00:00:00")) == []
    assert i.ready(to_datetime("2023-01-02 00:00:00")) == []
    assert i.ready(to_datetime("2023-01-02 04:00:00")) == []
    assert i.ready(to_datetime("2023-01-02 16:00:00")) == [
        (to_datetime("2023-01-01 00:00:00"), to_datetime("2023-01-02 00:00:00"))
    ]
    assert i.ready(to_datetime("2023-01-03 01:00:00")) == [
        (to_datetime("2023-01-01 00:00:00"), to_datetime("2023-01-02 00:00:00")),
    ]
    assert i.ready(to_datetime("2023-01-03 13:00:00")) == [
        (to_datetime("2023-01-01 00:00:00"), to_datetime("2023-01-02 00:00:00")),
        (to_datetime("2023-01-02 00:00:00"), to_datetime("2023-01-03 00:00:00")),
    ]
    assert i.ready(to_datetime("2023-01-05 13:00:00")) == [
        (to_datetime("2023-01-01 00:00:00"), to_datetime("2023-01-02 00:00:00")),
        (to_datetime("2023-01-02 00:00:00"), to_datetime("2023-01-03 00:00:00")),
        (to_datetime("2023-01-03 00:00:00"), to_datetime("2023-01-04 00:00:00")),
        # no 04-05 because the cutoff is 05 00:00 based on the intervals end date
    ]
    assert i.ready(to_datetime("2023-01-10 00:00:00")) == [
        (to_datetime("2023-01-01 00:00:00"), to_datetime("2023-01-02 00:00:00")),
        (to_datetime("2023-01-02 00:00:00"), to_datetime("2023-01-03 00:00:00")),
        (to_datetime("2023-01-03 00:00:00"), to_datetime("2023-01-04 00:00:00")),
        # no 04-05 because the cutoff is 05 00:00 based on the intervals end date
    ]


def test_intervals_from_compacted():
    compacted_intervals = [
        (to_datetime("2023-01-01"), to_datetime("2023-01-05")),
        (to_datetime("2023-01-07"), to_datetime("2023-01-10")),
        (to_datetime("2023-01-28"), to_datetime("2023-02-05")),
    ]

    i = Intervals.from_compacted(cron="@daily", compacted_intervals=compacted_intervals)

    assert i.start == to_datetime("2023-01-01")
    assert i.end == to_datetime("2023-02-05")

    assert len(i.cron_intervals) == len(i.data_intervals) == 35


def test_missing_intervals_start_of_day_aligned():
    compacted_intervals = [
        (to_datetime("2023-01-01"), to_datetime("2023-01-05")),
        (to_datetime("2023-01-07"), to_datetime("2023-01-10")),
        (to_datetime("2023-01-11"), to_datetime("2023-01-15")),
    ]

    i = Intervals.from_compacted(cron="@daily", compacted_intervals=compacted_intervals)

    assert i.missing() == [
        (to_datetime("2023-01-05"), to_datetime("2023-01-06")),
        (to_datetime("2023-01-06"), to_datetime("2023-01-07")),
        (to_datetime("2023-01-10"), to_datetime("2023-01-11")),
    ]

    assert i.missing(to_datetime("2023-01-05 00:00:00")) == []
    assert i.missing(to_datetime("2023-01-05 12:00:00")) == []
    assert i.missing(to_datetime("2023-01-06 00:00:00")) == [
        (to_datetime("2023-01-05"), to_datetime("2023-01-06")),
    ]


def test_missing_intervals_midday_aligned():
    i = Intervals(
        cron="0 12 * * *", start=to_datetime("2023-01-01"), end=to_datetime("2023-01-05 12:00:00")
    )

    i.mark_present([(to_datetime("2023-01-01"), to_datetime("2023-01-04"))])

    assert i.missing(to_datetime("2023-01-05 00:00:00")) == []
    assert i.missing(to_datetime("2023-01-05 04:00:00")) == []
    assert i.missing(to_datetime("2023-01-05 12:01:00")) == [
        (to_datetime("2023-01-04"), to_datetime("2023-01-05"))
    ]


def test_missing_intervals_with_lookback_start_of_day_aligned():
    i = Intervals(
        cron="@daily", start=to_datetime("2023-01-01"), end=to_datetime("2023-01-05 00:00:00")
    )

    i.mark_present([(to_datetime("2023-01-01"), to_datetime("2023-01-04"))])

    assert i.missing(to_datetime("2023-01-04 12:00:00"), lookback=2) == []
    assert i.missing(to_datetime("2023-01-05 00:00:00"), lookback=2) == [
        (to_datetime("2023-01-02"), to_datetime("2023-01-03")),
        (to_datetime("2023-01-03"), to_datetime("2023-01-04")),
        (to_datetime("2023-01-04"), to_datetime("2023-01-05")),
    ]
    assert i.missing(to_datetime("2023-01-05 00:00:00"), lookback=1) == [
        (to_datetime("2023-01-03"), to_datetime("2023-01-04")),
        (to_datetime("2023-01-04"), to_datetime("2023-01-05")),
    ]


def test_missing_intervals_with_lookback_midday_aligned():
    i = Intervals(
        cron="0 12 * * *", start=to_datetime("2023-01-01"), end=to_datetime("2023-01-06 00:00:00")
    )

    i.mark_present([(to_datetime("2023-01-01"), to_datetime("2023-01-04"))])

    assert i.missing(to_datetime("2023-01-05 00:00:00"), lookback=2) == []
    assert i.missing(to_datetime("2023-01-05 04:00:00"), lookback=2) == []
    assert i.missing(to_datetime("2023-01-05 12:01:00"), lookback=2) == [
        (to_datetime("2023-01-02"), to_datetime("2023-01-03")),
        (to_datetime("2023-01-03"), to_datetime("2023-01-04")),
        (to_datetime("2023-01-04"), to_datetime("2023-01-05")),
    ]
    assert i.missing(to_datetime("2023-01-05 12:01:00"), lookback=1) == [
        (to_datetime("2023-01-03"), to_datetime("2023-01-04")),
        (to_datetime("2023-01-04"), to_datetime("2023-01-05")),
    ]


def test_missing_intervals_with_lookback_align_with_earliest():
    i = Intervals(
        cron="0 12 * * *", start=to_datetime("2023-01-01"), end=to_datetime("2023-01-10 00:00:00")
    )

    i.mark_present(
        [
            (to_datetime("2023-01-01"), to_datetime("2023-01-04")),
            (to_datetime("2023-01-05"), to_datetime("2023-01-10")),
        ]
    )

    assert i.missing(to_datetime("2023-01-05 00:00:00"), lookback=1) == []
    assert i.missing(to_datetime("2023-01-05 12:00:00"), lookback=1) == [
        (to_datetime("2023-01-03"), to_datetime("2023-01-04")),
        (to_datetime("2023-01-04"), to_datetime("2023-01-05")),
    ]
    assert i.missing(to_datetime("2023-01-06 00:00:00"), lookback=1) == [
        (to_datetime("2023-01-03"), to_datetime("2023-01-04")),
        (to_datetime("2023-01-04"), to_datetime("2023-01-05")),
    ]
    assert i.missing(to_datetime("2023-01-06 12:00:00"), lookback=1) == [
        (to_datetime("2023-01-03"), to_datetime("2023-01-04")),
        (to_datetime("2023-01-04"), to_datetime("2023-01-05")),
        # even though 2023-01-06 is marked as present, because its predecessor has to be backfilled due to lookback then it does as well
        (to_datetime("2023-01-05"), to_datetime("2023-01-06")),
    ]
    assert i.missing(to_datetime("2023-01-07 00:00:00"), lookback=1) == [
        (to_datetime("2023-01-03"), to_datetime("2023-01-04")),
        (to_datetime("2023-01-04"), to_datetime("2023-01-05")),
        (to_datetime("2023-01-05"), to_datetime("2023-01-06")),
    ]
    assert i.missing(to_datetime("2023-01-07 12:00:00"), lookback=1) == [
        (to_datetime("2023-01-03"), to_datetime("2023-01-04")),
        (to_datetime("2023-01-04"), to_datetime("2023-01-05")),
        (to_datetime("2023-01-05"), to_datetime("2023-01-06")),
        (to_datetime("2023-01-06"), to_datetime("2023-01-07")),
    ]
    assert i.missing(to_datetime("2023-01-10 12:00:00"), lookback=1) == [
        (to_datetime("2023-01-03"), to_datetime("2023-01-04")),
        (to_datetime("2023-01-04"), to_datetime("2023-01-05")),
        (to_datetime("2023-01-05"), to_datetime("2023-01-06")),
        (to_datetime("2023-01-06"), to_datetime("2023-01-07")),
        (to_datetime("2023-01-07"), to_datetime("2023-01-08")),
        (to_datetime("2023-01-08"), to_datetime("2023-01-09")),
        # note: 09-10 not ready because the end date on the original Intervals was 2023-01-10 00:00:00 and not 2023-01-10 12:00:00
    ]


def test_missing_intervals_with_lookback_start_of_day_aligned_dont_try_to_go_beyond_start():
    i = Intervals(cron="@daily", start=to_datetime("2023-01-01"), end=to_datetime("2023-01-05"))
    assert i.missing(to_datetime("2023-01-01 00:00:00")) == []
    assert i.missing(to_datetime("2023-01-01 00:00:00"), lookback=1) == []
    assert i.missing(to_datetime("2023-01-02 00:00:00")) == [
        (to_datetime("2023-01-01"), to_datetime("2023-01-02"))
    ]
    assert i.missing(to_datetime("2023-01-02 00:00:00"), lookback=1) == [
        (to_datetime("2023-01-01"), to_datetime("2023-01-02"))
    ]

    i.mark_present([(to_datetime("2023-01-01"), to_datetime("2023-01-02"))])
    assert i.missing(to_datetime("2023-01-02 00:00:00")) == []
    assert (
        i.missing(to_datetime("2023-01-02 00:00:00"), lookback=1) == []
    )  # there has to be a missing interval to trigger lookback
    assert i.missing(to_datetime("2023-01-03 00:00:00"), lookback=1) == [
        (to_datetime("2023-01-01"), to_datetime("2023-01-02")),
        (to_datetime("2023-01-02"), to_datetime("2023-01-03")),
    ]
    assert i.missing(to_datetime("2023-01-03 00:00:00"), lookback=10) == [
        (to_datetime("2023-01-01"), to_datetime("2023-01-02")),
        (to_datetime("2023-01-02"), to_datetime("2023-01-03")),
    ]


def test_missing_intervals_with_lookback_midday_aligned_dont_try_to_go_beyond_start():
    i = Intervals(cron="0 12 * * *", start=to_datetime("2023-01-01"), end=to_datetime("2023-01-05"))
    assert i.missing(to_datetime("2023-01-01 00:00:00")) == []
    assert i.missing(to_datetime("2023-01-01 00:00:00"), lookback=1) == []
    assert i.missing(to_datetime("2023-01-02 00:00:00")) == []
    assert i.missing(to_datetime("2023-01-02 00:00:00"), lookback=1) == []
    assert i.missing(to_datetime("2023-01-03 00:00:00")) == [
        (to_datetime("2023-01-01"), to_datetime("2023-01-02"))
    ]
    assert i.missing(to_datetime("2023-01-03 00:00:00"), lookback=10) == [
        (to_datetime("2023-01-01"), to_datetime("2023-01-02"))
    ]

    i.mark_present([(to_datetime("2023-01-01"), to_datetime("2023-01-02"))])

    assert i.missing(to_datetime("2023-01-03 00:00:00")) == []
    assert (
        i.missing(to_datetime("2023-01-03 00:00:00"), lookback=1) == []
    )  # there has to be a missing interval to trigger lookback
    assert (
        i.missing(to_datetime("2023-01-03 00:00:00"), lookback=10) == []
    )  # there has to be a missing interval to trigger lookback
    assert i.missing(to_datetime("2023-01-04 00:00:00"), lookback=1) == [
        (to_datetime("2023-01-01"), to_datetime("2023-01-02")),
        (to_datetime("2023-01-02"), to_datetime("2023-01-03")),
    ]
    assert i.missing(to_datetime("2023-01-04 00:00:00"), lookback=10) == [
        (to_datetime("2023-01-01"), to_datetime("2023-01-02")),
        (to_datetime("2023-01-02"), to_datetime("2023-01-03")),
    ]


def test_missing_intervals_with_lookback_and_cutoff_start_of_day_aligned():
    i = Intervals(cron="@daily", start=to_datetime("2023-01-01"), end=to_datetime("2023-01-10"))

    assert (
        len(
            i.missing(
                to_datetime("2023-01-10"),
                lookback=1,
                cutoff_time=to_datetime("2023-01-05 00:00:00"),
            )
        )
        == 4
    )

    i.mark_present([(to_datetime("2023-01-01"), to_datetime("2023-01-05 00:00:00"))])

    assert (
        i.missing(to_datetime("2023-01-05"), lookback=1, cutoff_time=to_datetime("2023-01-05"))
        == []
    )
    assert i.missing(
        to_datetime("2023-01-06"), lookback=1, cutoff_time=to_datetime("2023-01-05")
    ) == [(to_datetime("2023-01-04"), to_datetime("2023-01-05"))]
    assert (
        i.missing(to_datetime("2023-01-07"), lookback=1, cutoff_time=to_datetime("2023-01-05"))
        == []
    )
    assert (
        i.missing(to_datetime("2023-01-10"), lookback=1, cutoff_time=to_datetime("2023-01-05"))
        == []
    )


def test_missing_intervals_with_lookback_and_cutoff_midday_aligned():
    i = Intervals(cron="0 12 * * *", start=to_datetime("2023-01-01"), end=to_datetime("2023-01-10"))

    assert (
        len(i.missing(to_datetime("2023-01-10"), lookback=1, cutoff_time=to_datetime("2023-01-05")))
        == 4
    )

    i.mark_present([(to_datetime("2023-01-01"), to_datetime("2023-01-05"))])

    assert (
        i.missing(
            to_datetime("2023-01-05 13:00:00"), lookback=1, cutoff_time=to_datetime("2023-01-05")
        )
        == []
    )
    assert (
        i.missing(
            to_datetime("2023-01-06 04:00:00"), lookback=1, cutoff_time=to_datetime("2023-01-05")
        )
        == []
    )
    assert i.missing(
        to_datetime("2023-01-06 13:00:00"), lookback=1, cutoff_time=to_datetime("2023-01-05")
    ) == [(to_datetime("2023-01-04"), to_datetime("2023-01-05"))]
    assert i.missing(
        to_datetime("2023-01-07 04:00:00"), lookback=1, cutoff_time=to_datetime("2023-01-05")
    ) == [(to_datetime("2023-01-04"), to_datetime("2023-01-05"))]
    assert (
        i.missing(
            to_datetime("2023-01-07 13:00:00"), lookback=1, cutoff_time=to_datetime("2023-01-05")
        )
        == []
    )
    assert (
        i.missing(
            to_datetime("2023-01-10 13:00:00"), lookback=1, cutoff_time=to_datetime("2023-01-05")
        )
        == []
    )


def test_missing_intervals_with_gaps_start_of_day_aligned():
    i = Intervals(cron="@daily", start=to_datetime("2023-01-01"), end=to_datetime("2023-01-10"))

    i.mark_present(
        [
            (to_datetime("2023-01-01 00:00:00"), to_datetime("2023-01-05 00:00:00")),
            (to_datetime("2023-01-06 00:00:00"), to_datetime("2023-01-10 00:00:00")),
        ]
    )

    assert i.missing() == [(to_datetime("2023-01-05 00:00:00"), to_datetime("2023-01-06 00:00:00"))]

    i = Intervals(cron="@daily", start=to_datetime("2023-01-05"), end=to_datetime("2023-01-10"))
    i.mark_present(
        [
            (to_datetime("2023-01-01 00:00:00"), to_datetime("2023-01-05 00:00:00")),
            (to_datetime("2023-01-06 00:00:00"), to_datetime("2023-01-10 00:00:00")),
        ]
    )
    assert i.missing() == [(to_datetime("2023-01-05 00:00:00"), to_datetime("2023-01-06 00:00:00"))]


def test_missing_intervals_with_gaps_midday_aligned():
    i = Intervals(cron="0 12 * * *", start=to_datetime("2023-01-01"), end=to_datetime("2023-01-10"))

    i.mark_present(
        [
            (to_datetime("2023-01-01 00:00:00"), to_datetime("2023-01-05 00:00:00")),
            (to_datetime("2023-01-06 00:00:00"), to_datetime("2023-01-10 00:00:00")),
        ]
    )

    assert i.missing() == [(to_datetime("2023-01-05 00:00:00"), to_datetime("2023-01-06 00:00:00"))]

    i = Intervals(cron="0 12 * * *", start=to_datetime("2023-01-05"), end=to_datetime("2023-01-10"))

    i.mark_present(
        [
            (to_datetime("2023-01-01 00:00:00"), to_datetime("2023-01-05 00:00:00")),
            (to_datetime("2023-01-06 00:00:00"), to_datetime("2023-01-10 00:00:00")),
        ]
    )

    assert i.missing() == [(to_datetime("2023-01-05 00:00:00"), to_datetime("2023-01-06 00:00:00"))]


def test_missing_intervals_with_gaps_and_lookback_start_of_day_aligned():
    i = Intervals(cron="@daily", start=to_datetime("2023-01-05"), end=to_datetime("2023-01-10"))

    i.mark_present(
        [
            (to_datetime("2023-01-01 00:00:00"), to_datetime("2023-01-05 00:00:00")),
            (to_datetime("2023-01-06 00:00:00"), to_datetime("2023-01-08 00:00:00")),
        ]
    )

    assert i.missing(lookback=2) == [
        # this one isnt missing due to lookback, it's missing due to never being marked present
        (to_datetime("2023-01-05 00:00:00"), to_datetime("2023-01-06 00:00:00")),
        (to_datetime("2023-01-06 00:00:00"), to_datetime("2023-01-07 00:00:00")),
        (to_datetime("2023-01-07 00:00:00"), to_datetime("2023-01-08 00:00:00")),
        (to_datetime("2023-01-08 00:00:00"), to_datetime("2023-01-09 00:00:00")),
        (to_datetime("2023-01-09 00:00:00"), to_datetime("2023-01-10 00:00:00")),
    ]
