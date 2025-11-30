from datetime import datetime, timedelta
from functools import cached_property
from typing import Any, List  # TYPE_CHECKING

from cron_descriptor import (CasingTypeEnum, ExpressionDescriptor,
                             FormatException, MissingFieldException)
from croniter import CroniterBadCronError, CroniterBadDateError, croniter
from pendulum.datetime import DateTime as pDateTime
from pendulum.tz.timezone import Timezone

from airflow import settings
from airflow.exceptions import AirflowTimetableInvalid
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables._cron import CronMixin
from airflow.timetables.base import (DagRunInfo, DataInterval, TimeRestriction,
                                     Timetable)
from airflow.utils.dates import cron_presets
from airflow.utils.timezone import convert_to_utc, make_aware, make_naive

CRON_DESCRIPTION_LOCALE = 'ru_RU'

# Календарь тут показывает случайный крон:
#   реализовано через заплатку: @property - _expression
#   причина: from airflow.www import views - "dag.timetable._expression"
#   т.е. в любом timetable от CronMixin должно быть свойство _expression: str


def _check_offset_assumption(dt: datetime):
    """
    Возвращает `dt` в часовом поясе текущей конфигурации Airflow,
    если значение TZ = UTC.

    Если `dt` не имеет часового пояса - возвращается `dt` без изменений
    (предполагается, что это уже локальное время)

    В случае, если часовой пояс есть и это НЕ UTC - будет ошибка!
    """

    if dt.utcoffset() == timedelta(0):
        return make_naive(dt)   # без указания tz = значение из конфига
    elif dt.utcoffset() is not None:
        raise ValueError("Неожиданный часовой пояс в расписании")

    return dt


def _is_schedule_fixed(expression: str) -> bool:
    """Figures out if the schedule has a fixed time (e.g. 3 AM every day).

    :return: True if the schedule has a fixed time, False if not.

    Detection is done by "peeking" the next two cron trigger time; if the
    two times have the same minute and hour value, the schedule is fixed,
    and we *don't* need to perform the DST fix.

    This assumes DST happens on whole minute changes (e.g. 12:59 -> 12:00).
    """
    cron = croniter(expression)
    next_a = cron.get_next(datetime)
    next_b = cron.get_next(datetime)
    return next_b.minute == next_a.minute and next_b.hour == next_a.hour


class MultiCronTimetable(CronMixin, Timetable):

    def __init__(self,
                 crons: str | List[str],
                 timezone: str | Timezone | None = None) -> None:

        if timezone:
            if isinstance(timezone, str):
                timezone = Timezone(timezone)
            self._timezone = timezone
        else:
            self._timezone = settings.TIMEZONE

        if isinstance(crons, str):
            crons = [crons]

        self._expressions = {cron_presets.get(cron, cron) for cron in crons}
        cron_descriptions = set()

        for cron_expression in self._expressions:
            descriptor = ExpressionDescriptor(
                expression=cron_expression,
                casing_type=CasingTypeEnum.Sentence,
                use_24hour_time_format=True,
                locale_code=CRON_DESCRIPTION_LOCALE
            )

            try:
                # checking for more than 5 parameters in Cron and avoiding evaluation for now,
                # as Croniter has inconsistent evaluation with other libraries
                if len(croniter(cron_expression).expanded) > 5:
                    raise FormatException()
                interval_description: str = descriptor.get_description()
            except (CroniterBadCronError, FormatException, MissingFieldException):
                interval_description = ""

            cron_descriptions.add(interval_description)

        self.description: str = " || ".join(cron_descriptions)

    def __eq__(self, other: Any) -> bool:
        """Both expression and timezone should match.

        This is only for testing purposes and should not be relied on otherwise.
        """
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._expressions == other._expressions and self._timezone == other._timezone

    @property
    def _expression(self):
        return next(iter(self._expressions))

    @property
    def summary(self) -> str:
        return ", ".join(self._expressions)

    @cached_property
    def _should_fix_dst(self) -> bool:
        # This is lazy so instantiating a schedule does not immediately raise
        # an exception. Validity is checked with validate() during DAG-bagging.
        return not all(_is_schedule_fixed(c) for c in self._expressions)

    def _get_next(self, current: pDateTime) -> pDateTime:
        """Get the first schedule after specified time, with DST fixed."""
        naive = make_naive(current, self._timezone)
        crons = {croniter(expression, start_time=naive)
                 for expression in self._expressions}
        earliest_dt: datetime = min(cron.get_next(datetime) for cron in crons)
        earliest_dt = _check_offset_assumption(earliest_dt)
        if not self._should_fix_dst:
            return convert_to_utc(make_aware(earliest_dt, self._timezone))
        delta = earliest_dt - naive
        result = current.in_timezone(self._timezone) + delta
        return convert_to_utc(result)  # type: ignore

    def _get_prev(self, current: pDateTime) -> pDateTime:
        """Get the first schedule before specified time, with DST fixed."""
        naive = make_naive(current, self._timezone)
        crons = {croniter(expression, start_time=naive)
                 for expression in self._expressions}
        latest_dt: datetime = max(cron.get_prev(datetime) for cron in crons)
        latest_dt = _check_offset_assumption(latest_dt)
        if not self._should_fix_dst:
            return convert_to_utc(make_aware(latest_dt, self._timezone))
        delta = naive - latest_dt
        result = current.in_timezone(self._timezone) - delta
        return convert_to_utc(result)  # type: ignore

    def validate(self) -> None:
        try:
            for cron_expression in self._expressions:
                croniter(cron_expression)
        except (CroniterBadCronError, CroniterBadDateError) as e:
            raise AirflowTimetableInvalid(str(e))

    def infer_manual_data_interval(self, *, run_after: pDateTime) -> DataInterval:
        # возвращаем время запуска, без расчётов с расписанием этого DAG
        # это видится более логичным для отображения ручных запусков

        return DataInterval.exact(run_after)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        if restriction.catchup:
            if last_automated_data_interval is not None:
                next_start_time = self._get_next(last_automated_data_interval.end)
            elif restriction.earliest is None:
                return None  # Don't know where to catch up from, give up.
            else:
                next_start_time = self._align_to_next(restriction.earliest)
        else:
            start_time_candidates = [self._align_to_next(pDateTime.utcnow())]
            if last_automated_data_interval is not None:
                start_time_candidates.append(
                    self._get_next(last_automated_data_interval.end))
            if restriction.earliest is not None:
                start_time_candidates.append(self._align_to_next(restriction.earliest))
            next_start_time = max(start_time_candidates)
        if restriction.latest is not None and restriction.latest < next_start_time:
            return None
        return DagRunInfo.interval(next_start_time, next_start_time)

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        from airflow.serialization.serialized_objects import decode_timezone

        return cls(data["expressions"], decode_timezone(data["timezone"]))

    def serialize(self) -> dict[str, Any]:
        from airflow.serialization.serialized_objects import encode_timezone

        return {"expressions": list(self._expressions), "timezone": encode_timezone(self._timezone)}


class MultiCronTimetablePlugin(AirflowPlugin):
    name = "multicron_plugin"
    timetables = [MultiCronTimetable]
