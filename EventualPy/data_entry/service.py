import threading
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Callable, Dict, Awaitable, Iterable, List

import asyncio
import rx
from rx.core.typing import Disposable
from rx.operators import take_while

from EventualPy.annotation.models import Annotation
from EventualPy.data_entry.models import DataEntryField


class AbstractDataEntryService(ABC):
    @abstractmethod
    async def annotate_field(self, field_id: int, annotation: Annotation) -> None:
        raise NotImplementedError()

    @abstractmethod
    def start_publications(
        self,
        publish: Callable[[DataEntryField], Awaitable[None]],
        acknowledge: Callable[[int, Annotation], Awaitable[None]],
        interval: timedelta,
    ):
        raise NotImplementedError()

    @property
    @abstractmethod
    def unannotated_fields(self) -> Iterable[DataEntryField]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def field_count(self) -> int:
        raise NotImplementedError()


class InMemoryDataEntryService(AbstractDataEntryService):

    def __init__(
        self,
        field_count: int,
        create_interval: timedelta,
        fail_annotate_every: int,
    ):
        self._field_count = field_count
        self._create_interval = create_interval

        self._annotate_lock = threading.Lock()
        self._fail_annotate_every = fail_annotate_every
        self._annotate_attempt = 0

        self._fields: Dict[int, DataEntryField] = {}

        self._field_creation_sched = self._schedule_field_creation()

        self._repub_interval = None
        self._publish = None
        self._acknowledge = None
        self._repub_schedule = None

    @property
    def field_count(self) -> int:
        return len(self._fields)

    @property
    def unannotated_fields(self) -> List[DataEntryField]:
        unannotated_fields = []
        for field_id, field in self._fields.items():
            if field.annotation is None:
                unannotated_fields.append(field)

        return unannotated_fields

    def start_publications(
        self,
        publish: Callable[[DataEntryField], Awaitable[None]],
        acknowledge: Callable[[int, Annotation], Awaitable[None]],
        interval: timedelta,
    ):
        self._publish = publish
        self._acknowledge = acknowledge
        self._repub_interval = interval
        self._repub_schedule = self._start_republishing()

    async def annotate_field(self, field_id: int, annotation: Annotation) -> None:

        with self._annotate_lock:
            self._annotate_attempt += 1
            will_succeed = self._annotate_attempt % self._fail_annotate_every != 0
            if will_succeed:
                field = self._fields.get(field_id)
                if field:
                    field.annotation = annotation

                    if self._acknowledge is not None:
                        await self._acknowledge(field.id, annotation)

    def _schedule_field_creation(self) -> Disposable:
        interval = rx.interval(self._create_interval)
        composed = interval.pipe(
            take_while(lambda _: len(self._fields) < self._field_count)
        )
        subscription = composed.subscribe(on_next=self._generate_field)
        return subscription

    def _generate_field(self, counter) -> None:
        next_id = counter + 1
        field = DataEntryField(id=next_id, annotations=[])
        self._fields[next_id] = field

        loop = asyncio.new_event_loop()
        if self._publish is not None:
            loop.run_until_complete(self._publish(field))

    def _start_republishing(self) -> Disposable:
        sched = rx.interval(self._repub_interval).subscribe(
            on_next=self._republish_unannotated_fields
        )

        return sched

    def _republish_unannotated_fields(self, _) -> None:

        if self._publish is not None:

            loop = asyncio.new_event_loop()

            for field in self.unannotated_fields:
                loop.run_until_complete(self._publish(field))
