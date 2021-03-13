import threading
from abc import ABC
from datetime import timedelta
from typing import Callable, Dict, Awaitable

import asyncio
import rx
from rx.core.typing import Disposable

from EventualPy.annotation.models import Annotation
from EventualPy.data_entry.models import DataEntryField


class AbstractDataEntryService(ABC):
    async def annotate_field(self, field_id: int, annotation: Annotation) -> None:
        raise NotImplementedError()

    def start_publications(
        self,
        publish: Callable[[DataEntryField], Awaitable[None]],
        acknowledge: Callable[[int, Annotation], Awaitable[None]],
        interval: timedelta,
    ):
        raise NotImplementedError()


class InMemoryDataEntryService(AbstractDataEntryService):
    def __init__(self, field_count: int, create_interval: timedelta):
        self._field_count = field_count
        self._create_interval = create_interval
        self._next_id = 0

        self._fields: Dict[int, DataEntryField] = {}

        self._field_creation_sched = self._schedule_field_creation()
        self._publications = []
        self._publish = None
        self._acknowledge = None
        self._repub_schedule = None

    def start_publications(
        self,
        publish: Callable[[DataEntryField], None],
        acknowledge: Callable[[int, Annotation], None],
        interval: timedelta,
    ):
        self._publish = publish
        self._acknowledge = acknowledge
        self._repub_schedule = self._start_republishing()

    async def annotate_field(self, field_id: int, annotation: Annotation) -> None:

        # TODO: Add failure rate
        field = self._fields.get(field_id)
        if field:
            field.annotations.append(annotation)

            if self._acknowledge is not None:
                asyncio.create_task(self._acknowledge(field.id, annotation))

    def _schedule_field_creation(self) -> Disposable:
        sched = rx.interval(self._create_interval).subscribe(
            on_next=self._generate_field
        )
        return sched

    def _generate_field(self, _) -> None:
        # TODO: Find a way to cancel the schedule instead of this IF statement
        if len(self._fields) < self._field_count:
            self._next_id += 1
            field = DataEntryField(id=self._next_id, annotations=[])
            self._fields[self._next_id] = field

            loop = asyncio.new_event_loop()
            if self._publish is not None:
                self._publications.append(loop.create_task(self._publish(field)))

    def _start_republishing(self) -> Disposable:
        # TODO: Implement republish unannotated fields
        # TODO: Cleanup completed publications
        pass