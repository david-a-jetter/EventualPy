from abc import ABC, abstractmethod
import threading

import asyncio
import rx
from asyncio import Task
from datetime import timedelta
from typing import Dict, Callable, Awaitable

from rx.core.typing import Disposable

from .models import Annotation
from ..data_entry.models import DataEntryField


class AbstractAnnotationService(ABC):
    @abstractmethod
    async def annotate(self, field: DataEntryField) -> Task:
        raise NotImplementedError()

    @abstractmethod
    async def acknowledge(self, field: DataEntryField, annotation: Annotation) -> Task:
        raise NotImplementedError()

    @property
    @abstractmethod
    def acknowledged_annotations(self) -> Dict[int, Annotation]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def unacknowledged_annotations(self) -> Dict[int, Annotation]:
        raise NotImplementedError()


class InMemoryAnnotationService(AbstractAnnotationService):
    def __init__(
        self,
        publish_annotation: Callable[[int, Annotation], Awaitable[None]],
        republish_interval: timedelta,
        fail_annotate_every: int,
        fail_ack_every: int,
    ):
        self._publish_annotation = publish_annotation
        self._republish_interval = republish_interval
        self._fail_ack_every = fail_ack_every
        self._fail_annotate_every = fail_annotate_every

        self._ack_lock = threading.Lock()
        self._annotate_lock = threading.Lock()
        self._ack_increment = 0
        self._annotate_increment = 0

        self._annotations: Dict[int, Annotation] = {}
        self._schedule = self._schedule_republish()

    @property
    def acknowledged_annotations(self) -> Dict[int, Annotation]:
        with self._annotate_lock:
            acked = dict(
                filter(
                    lambda kvp: kvp[1].acknowledged is True, self._annotations.items()
                )
            )

        return acked

    @property
    def unacknowledged_annotations(self) -> Dict[int, Annotation]:
        with self._annotate_lock:
            acked = dict(
                filter(
                    lambda kvp: kvp[1].acknowledged is False, self._annotations.items()
                )
            )

        return acked

    async def acknowledge(self, field_id: int, annotation: Annotation) -> None:

        will_succeed = True
        with self._ack_lock:
            self._ack_increment += 1
            will_succeed = self._ack_increment % self._fail_ack_every != 0

            if will_succeed:
                field_annotation: Annotation = self._annotations.get(field_id)

                if (
                    field_annotation is not None
                    and field_annotation.id == annotation.id
                ):
                    field_annotation.acknowledged = True

    async def annotate(self, field: DataEntryField) -> None:

        will_succeed = True
        # See if we should fail this request. Simulate some rate of failure
        with self._annotate_lock:
            # We also need a fake a DB PK identifier since this is in memory
            annotation_id = self._annotate_increment + 1
            self._annotate_increment = annotation_id
            will_succeed = self._annotate_increment % self._fail_ack_every != 0

            if will_succeed:
                field_annotation: Annotation = self._annotations.get(field.id)

                # Add the annotations list to the dictionary if we need to
                if field_annotation is None:
                    field_annotation = Annotation(
                        id=annotation_id, data=object(), acknowledged=False
                    )
                    self._annotations[field.id] = field_annotation

                asyncio.create_task(
                    (self._publish_annotation(field.id, field_annotation))
                )

    def _schedule_republish(self) -> Disposable:
        sched = rx.interval(self._republish_interval).subscribe(
            on_next=self._republish_annotations
        )

        return sched

    def _republish_annotations(self, _) -> None:
        loop = asyncio.new_event_loop()
        with self._annotate_lock:
            for field_id, field_annotation in self._annotations.items():
                if field_annotation.acknowledged is False:
                    loop.run_until_complete(
                        (self._publish_annotation(field_id, field_annotation))
                    )
