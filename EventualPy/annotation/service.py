from abc import ABC, abstractmethod
import threading

import asyncio
import rx
from asyncio import Task
from datetime import timedelta
from typing import Dict, List, Callable, Awaitable

from rx.core.typing import Disposable

from EventualPy.annotation.models import Annotation
from EventualPy.data_entry.models import DataEntryField


class AbstractAnnotationService(ABC):
    @abstractmethod
    async def annotate(self, field: DataEntryField) -> Task:
        raise NotImplementedError()

    @abstractmethod
    async def acknowledge(self, field: DataEntryField, annotation: Annotation) -> Task:
        raise NotImplementedError()


class InMemoryAnnotationService(AbstractAnnotationService):
    def __init__(
        self,
        publish_annotation: Callable[[int, Annotation], Awaitable[None]],
        republish_interval: timedelta,
        fail_ack_every: int,
        fail_annotate_every: int,
    ):
        self._publish_annotation = publish_annotation
        self._republish_interval = republish_interval
        self._fail_ack_every = fail_ack_every
        self._fail_annotate_every = fail_annotate_every

        self._ack_lock = threading.Lock()
        self._annotate_lock = threading.Lock()
        self._pub_lock = threading.Lock()
        self._ack_increment = 0
        self._annotate_increment = 0

        self._annotations: Dict[int, List[Annotation]] = {}
        self._publications: List[Task] = []
        self._schedule = self._schedule_republish()

    def acknowledge(self, field: DataEntryField, annotation: Annotation) -> None:

        will_succeed = True
        with self._ack_lock:
            self._ack_increment += 1
            will_succeed = self._ack_increment % self._fail_ack_every != 0

        if will_succeed:
            ackables: List[Annotation] = self._annotations.get(field.id, [])

            for ackable in ackables:
                if ackable.id == annotation.id:
                    ackable.acknowledged = True
                    break

    async def annotate(self, field: DataEntryField) -> None:

        will_succeed = True
        # See if we should fail this request. Simulate some rate of failure
        with self._annotate_lock:
            # We also need a fake a DB PK identifier since this is in memory
            annotation_id = self._annotate_increment + 1
            self._annotate_increment = annotation_id
            will_succeed = self._annotate_increment % self._fail_ack_every != 0

        if will_succeed:
            field_annotations: List[Annotation] = self._annotations.get(field.id)

            # Add the annotations list to the dictionary if we need to
            if field_annotations is None:
                field_annotations = []
                self._annotations[field.id] = field_annotations

            annotation = None
            # If we already have annotations for this field, we assume the last one is the most desirable
            # TODO: Support multiple annotations / acks per field ID + context
            if len(field_annotations) > 0:
                annotation = field_annotations[-1]
            else:
                annotation = Annotation(
                    id=annotation_id, data=object(), acknowledged=False
                )
                field_annotations.append(annotation)

        with self._pub_lock:
            print(f"pub {field.id}")
            self._publications.append(
                asyncio.create_task((self._publish_annotation(field.id, annotation)))
            )

    def _schedule_republish(self) -> Disposable:
        sched = rx.interval(self._republish_interval).subscribe(
            on_next=self._republish_annotations
        )

        return sched

    def _republish_annotations(self, _) -> None:
        # Clean up pubs that are done
        with self._pub_lock:
            running_pubs = []
            for pub in self._publications:
                if pub.done() is False:
                    running_pubs.append(pub)
            self._publications = running_pubs

            loop = asyncio.new_event_loop()

            for field_id, field_annotations in self._annotations.items():
                for annotation in field_annotations:
                    if annotation.acknowledged is False:
                        print(f"repub {field_id}")
                        self._publications.append(
                            loop.create_task(
                                (self._publish_annotation(field_id, annotation))
                            )
                        )
