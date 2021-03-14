import asyncio
from datetime import timedelta

import rx
from rx.operators import take_while

from EventualPy.annotation.service import InMemoryAnnotationService
from EventualPy.data_entry.service import InMemoryDataEntryService


async def demo():
    one_second = timedelta(seconds=1)
    max_field_count = 1000
    fail_every = 5
    data_entry_service = InMemoryDataEntryService(
        field_count=max_field_count,
        create_interval=timedelta(milliseconds=1),
        fail_annotate_every=fail_every)
    annotation_service = InMemoryAnnotationService(
        publish_annotation=data_entry_service.annotate_field,
        republish_interval=one_second,
        fail_annotate_every=fail_every,
        fail_ack_every=fail_every)
    data_entry_service.start_publications(
        publish=annotation_service.annotate,
        acknowledge=annotation_service.acknowledge,
        interval=one_second)

    created_field_count = 0
    acked_annotion_count = 0
    unannotated_field_count = 0
    unacked_annotion_count = 0

    def consistent(_) -> bool:
        return (
            created_field_count == max_field_count
            and unannotated_field_count == 0
            and unacked_annotion_count == 0
        )

    def inconsistent(_) -> bool:
        return not consistent(_)

    def eval_state(counter: int):
        nonlocal created_field_count
        nonlocal acked_annotion_count
        nonlocal unannotated_field_count
        nonlocal unacked_annotion_count
        created_field_count = data_entry_service.field_count
        acked_annotion_count = len(annotation_service.acknowledged_annotations)
        unacked_annotion_count = len(annotation_service.unacknowledged_annotations)
        unannotated_field_count = len(data_entry_service.unannotated_fields)

        print(f"Interation: {counter + 1}")
        print(f"Field Count: {created_field_count}")
        print(f"Acked Annotation Count: {acked_annotion_count}")
        print(f"Uncked Annotation Count: {unacked_annotion_count}")
        print(f"Unannotated Field Count: {unannotated_field_count}")
        print("")

        if consistent(None):
            print("---------------")
            print("Eventually has arrived")
            print("---------------")

    sched = rx.interval(timedelta(seconds=1))
    composed = sched.pipe(take_while(inconsistent))
    composed.subscribe(eval_state)


def main():
    asyncio.run(demo())
