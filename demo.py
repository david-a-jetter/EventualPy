import asyncio
from datetime import timedelta

import rx
from rx.operators import take_while

from EventualPy.annotation.service import InMemoryAnnotationService
from EventualPy.data_entry.service import InMemoryDataEntryService


async def demo():
    one_second = timedelta(seconds=1)
    max_field_count = 1000
    field = InMemoryDataEntryService(max_field_count, timedelta(milliseconds=1), 20)
    annotation = InMemoryAnnotationService(field.annotate_field, one_second, 20, 20)
    field.start_publications(annotation.annotate, annotation.acknowledge, one_second)

    created_field_count = 0
    annotation_count = 0
    unannotated_field_count = 0

    def consistent(_) -> bool:
        return created_field_count == max_field_count and unannotated_field_count == 0

    def inconsistent(_) -> bool:
        return not consistent(_)

    def eval_state(counter: int):
        nonlocal created_field_count
        nonlocal annotation_count
        nonlocal unannotated_field_count
        created_field_count = len(field._fields)
        annotation_count = len(annotation._annotations)
        unannotated_field_count = len(field.unannotated_fields)

        print(f"Counter: {counter}")
        print(f"Field Count: {created_field_count}")
        print(f"Annotation Count: {annotation_count}")
        print(f"Unannotated Field Count: {unannotated_field_count}")

        if consistent(None):
            print("Finally reached")

    sched = rx.interval(timedelta(seconds=1))
    composed = sched.pipe(take_while(inconsistent))
    composed.subscribe(eval_state)


def main():
    asyncio.run(demo())


if __name__ == "__main__":
    main()
