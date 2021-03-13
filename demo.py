import asyncio
from datetime import timedelta

import rx

from EventualPy.annotation.service import InMemoryAnnotationService
from EventualPy.data_entry.service import InMemoryDataEntryService


async def demo():
    one_second = timedelta(seconds=1)
    field_count = 1000
    field = InMemoryDataEntryService(field_count, timedelta(milliseconds=1), 100)
    annotation = InMemoryAnnotationService(field.annotate_field, one_second, 100, 100)
    field.start_publications(annotation.annotate, annotation.acknowledge, one_second)

    def eval_state(counter: int):
        created_field_count = len(field._fields)
        annotation_count = len(annotation._annotations)
        unannotated_count = len(field.unannotated_fields)

        print(f"Counter: {counter}")
        print(f"Field Count: {created_field_count}")
        print(f"Annotation Count: {annotation_count}")
        print(f"Unannotated Field Count: {unannotated_count}")

        if created_field_count == field_count and unannotated_count == 0:
            print("Finally reached")
            raise Exception()

    rx.interval(timedelta(seconds=1)).subscribe(eval_state)


def main():
    asyncio.run(demo())


if __name__ == "__main__":
    main()
