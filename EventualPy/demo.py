from asyncio import Task
from datetime import timedelta

import asyncio
from typing import Awaitable

from EventualPy.annotation.models import Annotation
from EventualPy.annotation.service import InMemoryAnnotationService
from EventualPy.data_entry.models import DataEntryField


async def print_stuff(field_id: int, annotation: Annotation) -> Awaitable[None]:
    print(field_id, annotation)


async def demo():
    print("A")
    interval = timedelta(seconds=1)
    service = InMemoryAnnotationService(print_stuff, interval, 100, 100)
    for i in range(0, 10):
        field = DataEntryField(id=i, annotations=[])
        asyncio.create_task(service.annotate(field))
        print(i)


def main():
    asyncio.run(demo())


if __name__ == "__main__":
    main()
