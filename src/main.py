import asyncio
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], '..'))

from queries.orm import SyncORM, AsyncORM
from queries.core import SyncCore

async def main():
    if "--orm" in sys.argv and "--async" in sys.argv:
        await AsyncORM.create_tables()
        await AsyncORM.insert_workers()
        await AsyncORM.select_workers()
        await AsyncORM.update_worker()
        await AsyncORM.insert_resumes()
        await AsyncORM.select_resumes_avg_compensation()
        await AsyncORM.insert_additional_resumes()
        await AsyncORM.join_cte_subquery_window_func()

    if "--orm" in sys.argv and "--sync" in sys.argv:
        SyncORM.create_tables()
        SyncORM.insert_workers()
        SyncORM.select_workers()
        SyncORM.update_worker()
        SyncORM.insert_resumes()
        SyncORM.select_resumes_avg_compensation()
        SyncORM.insert_additional_resumes()
        SyncORM.select_workers_with_lazy_relationship()
        SyncORM.select_workers_with_joined_relationship()
        SyncORM.select_workers_with_selectin_relationship()
        SyncORM.select_workers_with_condition_relationship()
        SyncORM.select_workers_with_condition_relationship_contains_eagr()

if __name__ == "__main__":
    asyncio.run(main())



