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

if __name__ == "__main__":
    asyncio.run(main())



