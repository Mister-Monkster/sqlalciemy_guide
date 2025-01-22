from sqlalchemy import text, insert, select, func, cast, Integer, and_, update, func
from sqlalchemy.orm import aliased
from src.database import sync_engine, async_engine, session_factory, async_session_factory
from src.models import WorkersOrm, Base, ResumesOrm


class SyncORM:
    @staticmethod
    def create_tables():
        sync_engine.echo = False
        Base.metadata.drop_all(sync_engine)
        Base.metadata.create_all(sync_engine)
        sync_engine.echo = True

    @staticmethod
    def insert_workers():
        with session_factory() as session:
            worker_jack = WorkersOrm(username="Jack")
            worker_michael = WorkersOrm(username='Michael')
            session.add_all([worker_jack, worker_michael])
            session.flush()
            session.commit()

    @staticmethod
    def select_workers():
        with session_factory() as session:
            # worker_id = 1
            # worker_jack = session.get(WorkersOrm, worker_id)
            query = select(WorkersOrm)  #SELECT * FROM workers
            result = session.execute(query)
            workers = result.scalars().all()
            print(f"{workers=}")

    @staticmethod
    def update_worker(worker_id: int = 2, new_username: str = 'James'):
        with session_factory() as session:
            worker_michael = session.get(WorkersOrm, worker_id)
            worker_michael.username = new_username
            session.refresh(worker_michael)
            session.commit()

    @staticmethod
    def insert_resumes():
        with session_factory() as session:
            data = (
                {
                    "title": "Python Junior Developer",
                    "compensation": 50000,
                    "workload": 'fulltime',
                    "worker_id": 1
                },
                {
                    "title": "Python Разработчик",
                    "compensation": 150000,
                    "workload": 'fulltime',
                    "worker_id": 1
                },
                {
                    "title": "Python Data Engineer",
                    "compensation": 250000,
                    "workload": 'parttime',
                    "worker_id": 2
                },
                {
                    "title": "Data Scientist",
                    "compensation": 300000,
                    "workload": 'fulltime',
                    "worker_id": 2
                }
            )

            insert_resumes = insert(WorkersOrm).values(data)

            session.commit()

    @staticmethod
    def select_resumes_avg_compensation(like_language: str = "Python"):
        """
        select workload, avg(compensation)::int as avg_compensation
        from resumes
        where title like '%Python%' and compensation > 40000
        group by workload
        """
        with session_factory() as session:
            query = (
                select(ResumesOrm.workload,
                       cast(func.avg(ResumesOrm.compensation), Integer).label("avg_compensation"),
                       )
                .select_from(ResumesOrm)
                .filter(and_(ResumesOrm.title.contains(like_language),
                             ResumesOrm.compensation > 40000
                             ))
                .group_by(ResumesOrm.workload)
                .having(cast(func.avg(ResumesOrm.compensation), Integer) > 70000)
            )
            print(query.compile(compile_kwargs={"literal_binds": True}))
            res = session.execute(query)
            result = res.all()
            print(result[0].avg_compensation)


class AsyncORM:
    @staticmethod
    async def create_tables():
        async_engine.echo = False
        Base.metadata.drop_all(sync_engine)
        Base.metadata.create_all(sync_engine)
        async_engine.echo = True

    @staticmethod
    async def insert_workers():
        async with async_session_factory() as session:
            worker_jack = WorkersOrm(username="Jack")
            worker_michael = WorkersOrm(username='Michael')
            session.add_all([worker_jack, worker_michael])
            await session.flush()
            await session.commit()

    @staticmethod
    async def select_workers():
        async with async_session_factory() as session:
            # worker_id = 1
            # worker_jack = session.get(WorkersOrm, worker_id)
            query = select(WorkersOrm)  #SELECT * FROM workers
            result = await session.execute(query)
            workers = result.scalars().all()
            print(f"{workers=}")


    @staticmethod
    async def update_worker(worker_id: int = 2, new_username: str = "James"):
        async with async_session_factory() as session:
            worker_michael = await session.get(WorkersOrm, worker_id)
            worker_michael.username = new_username
            await session.commit()

    @staticmethod
    async def insert_resumes():
        async with async_session_factory() as session:
            data = (
                {
                    "title": "Python Junior Developer",
                    "compensation": 50000,
                    "workload": 'fulltime',
                    "worker_id": 1
                },
                {
                    "title": "Python Разработчик",
                    "compensation": 150000,
                    "workload": 'fulltime',
                    "worker_id": 1
                },
                {
                    "title": "Python Data Engineer",
                    "compensation": 250000,
                    "workload": 'parttime',
                    "worker_id": 2
                },
                {
                    "title": "Data Scientist",
                    "compensation": 300000,
                    "workload": 'fulltime',
                    "worker_id": 2
                }
            )

            insert_resumes = insert(ResumesOrm).values(data)
            await session.execute(insert_resumes)
            await session.commit()

    @staticmethod
    async def select_resumes_avg_compensation(like_language: str = "Python"):
        """
        select workload, avg(compensation)::int as avg_compensation
        from resumes
        where title like '%Python%' and compensation > 40000
        group by workload
        """
        async with async_session_factory() as session:
            query = (
                select(ResumesOrm.workload,
                       cast(func.avg(ResumesOrm.compensation), Integer).label("avg_compensation"),
                       )
                .select_from(ResumesOrm)
                .filter(and_(ResumesOrm.title.contains(like_language),
                             ResumesOrm.compensation > 40000
                             ))
                .group_by(ResumesOrm.workload)
                .having(cast(func.avg(ResumesOrm.compensation), Integer) > 70000)
            )
            print(query.compile(compile_kwargs={"literal_binds": True}))
            res = await session.execute(query)
            result = res.all()
            print(result[0].avg_compensation)


    @staticmethod
    async def insert_additional_resumes():
        async with async_session_factory() as session:
            workers = [
                {"username": "Artem"},  # id 3
                {"username": "Roman"},  # id 4
                {"username": "Petr"},  # id 5
            ]
            resumes = [
                {"title": "Python программист", "compensation": 60000, "workload": "fulltime", "worker_id": 3},
                {"title": "Machine Learning Engineer", "compensation": 70000, "workload": "parttime", "worker_id": 3},
                {"title": "Python Data Scientist", "compensation": 80000, "workload": "parttime", "worker_id": 4},
                {"title": "Python Analyst", "compensation": 90000, "workload": "fulltime", "worker_id": 4},
                {"title": "Python Junior Developer", "compensation": 100000, "workload": "fulltime", "worker_id": 5},
            ]
            insert_workers = insert(WorkersOrm).values(workers)
            insert_resumes = insert(ResumesOrm).values(resumes)
            await session.execute(insert_workers)
            await session.execute(insert_resumes)
            await session.commit()

    @staticmethod
    async def join_cte_subquery_window_func(like_language:str = "Python"):
        """
                    WITH helper2 AS(
                SELECT *, compensation-avg_workload_compensation AS compensation_diff
                FROM
                (SELECT
                    w.id,
                    w.username,
                    r.compensation,
                    r.workload,
                    avg(r.compensation) OVER (PARTITION BY workload)::int AS avg_workload_compensation
                FROM resumes r
                JOIN workers w ON r.worker_id = w.id) helper1
            )
            SELECT * FROM helper2
            ORDER BY compensation_diff DESC
        """
        async with async_session_factory() as session:
            r = aliased(ResumesOrm)
            w = aliased(WorkersOrm)
            subq = (
                select(
                    r,
                    w,
                    func.avg(r.compensation).over(partition_by=r.workload).cast(Integer).label("avg_workload_compensation"),
                )
                # .select_from(r)
                .join(r, r.worker_id == w.id).subquery("helper1")
            )
            cte = (select(
                subq.c.worker_id,
                subq.c.username,
                subq.c.compensation,
                subq.c.workload,
                subq.c.avg_workload_compensation,
                (subq.c.compensation - subq.c.avg_workload_compensation).label("compensation_diff"),
            )
                   .cte("helper2"))

            query = (
                select(cte).
                order_by(cte.c.compensation_diff.desc())
            )
            res = await session.execute(query)
            result = res.all()
            print(f"{result=}")
            # print(query.compile(compile_kwargs={"literal_binds": True}))