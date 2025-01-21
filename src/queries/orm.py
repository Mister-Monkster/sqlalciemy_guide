from sqlalchemy import text, insert, select, func, cast, Integer, and_
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
            for worker in data:
                title = worker["title"]
                compensation = worker["compensation"]
                workload = worker["workload"]
                worker_id = worker["worker_id"]
                session.add(
                    ResumesOrm(title=title,
                               compensation=compensation,
                               workload=workload,
                               worker_id=worker_id)
                )
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
        sync_engine.echo = False
        Base.metadata.drop_all(sync_engine)
        Base.metadata.create_all(sync_engine)
        sync_engine.echo = True

    @staticmethod
    async def insert_data():
        async with async_session_factory() as session:
            worker_bobr = WorkersOrm(username="Bobr")
            worker_volk = WorkersOrm(username='Volk')
            session.add_all([worker_bobr, worker_volk])
            await session.commit()
