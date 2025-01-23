import datetime
from typing import Optional, Annotated

from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, func, text, CheckConstraint, Index, \
    PrimaryKeyConstraint
from database import Base, str_256
from sqlalchemy.orm import Mapped, mapped_column, relationship
import enum

intpk = Annotated[int, mapped_column(primary_key=True)]
created_at = Annotated[datetime.datetime, mapped_column(server_default=text("TIMEZONE( 'utc', now())"))]
updated_at = Annotated[datetime.datetime, mapped_column(server_default=text("TIMEZONE( 'utc', now())"),
                                                        onupdate=datetime.datetime.utcnow, )]


class WorkersOrm(Base):
    __tablename__ = 'workers'

    id: Mapped[intpk]
    username: Mapped[str]

    resumes: Mapped[list["ResumesOrm"]] = relationship(
        back_populates='worker',
    )

    resumes_parttime: Mapped[list["ResumesOrm"]] = relationship(
        back_populates="worker",
        primaryjoin="and_(WorkersOrm.id == ResumesOrm.worker_id, ResumesOrm.workload == 'parttime')",
        order_by="ResumesOrm.id.desc()",

    )



class Workload(enum.Enum):
    parttime = 'parttime'
    fulltime = 'fulltime'


class ResumesOrm(Base):
    __tablename__ = 'resumes'

    id: Mapped[intpk]
    title: Mapped[str_256]
    compensation: Mapped[Optional[int]]
    workload: Mapped[Workload]
    worker_id: Mapped[int] = mapped_column(ForeignKey("workers.id", ondelete="CASCADE"))
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]

    worker: Mapped["WorkersOrm"] = relationship(back_populates='resumes')

    __table_args__ = (
        PrimaryKeyConstraint("id", "identification"),
        Index("title_index", "title"),
        CheckConstraint("compensation > 0", name='check_compensation_positive')
    )


metadata_obj = MetaData()

workers_table = Table("workers",
                      metadata_obj,
                      Column("id", Integer, primary_key=True),
                      Column("username", String),
                      )
