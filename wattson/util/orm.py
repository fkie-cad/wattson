"""
This file contains all classes of the ORM (object relational mapping) used for
the communication with a database.
All of the classes must be declared here so they definitely use the same base.
"""

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer

# create a "base mapping" which must be passed to all classes here.
Base = declarative_base()


class RecvdASDU(Base): #type:ignore
    """Represents a received ASDU."""
    __tablename__ = "rcvdAsdus"
    id = Column(Integer, primary_key=True)
    type = Column(Integer)
    cot = Column(Integer)
    coa = Column(Integer)

    def __repr__(self):
        return "<ASDU(type='%s', cot='%s', coa='%s')>" % (
            self.type, self.cot, self.coa)
