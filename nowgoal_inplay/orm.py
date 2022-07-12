from sqlalchemy import create_engine, ForeignKey
from sqlalchemy.orm import backref, relationship, sessionmaker, column_property, synonym
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import TypeDecorator
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy import String, Integer, Column, DateTime, cast, Date, Time, Boolean, DECIMAL
from sqlalchemy.exc import SQLAlchemyError
from contextlib import contextmanager
from decimal import Decimal
from datetime import date, datetime, timedelta

engine = create_engine(r'sqlite:///nowgoal.db')
Session = sessionmaker(bind=engine)
Base = declarative_base()


class Decimal2(TypeDecorator):
    impl = Integer

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if type(value)==str:
            if '/' in value:
                n1,n2=value.split('/')
                value=(Decimal(n1)+Decimal(n2))/2
            else:
                value=Decimal(value)
        if int(value * 1000 % 10):
            print(value)
            raise SQLAlchemyError('只接受两位小数：' + f'{value}')
        return int(value * 100)

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return Decimal(value) / 100


class league(Base):
    __tablename__ = 'league'
    id = Column(Integer, primary_key=True)
    name = Column(String(20))
    name_short = Column(String(3))
    seasons = relationship('season', back_populates='league')
    matches = relationship('match', back_populates='league')

    def __repr__(self):
        return f'{self.name_short} {self.id} {self.name}'


class season(Base):
    __tablename__ = 'season'
    id = Column(Integer, primary_key=True)
    name = Column(String(20))
    league = relationship('league', back_populates='seasons')
    league_id = Column(Integer, ForeignKey('league.id'))
    matches = relationship('match', back_populates='season')

    def __repr__(self):
        return f'{self.id} {self.league.name} {self.name}'


class match(Base):
    __tablename__ = 'match'
    id = Column(Integer, primary_key=True)
    league_id = Column(Integer, ForeignKey('league.id'))
    league = relationship('league', back_populates='matches')
    season_id = Column(Integer, ForeignKey('season.id'))
    season = relationship('season', back_populates='matches')
    round_id = Column(Integer)
    kickoff = Column(DateTime)
    home_id = Column('home', Integer, ForeignKey('team.id'))
    away_id = Column('away', Integer, ForeignKey('team.id'))
    score_half = Column('score_half', String(5))
    score_full = Column('score_full', String(5))
    home = relationship('team', foreign_keys=[home_id])
    away = relationship('team', foreign_keys=[away_id])
    odds_had = relationship('had',back_populates='match')
    odds_asian = relationship('asian', back_populates='match')
    odds_hilo = relationship('hilo', back_populates='match')


    def __repr__(self):
        return f'{self.league.name} {self.season.name} {self.home.name} {self.away.name}'


class team(Base):
    __tablename__ = 'team'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    namecn = Column('name_cht', String)
    pic_url = Column(String)

    def __repr__(self):
        return f'{self.name} {self.namecn}'


class bookmaker(Base):
    __tablename__ = 'bookmaker'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    def __repr__(self):
        return f'{self.id} {self.name}'


class had(Base):
    __tablename__ = 'odds_had'
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey('match.id'))
    match = relationship('match', back_populates='odds_had')
    bookmaker_id = Column(Integer, ForeignKey('bookmaker.id'))
    bookmaker = relationship('bookmaker')
    gametime = Column(Integer)
    foul_home = Column(Integer)
    foul_away = Column(Integer)
    score = Column(String(5))
    closed = Column(Boolean)
    odds_home = Column(Decimal2)
    odds_draw = Column(Decimal2)
    odds_away = Column(Decimal2)
    o1=synonym('odds_home')
    o2=synonym('odds_draw')
    o3=synonym('odds_away')
    updated = Column('update_time_str', DateTime)
    stage = Column(String(5))

    def __repr__(self):
        if self.closed:
            return f'{self.gametime} Closed'
        else:
            return f'{self.gametime} {self.odds_home} {self.odds_draw} {self.odds_away}'


class asian(Base):
    __tablename__ = 'odds_asian'
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey('match.id'))
    match = relationship('match', back_populates='odds_asian')
    bookmaker_id = Column(Integer, ForeignKey('bookmaker.id'))
    bookmaker = relationship('bookmaker')
    gametime = Column(Integer)
    foul_home = Column(Integer)
    foul_away = Column(Integer)
    score = Column(String(5))
    closed = Column(Boolean)
    odds_home = Column(Decimal2)
    handicap = Column(Decimal2)
    odds_away = Column(Decimal2)
    o1=synonym('odds_home')
    o2=synonym('handicap')
    o3=synonym('odds_away')
    updated = Column('update_time_str', DateTime)
    stage = Column(String(5))

    def __repr__(self):
        if self.closed:
            return f'{self.gametime} Closed'
        else:
            return f'{self.gametime} {self.handicap} {self.odds_home} {self.odds_away}'


class hilo(Base):
    __tablename__ = 'odds_hilo'
    id = Column(Integer, primary_key=True)
    match_id = Column(Integer, ForeignKey('match.id'))
    match = relationship('match', back_populates='odds_hilo')
    bookmaker_id = Column(Integer, ForeignKey('bookmaker.id'))
    bookmaker = relationship('bookmaker')
    gametime = Column(Integer)
    foul_home = Column(Integer)
    foul_away = Column(Integer)
    score = Column(String(5))
    closed = Column(Boolean)
    odds_hi = Column('odds_over', Decimal2)
    odds_line = Column('line', Decimal2)
    odds_lo = Column('odds_under', Decimal2)
    o1=synonym('odds_hi')
    o2=synonym('odds_line')
    o3=synonym('odds_lo')
    updated = Column('update_time_str', DateTime)
    stage = Column(String(5))

    def __repr__(self):
        if self.closed:
            return f'{self.gametime} Closed'
        else:
            return f'{self.gametime} {self.odds_hi} {self.odds_line} {self.odds_lo}'


@contextmanager
def session_scope():
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def query(obj):
    if obj.metadata == Base.metadata:
        with session_scope() as session:
            return session.query(obj)
