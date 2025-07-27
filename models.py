"""SQLAlchemy models"""
from sqlalchemy import Column, ForeignKey, Integer, String, Float, Date
from sqlalchemy.orm import relationship

from database import Base


class Player(Base):
    __tablename__ = "player"

    player_id = Column(Integer, primary_key=True, index=True)
    gsis_id = Column(String, nullable=True)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    position = Column(String, nullable=False)
    last_changed_date = Column(Date, nullable=False)

    performances = relationship("Performance", back_populates="player")


    # Many-to-many relationship between Player and Team tables
    teams = relationship("Team", secondary="team_player", back_populates="players")    


class Performance(Base):
    __tablename__ = "performance"

    performance_id = Column(Integer, primary_key=True, index=True)
    week_number = Column(String, nullable=False)
    fantasy_points = Column(Float, nullable=False)
    last_changed_date = Column(Date, nullable=False)

    player_id = Column(Integer, ForeignKey("player.player_id"))

    player = relationship("Player", back_populates="performances")


class League(Base):
    __tablename__ = "league"

    league_id = Column(Integer, primary_key=True, index=True)
    league_name = Column(String, nullable=False)
    scoring_type = Column(String, nullable=False)
    league_size = Column(Integer, nullable=False) #new v1
    last_changed_date = Column(Date, nullable=False)

    teams = relationship("Team", back_populates="league")

#edit v0.2
class Team(Base):
    __tablename__ = "team"

    team_id = Column(Integer, primary_key=True, index=True)
    team_name = Column(String, nullable=False)
    last_changed_date = Column(Date, nullable=False)    

    league_id = Column(Integer, ForeignKey("league.league_id"))

    league = relationship("League", back_populates="teams")

    players = relationship("Player", secondary="team_player", back_populates="teams")

    weekly_scores = relationship('TeamWeek', back_populates='team')

class TeamWeek(Base):
    __tablename__ = "team_week"

    team_id = Column(Integer, ForeignKey("team.team_id"), primary_key=True, index=True)
    week_number = Column(String, ForeignKey("week.week_number"), primary_key=True, index=True)
    fantasy_points = Column(Float, nullable=False)
    last_changed_date = Column(Date, nullable=False)    

    team = relationship('Team', back_populates='weekly_scores')


class TeamPlayer(Base):
    __tablename__ = "team_player"

    team_id = Column(Integer, ForeignKey("team.team_id"), primary_key=True, index=True)
    player_id = Column(Integer, ForeignKey("player.player_id"), primary_key=True, index=True)
    last_changed_date = Column(Date, nullable=False)    

#updated v0.2
class Week(Base):
    __tablename__ = "week"

    week_number = Column(String, primary_key=True, index=True)
    ppr_8_max_points = Column(Float, nullable=False)
    ppr_10_max_points = Column(Float, nullable=False)
    ppr_12_max_points = Column(Float, nullable=False)
    ppr_14_max_points = Column(Float, nullable=False)
    half_ppr_8_max_points = Column(Float, nullable=False)
    half_ppr_10_max_points = Column(Float, nullable=False)
    half_ppr_12_max_points = Column(Float, nullable=False)
    half_ppr_14_max_points = Column(Float, nullable=False)
    std_8_max_points = Column(Float, nullable=False)
    std_10_max_points = Column(Float, nullable=False)
    std_12_max_points = Column(Float, nullable=False)
    std_14_max_points = Column(Float, nullable=False)
    last_changed_date = Column(Date, nullable=False)