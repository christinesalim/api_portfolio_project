"""Pydantic schemas"""
from pydantic import BaseModel, ConfigDict
from typing import List
from datetime import date


class Performance(BaseModel):
    model_config = ConfigDict(from_attributes = True)
    performance_id : int
    player_id : int
    week_number : str
    fantasy_points : float
    last_changed_date : date
        


class PlayerBase(BaseModel):
    model_config = ConfigDict(from_attributes = True)    
    player_id : int
    gsis_id: str
    first_name : str
    last_name : str
    position : str
    last_changed_date : date

class Player(PlayerBase):
    model_config = ConfigDict(from_attributes = True)
    performances: List[Performance] = []


class TeamBase(BaseModel):
    model_config = ConfigDict(from_attributes = True)
    league_id : int
    team_id : int
    team_name : str
    last_changed_date : date

#added for v0.2
class Week(BaseModel):
    model_config = ConfigDict(from_attributes = True)
    week_number : str
    ppr_8_max_points : float
    ppr_10_max_points : float
    ppr_12_max_points : float
    ppr_14_max_points : float
    half_ppr_8_max_points : float
    half_ppr_10_max_points : float
    half_ppr_12_max_points : float
    half_ppr_14_max_points : float
    std_8_max_points : float
    std_10_max_points : float
    std_12_max_points : float
    std_14_max_points : float
    last_changed_date : date

#added for v0.2
class TeamWeek(BaseModel):
    model_config = ConfigDict(from_attributes = True)
    week_number : str
    fantasy_points : float
    last_changed_date : date


#updated for v0.2
class Team(TeamBase):
    model_config = ConfigDict(from_attributes = True)
    players: List[PlayerBase] = []
    weekly_scores: List[TeamWeek] = []

class League(BaseModel):
    model_config = ConfigDict(from_attributes = True)
    league_id : int
    league_name : str
    scoring_type : str
    league_size : int #added v1
    last_changed_date : date
    teams: List[TeamBase] = []

class Counts(BaseModel):
    league_count : int
    team_count : int
    player_count : int
    week_count : int #v0.2