import pytest
import my_swcpy
print(my_swcpy.__file__)
from my_swcpy import SWCConfig
from my_swcpy import SWCClient

from my_swcpy.schemas import League, Team, Player, Performance, Counts
from io import BytesIO
import pyarrow.parquet as pq
import pandas as pd
import logging

logging.basicConfig(level=logging.DEBUG)


def test_health_check():
    """Tests health check from SDK"""
    config = SWCConfig( swc_base_url="http://0.0.0.0:8000", swc_backoff=False)
    client = SWCClient(config)
    response = client.get_health_check()
    assert response.status_code == 200
    assert response.json() == {"message": "API health check successful"}


def test_list_leagues():
    """Tests get leagues from SDK"""
    config = SWCConfig(swc_base_url="http://0.0.0.0:8000", swc_backoff=False)
    client = SWCClient(config)
    leagues_response = client.list_leagues()
    # Assert the endpoint returned a list object
    assert isinstance(leagues_response, list)
    # Assert each item in the list is an instance of Pydantic League object
    for league in leagues_response:
        assert isinstance(league, League)
    # Asset that 5 League objects are returned
    assert len(leagues_response) == 5

def test_get_league_by_id():
    """Tests get league by id from SDK"""
    config = SWCConfig(swc_base_url="http://0.0.0.0:8000", swc_backoff=False)
    client = SWCClient(config)
    league_id = 5002
    league_response = client.get_league_by_id(league_id)
    # Assert the endpoint returned a League object
    assert isinstance(league_response, League)
    # Assert the League object has the expected id
    assert league_response.league_id == league_id


def test_get_counts():
    """Tests get counts from SDK"""
    config = SWCConfig(swc_base_url="http://0.0.0.0:8000", swc_backoff=False)
    client = SWCClient(config)
    counts_response = client.get_counts()
    # Assert the endpoint returned a Counts object
    assert isinstance(counts_response, Counts)
    # Assert the Counts object has the expected counts
    assert counts_response.player_count == 1018
    assert counts_response.team_count == 20
    assert counts_response.league_count == 5



def test_list_teams(): 
    """Tests get teams from SDK"""
    config = SWCConfig(swc_base_url="http://0.0.0.0:8000",swc_backoff=False)
    client = SWCClient(config)    
    teams_response = client.list_teams()
    # Assert the endpoint returned a list object
    assert isinstance(teams_response, list)
    # Assert each item in the list is an instance of Pydantic League object
    for team in teams_response:
        assert isinstance(team, Team)
    # Asset that 5 League objects are returned
    assert len(teams_response) == 20

def test_get_player_by_id():
    """Tests get player by id from SDK"""
    config = SWCConfig(swc_base_url="http://0.0.0.0:8000",swc_backoff=False)
    client = SWCClient(config)    
    player_response = client.get_player_by_id(1001)
    # Assert the endpoint returned a player object
    assert isinstance(player_response, Player)
    
    assert player_response.player_id == 1001


def test_get_player_performances():
    """Tests get player performances from SDK"""
    config = SWCConfig(swc_base_url="http://0.0.0.0:8000",swc_backoff=False)
    client = SWCClient(config)
    performances_response = client.list_performances(limit=200)
    # Assert the endpoint returned a list object
    assert isinstance(performances_response, list)
    # Assert each item in the list is an instance of Pydantic Performance object
    for performance in performances_response:
        assert isinstance(performance, Performance)
    # Asset that 100 Performance objects are returned
    assert len(performances_response) == 200



def test_bulk_player_file_parquet(): 
    """Tests bulk player download through SDK - Parquet"""

    config = SWCConfig(swc_base_url="http://0.0.0.0:8000",swc_backoff=False,swc_bulk_file_format = "parquet") 
    client = SWCClient(config)    

    player_file_parquet = client.get_bulk_player_file()

    # Assert the file has the correct number of records (including header)
    player_table = pq.read_table(BytesIO(player_file_parquet)) 
    player_df = player_table.to_pandas()
    assert len(player_df) == 1018

