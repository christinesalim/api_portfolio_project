import httpx
import my_swcpy.swc_config as config
from .schemas import League, Team, Player, Performance, Counts
from typing import List
import backoff
import logging
logger = logging.getLogger(__name__)


class SWCClient:
    """Interacts with the SportsWorldCenteral API.
       This SDK class simplifies the process of using the SWC Fantasy 
       Football API. It supports all the functions of the SWC API and returns
       validated data types.

       Typical usage example:

       client = SWCClient()
       response = client.get_health_check()

    """

    HEALTH_CHECK_ENDPOINT = "/"
    LIST_LEAGUES_ENDPOINT = "/v0/leagues/"
    LIST_PLAYERS_ENDPOINT = "/v0/players/"
    LIST_PERFORMANCES_ENDPOINT = "/v0/performances/"
    LIST_TEAMS_ENDPOINT = "/v0/teams/"
    GET_COUNTS_ENDPOINT = "/v0/counts/"

    BULK_FILE_BASE_URL = (
        "https://raw.githubusercontent.com/christinesalim/api_portfolio_project/main/bulk/"
    )

    def __init__(self, input_config: config.SWCConfig):
        """Constructor for the SWCClient class.

        Args:
            input_config (SWCConfig): Configuration object containing API settings.
        """
        logger.debug(f"Bulk file base URL: {self.BULK_FILE_BASE_URL}")

        logger.debug(f"Input config: {input_config}")

        self.swc_base_url = input_config.swc_base_url
        self.backoff = input_config.swc_backoff
        self.backoff_max_time = input_config.swc_backoff_max
        self.bulk_file_format = input_config.swc_bulk_file_format

        self.BULK_FILE_NAMES = {
            "players": "player_data",
            "leagues": "league_data",
            "performances": "performance_data",
            "teams": "team_data",
            "team_players": "team_player_data",
        }

        if self.backoff:
            self.call_api = backoff.on_exception(
                wait_gen=backoff.expo,
                exception=(httpx.RequestError, httpx.HTTPStatusError),
                max_time=self.swc_backoff_max_time,
                jitter=backoff.random_jitter,
            )(self.call_api)

        if self.bulk_file_format.lower() == "parquet":
            self.BULK_FILE_NAMES = {
                key: value + ".parquet" for key,value in self.BULK_FILE_NAMES.items()
            }
        else:
            self.BULK_FILE_NAMES = {
                key: value + ".csv" for key,value in self.BULK_FILE_NAMES.items()
            }
        logger.debug(f"Bulk file names: {self.BULK_FILE_NAMES}")


    def call_api(self, 
        api_endpoint: str,
        api_params: dict = None,
    ) -> httpx.Response:
        """Calls the SWC API with the specified endpoint and parameters.

        Args:
            api_endpoint (str): The API endpoint to call.
            api_params (dict, optional): Parameters to include in the API call.

        Returns:
            httpx.Response: The response from the API call.
        """
        if api_params:
            api_params = {key: val for key, val in api_params.items() if val is not None}

        try:
            with httpx.Client(base_url=self.swc_base_url) as client:
                logger.debug(f"base_url: {self.swc_base_url}, api_endpoint: {api_endpoint}, \
                api_params: {api_params} ")
                response = client.get(api_endpoint, params=api_params)
                logger.debug(f"Response JSON: {response.json()}")
                return response
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP status error: {e.response.status_code} {e.response.text}")
            raise

        except httpx.RequestError as e:
            logger.error(f"Request error: {str(e)}")
            raise

   

    def get_health_check(self) -> httpx.Response:
        """Checks if API is running and healthy.

        Calls the API health check endpoint and returns a standard
        message if the API is running normally. Can be used to check
        status of API before making more complicated API calls.

        Returns:
            An httpx.Response object that contains the HTTP status,
            JSON response and other information received from the API.

        """
        logger.debug("Entered health check")
        endpoint_url = self.HEALTH_CHECK_ENDPOINT
        return self.call_api(endpoint_url)

    def list_leagues(
        self,
        skip: int = 0,
        limit: int = 100,
        minimum_last_changed_date: str = None,
        league_name: str = None,
    ) -> List[League]:
        """Returns a List of Leagues filtered by parameters.

        Calls the API v0/leagues endpoint and returns a list of
        League objects.

        Returns:
        A List of schemas.League objects. Each represents one
        SportsWorldCentral fantasy league.

        """
        logger.debug("Entered list leagues")

        params = {
            "skip": skip,
            "limit": limit,
            "minimum_last_changed_date" :minimum_last_changed_date,
            "league_name": league_name,
        }

        response = self.call_api(self.LIST_LEAGUES_ENDPOINT, params)
        return [League(**league) for league in response.json()]


    def get_league_by_id (self, league_id: int) -> League:
        """Returns a lLeague matching the league_id.

        Calls the API v0/leagues/{league_id} endpoint and returns a single League.

        Returns:
        A schemas.League object that represents one SportsWorldCentral fantasy league.

        """
        logger.debug(f"Entered get league by ID with league_id: {league_id}")
        #build the endpoint URL
        endpoint_url = f"{self.LIST_LEAGUES_ENDPOINT}{league_id}"
        #make the API call
        response = self.call_api(endpoint_url)
        responseLeague = League(**response.json())
        logger.debug(f"League response: {responseLeague}")
        return responseLeague



    def get_counts (self) -> Counts:
        """
        Returns a Counts object of several endpoints.

        Calls the API v0/Counts endpoint and returns a Counts object.

        Returns:
        A schemas.Counts object that contains the counts of various entities.
        """

        logger.debug("Entered get counts")
        response = self.call_api(self.GET_COUNTS_ENDPOINT)
        responseCounts = Counts(**response.json())
        return responseCounts

    def list_teams(
        self,
        skip: int = 0,
        limit: int = 100,
        minimum_last_changed_date: str = None,
        team_name: str = None,
        league_id: int = None,
    ):
        """Returns a List of Teams filtered by parameters.

        Calls the API v0/teams endpoint and returns a list of
        Team objects.

        Returns:
        A List of schemas.Team objects. Each represents one
        team in SportsWorldCentral fantasy league.
        """
        logger.debug("Entered list teams")

        params = {
            "skip": skip,
            "limit": limit,
            "minimum_last_changed_date": minimum_last_changed_date,
            "team_name": team_name,
            "league_id": league_id,
        }

        response = self.call_api(self.LIST_TEAMS_ENDPOINT, params)
        return [Team(**team) for team in response.json()]


    def get_player_by_id (
        self,
        playter_id: int,
    ):
        """Returns a Player matching the player_id.

        Calls the API v0/players/{player_id} endpoint and returns a single Player.

        Returns:
        A schemas.Player object that represents one SportsWorldCentral fantasy player.
        """
        logger.debug(f"Entered get player by ID with player_id: {playter_id}")
        #build the endpoint URL
        endpoint_url = f"{self.LIST_PLAYERS_ENDPOINT}{playter_id}"

        #make the API call
        response = self.call_api(endpoint_url);
        responseTeam = Player(**response.json())
        logger.debug(f"Player response: {responseTeam}")
        return responseTeam


    def list_performances(
        self,
        skip: int = 0,
        limit: int = 100,
        minimum_last_changed_date: str = None,
    ) -> List[Performance]:
        """Returns a List of Performances filtered by parameters.

        Calls the API v0/performances endpoint and returns a list of
        Performance objects.

        Returns:
        A List of schemas.Performance objects. Each represents one
        performance in SportsWorldCentral fantasy league.
        """
        logger.debug("Entered list performances")

        params = {
            "skip": skip,
            "limit": limit,
            "minimum_last_changed_date": minimum_last_changed_date,
        }

        response = self.call_api(self.LIST_PERFORMANCES_ENDPOINT, params)
        logger.debug("Raw response JSON: %s", response.json())

        return [Performance(**performance) for performance in response.json()]


#bulk endpoints


    def get_bulk_player_file(self) -> bytes:
        """Returns a bulk file with player data"""

        logger.debug("Entered get bulk player file")

        player_file_path = self.BULK_FILE_BASE_URL + self.BULK_FILE_NAMES["players"]

        response = httpx.get(player_file_path, follow_redirects=True)

        if response.status_code == 200:
            logger.debug("File downloaded successfully")
            return response.content

    def get_bulk_league_file(self) -> bytes:
        """Returns a CSV file with league data"""

        logger.debug("Entered get bulk league file")

        league_file_path = self.BULK_FILE_BASE_URL + self.BULK_FILE_NAMES["leagues"]

        response = httpx.get(league_file_path, follow_redirects=True)

        if response.status_code == 200:
            logger.debug("File downloaded successfully")
            return response.content

    def get_bulk_performance_file(self) -> bytes:
        """Returns a CSV file with performance data"""

        logger.debug("Entered get bulk performance file")

        performance_file_path = (
            self.BULK_FILE_BASE_URL + self.BULK_FILE_NAMES["performances"]
        )

        response = httpx.get(performance_file_path, follow_redirects=True)

        if response.status_code == 200:
            logger.debug("File downloaded successfully")
            return response.content

    def get_bulk_team_file(self) -> bytes:
        """Returns a CSV file with team data"""

        logger.debug("Entered get bulk team file")

        team_file_path = self.BULK_FILE_BASE_URL + self.BULK_FILE_NAMES["teams"]

        response = httpx.get(team_file_path, follow_redirects=True)

        if response.status_code == 200:
            logger.debug("File downloaded successfully")
            return response.content

    def get_bulk_team_player_file(self) -> bytes:
        """Returns a CSV file with team player data"""

        logger.debug("Entered get bulk team player file")

        team_player_file_path = (
            self.BULK_FILE_BASE_URL + self.BULK_FILE_NAMES["team_players"]
        )

        response = httpx.get(team_player_file_path, follow_redirects=True)

        if response.status_code == 200:
            logger.debug("File downloaded successfully")
            return response.content
