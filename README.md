# API Portfolio Project
This project demonstrates API creation using Python and FastAPI.

This project was built from examples from the book 
 [Hands-On APIs For API and Data Science](https://handsonapibook.com/).

cd my_work
sqlite3 fantasy_data.db < setup_and_import.sql

#To Install requirements for project
pip3 install -r requirements.txt

#Test crud.py
pytest test-crud.py


pytest -k "test_read_players_by_name"


#Run the application
fastapi run main.py


#Docker commands
docker build -t apicontainerimage .

docker images

docker run --publish 80:80 --name apicontainer1 apicontainerimage


#To install sdk in local directory
tree --prune -I 'build|*egg-info|__pycache__'
pip3 install --upgrade .


#To run the sdk test need to set the path to src folder
cd my_work/sdk
PYTHONPATH=src pytest tests/test_swcpy.py


#Set the debug level for the test
 PYTHONPATH=src pytest -s --log-cli-level=DEBUG tests/test_swcpy.py 