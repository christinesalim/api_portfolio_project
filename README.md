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



Notes: the /v0/weeks endpoint was missing from the code in book. Added it from this repository
https://github.com/Ryandaydev/good-stuff-dc/tree/main

Also updated the fantasy_data.db, main.py, models.py and crud.py files. 

To support the API calls from the jupyter notebook I had to make the Ports listed in the PORTS tab public for the URL:https://urban-potato-vqxxv656rwh6x6j.github.dev/

