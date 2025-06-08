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