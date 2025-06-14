FROM python:3.10-slim

WORKDIR /code

COPY requirements.txt /code/

RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY *.py /code/
COPY *.db /code/

CMD ["uvicorn", "main:app", "--host", "0.0.0", "--port", "80"]