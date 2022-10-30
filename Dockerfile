FROM python:3.9-slim

WORKDIR /code

COPY requirements.txt .

RUN pip install -r requirements.txt
					
COPY . /code

CMD [ "python", "rabbit_service.py" ]