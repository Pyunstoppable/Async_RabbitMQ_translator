### Async RabbitMQ translator

This service receives and sends messages using RabbitMQ,
interacts with the state machine and MongoDB.


### Reqiurements

[Aio-pika](https://aio-pika.readthedocs.io/en/latest/)

[motor](https://github.com/mongodb/motor)

[transitions](https://github.com/pytransitions/transitions)


### Description

This service receives user callbacks from the messenger via RabbitMQ,
then processes the transitions and states of the state machine,
based on which it forms a response to the RabbitMQ queue. At the same time, records are kept in MongoDB.


### Where do we start?

Set Environment Variables:
   You can also set the settings RABBIT_SETTINGS and MONGO_SETTINGS or skip, then the standard ones will be used

### Run service

python rabbit_service.py

### Run service in Docker

#### RabbitMQ and MongoDB
docker run -d --hostname my-rabbit --name some-rabbit -p 8080:15672 -p 5672:5672 rabbitmq:3-management
docker run -d --hostname my-mongo --name some-mongo -p 27017:27017 mongo:latest

#### Service
1. docker build --rm --network host -f ./Dockerfile -t rabbit_service:latest .
2. docker run --network=host --hostname=rabbit_service -d --name=rabbit_service -e "MONGO_SETTINGS=mongodb://localhost:27017" -e "RABBIT_SETTINGS=amqp://guest:guest@localhost:5672/" rabbit_service:latest
