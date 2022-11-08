### Async RabbitMQ translator

this service receives and sends messages using RabbitMQ


### Reqiurements

[Aio-pika](https://aio-pika.readthedocs.io/en/latest/)




### Demo

![preview](/preview.jpg "preview")

### Where do we start?

Set Environment Variables:
   You can also set the settings RABBIT_SETTINGS or skip, then the standard ones will be used

### Run service

python rabbit_service.py

### Run service in Docker

#### Rabbit
docker run -d --hostname my-rabbit --name some-rabbit -p 8080:15672 -p 5672:5672 rabbitmq:3-management

#### Service
1. docker build --rm --network host -f ./Dockerfile -t rabbit_service:latest .
2. docker run --network=host --hostname=rabbit_service -d --name=rabbit_service -e "RABBIT_SETTINGS=amqp://guest:guest@localhost:5672/" rabbit_service:latest


docker run -d --hostname my-mongo --name some-mongo -p 27017:27017 mongo:latest
