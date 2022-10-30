import aio_pika
import logging
import zlib
import ast
import asyncio
import os
from aio_pika.abc import AbstractRobustConnection, AbstractChannel


que_first_service = 'que_first_service'
que_second_service = 'que_second_service'
RABBIT_SETTINGS = os.getenv('RABBIT_SETTINGS', 'amqp://guest:guest@localhost:5672/')

logging.basicConfig(format='%(processName)s - [%(asctime)s: %(levelname)s] %(message)s')
logger = logging.getLogger("ss")
logger.setLevel(logging.INFO)
logger.info("Start service")


async def amqp_connection() -> tuple[AbstractRobustConnection, AbstractChannel]:
    """Connecting to RabbitMQ"""
    connection = await aio_pika.connect_robust(RABBIT_SETTINGS)
    channel = await connection.channel()
    return connection, channel


async def send_to_amqp(mes: str, q: str) -> None:
    """Send messages to RabbitMQ"""
    logger.info(f" [x] Sent mess {mes}")
    mes = str(mes).encode('utf-8')
    # compress message
    logger.info(f"Size before compress: {len(mes)}")
    mes = zlib.compress(mes, level=-1)
    logger.info(f"Size after compress: {len(mes)}")
    connection, channel = await amqp_connection()
    await channel.declare_queue(q, durable=True)
    await channel.default_exchange.publish(aio_pika.Message(body=mes), routing_key=q)


async def receive_from_amqp(q: str) -> None:
    """Receive messages from RabbitMQ"""
    connection, channel = await amqp_connection()
    logger.info(' [*] Waiting for messages. To exit press CTRL+C')
    await channel.set_qos(prefetch_count=1)
    # Declaring queue
    queue = await channel.declare_queue(q, durable=True)
    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                # decompress message
                mes = zlib.decompress(message.body)
                response = ast.literal_eval(mes.decode('utf-8'))
                logger.info(f'The message was received to RabbitMQ, queue={q}')
                # in this place, you can do some work / send it to another service, etc.
                await send_to_amqp(response, que_second_service)
                logger.info(f'The message was sent to RabbitMQ, queue={que_second_service}')

                if queue.name in mes.decode():
                    break

    try:
        logger.info('start consuming')
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        exit()


if __name__ == "__main__":
    asyncio.run(receive_from_amqp(que_first_service))
