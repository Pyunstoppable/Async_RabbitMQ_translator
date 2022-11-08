import ast
import asyncio
import logging
import os
import pickle
import zlib

import aio_pika
import motor.motor_asyncio
from aio_pika.abc import AbstractChannel, AbstractRobustConnection
from bson.binary import Binary
from motor.motor_asyncio import AsyncIOMotorCollection
from transitions.core import MachineError
from transitions.extensions import MachineFactory
from transitions.extensions.nesting import NestedState
from transitions.extensions.asyncio import HierarchicalAsyncMachine

logging.basicConfig(format='%(processName)s - [%(asctime)s: %(levelname)s] %(message)s')
logger = logging.getLogger("transitions")
logger.setLevel(logging.INFO)
logger.info("Start service")

RABBIT_SETTINGS = os.getenv('RABBIT_SETTINGS', 'amqp://guest:guest@localhost:5672/')
que_first_service = 'que_first_service'
que_second_service = 'que_second_service'

MONGO_SETTINGS = os.getenv('MONGO_SETTINGS', 'mongodb://localhost:27017')
client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_SETTINGS)
db = client.fsm_database
fsm_col_machines = db.fsm_machines
fsm_col_orders = db.fsm_orders

NestedState.separator = '>>'
states = ['empty',
          {'name': 'Большую', 'children': ['Наличкой', 'Картой']},
          {'name': 'Маленькую', 'children': ['Наличкой', 'Картой']},
          'full', 'cancel']

transitions = [
    ['Отменить', ['Большую>>Наличкой', 'Большую>>Картой', 'Маленькую>>Наличкой', 'Маленькую>>Картой'], 'cancel'],
    ['Да', ['Большую>>Наличкой', 'Большую>>Картой', 'Маленькую>>Наличкой', 'Маленькую>>Картой'], 'full'],
    ['Большую', ['empty', 'Большую', 'Маленькую'], 'Большую'],
    ['Большую', 'Маленькую>>Наличкой', 'Большую>>Наличкой'],
    ['Большую', 'Маленькую>>Картой', 'Большую>>Картой'],
    ['Большую', 'Большую>>Наличкой', 'Большую>>Наличкой'],
    ['Большую', 'Большую>>Картой', 'Большую>>Картой'],
    ['Наличкой', 'Большую', 'Большую>>Наличкой'],
    ['Наличкой', 'Маленькую', 'Маленькую>>Наличкой'],
    ['Картой', 'Большую', 'Большую>>Картой'],
    ['Картой', 'Маленькую', 'Маленькую>>Картой'],
    ['Маленькую', ['empty', 'Большую', 'Маленькую'], 'Маленькую'],
    ['Маленькую', 'Большую>>Наличкой', 'Маленькую>>Наличкой'],
    ['Маленькую', 'Большую>>Картой', 'Маленькую>>Картой'],
    ['Маленькую', 'Маленькую>>Наличкой', 'Маленькую>>Наличкой'],
    ['Маленькую', 'Маленькую>>Картой', 'Маленькую>>Картой']]


async def mongo_actions(action: str, collection: AsyncIOMotorCollection, user: int, data=None):
    """Basic tools for MongoDB"""
    if action == 'insert':
        await collection.insert_one({'Name': user, 'data': data})
    elif action == 'update':
        await collection.update_one({'Name': user}, {"$set": {'data': data}})
    elif action == 'delete':
        await collection.delete_one({'Name': user})
    elif action == 'find':
        return collection.find({'Name': user})

    logger.info(f'{action} for {user}')


async def get_or_create_fsm(user: str, start: int = 0) -> tuple[HierarchicalAsyncMachine, str]:
    """Create state machine for new user or
    return currently. Param start is True
    when user give command /start from messager
    """
    get_fsm = await mongo_actions('find', fsm_col_machines, user)
    get_fsm = await get_fsm.to_list(1)
    if get_fsm:
        fsm = pickle.loads(get_fsm[0]['data'])
        if start == 1:
            logger.info('New order, switch to state Empty')
            fsm.set_state('empty')
            pickled_fsm = pickle.dumps(fsm)
            await mongo_actions('update', fsm_col_machines, user, data=Binary(pickled_fsm))
            # ! get_fsm = await mongo_actions('find', fsm_col_machines, user)
            # ! get_fsm = await get_fsm.to_list(1)
            # ! fsm = pickle.loads(get_fsm[0]['data'])
        return fsm, fsm.state
    else:
        async_machine_cls = MachineFactory.get_predefined(nested=True, asyncio=True)
        fsm = async_machine_cls(states=states,
                                transitions=transitions,
                                initial='empty',
                                ignore_invalid_triggers=False)
        pickled_fsm = pickle.dumps(fsm)
        await mongo_actions('insert', fsm_col_machines, user, Binary(pickled_fsm))
        return fsm, fsm.state


async def update_state(user: str, trigger: str) -> tuple[str, int]:
    """Update state machine. We given trigger from
    callback messager and make transition for
    user fsm. After pickled machine and destination
    in MongoDB. If state are final then make record
    about order in DB. If the user sends callbacks
    that are not possible for the state machine,
    then this is a signal that the checkout is complete.

    Returns:
        Current state from FMS; Flag:
        0 for command start, 1 for callbacks, 2 for command hist
    """
    current_fsm, current_fsm_state = await get_or_create_fsm(user)
    try:
        await current_fsm.dispatch(trigger)
        logger.info(f"Set state = {current_fsm.state}")
        if current_fsm.state == 'full':

            logger.info("Save order")
            await mongo_actions('insert', fsm_col_orders, user, current_fsm_state)

        pickled_fsm = pickle.dumps(current_fsm)
        await mongo_actions('update', fsm_col_machines, user, data=Binary(pickled_fsm))
        return current_fsm.state, 0

    except MachineError:
        logger.info('Incorrect state, return to user current state')
        return current_fsm.state, 1


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

                # Given from Telegram > Rabbit, processing, before send message > Rabbit > Telegram
                if response['val'] == '/start':
                    current_fsm, current_fsm_state = await get_or_create_fsm(response['chat'], start=1)
                    logger.info(f'{current_fsm}, {current_fsm_state}')
                    response['flag'] = 0
                    response['state'] = current_fsm_state
                    await send_to_amqp(response, que_second_service)
                    logger.info(f'The message was sent to RabbitMQ, queue={que_second_service}')

                elif response['val'] == '/hist':
                    history = await mongo_actions('find', fsm_col_orders, response['chat'])
                    history = await history.to_list(None)
                    list_order = 'Вы заказывали:\n'
                    if history:
                        for number, rows in enumerate(history, 1):
                            size, payment = rows['data'].split('>>')
                            list_order += f'{number}. {size} пиццу, оплата {payment.lower()}\n'
                    else:
                        list_order = 'У Вас еще не было заказов'
                    response['flag'] = 2
                    response['list_order'] = list_order
                    await send_to_amqp(response, que_second_service)
                    logger.info(f'The message was sent to RabbitMQ, queue={que_second_service}')

                else:
                    # processing from telegram callback.data
                    current_fsm, current_fsm_state = await get_or_create_fsm(response['chat'])
                    current_fsm_state, flag = await update_state(response['chat'], response['val'])
                    response['flag'] = flag
                    response['state'] = current_fsm_state
                    logger.info(f'{current_fsm}, {current_fsm_state}')
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
