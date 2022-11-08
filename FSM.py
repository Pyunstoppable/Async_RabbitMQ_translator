from transitions.extensions import MachineFactory
from transitions.extensions.nesting import NestedState
from transitions.core import MachineError
import logging
import pickle
import asyncio
import motor.motor_asyncio
import os
from bson.binary import Binary

logging.basicConfig(format='%(processName)s - [%(asctime)s: %(levelname)s] %(message)s')
logger = logging.getLogger('transitions')
logger.setLevel(logging.INFO)

MONGO_SETTINGS = os.getenv('MONGO_SETTINGS', 'mongodb://localhost:27017')
client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_SETTINGS)
db = client.fsm_database
fsm_col_machines = db.fsm_machines
fsm_col_orders = db.fsm_orders
test_user = 123456
test_user2 = 456
async def mongo_actions(action, collection, user, data=None):
    if action == 'insert':
        await collection.insert_one({'Name': user, 'data': data})
    elif action == 'update':
        await collection.update_one({'Name': user}, {"$set": {'data': data}})
    elif action == 'delete':
        await collection.delete_one({'Name': user})
    elif action == 'find':
        return collection.find({'Name': user})

    logger.info(f'{action} for {user}')


NestedState.separator = '>>'
states = ['empty',
          {'name': 'Большую', 'children':['Наличкой', 'Картой']},
          {'name': 'Маленькую', 'children':['Наличкой', 'Картой']},
          'full']

transitions = [
  ['cancel', '*', 'empty'],
  ['confirm', ['Большую>>Наличкой', 'Большую>>Картой', 'Маленькую>>Наличкой', 'Маленькую>>Картой'], 'full'],
  
  ['Большую', 'empty', 'Большую'],
  ['Наличкой', 'Большую', 'Большую>>Наличкой'],
  ['Картой', 'Большую', 'Большую>>Картой'],

  ['Маленькую', 'empty', 'Маленькую'],
  ['Наличкой', 'Маленькую', 'Маленькую>>Наличкой'],
  ['Картой', 'Маленькую', 'Маленькую>>Картой'],
]


async def get_or_create_fsm(user):
    get_fsm = await mongo_actions('find', fsm_col_machines, user)
    get_fsm = await get_fsm.to_list(1)
    if get_fsm:
        fsm = pickle.loads(get_fsm[0]['data'])
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


async def update_state(user, trigger):
    current_fsm, current_fsm_state = await get_or_create_fsm(user)
    # save order
    if trigger == 'confirm':
        await mongo_actions('insert', fsm_col_orders, user, current_fsm.state)
        await mongo_actions('delete', fsm_col_machines, user)
    elif trigger == 'cancel':
        await mongo_actions('delete', fsm_col_machines, user)
    else:
        try:
            await current_fsm.dispatch(trigger)
        except MachineError:
            logger.info('incorrect transition!')
        else:
            pickled_fsm = pickle.dumps(current_fsm)
            await mongo_actions('update', fsm_col_machines, user, data=Binary(pickled_fsm))
        finally:
            return current_fsm.state


async def testers():

    current_fsm, current_fsm_state = await get_or_create_fsm(test_user)
    print(current_fsm, current_fsm_state)

    st = await update_state(test_user, 'Большую')
    print(st)

    st = await update_state(test_user, 'Наличкой')
    print(st)

    st = await update_state(test_user, 'confirm')
    print(st)

    current_fsm, current_fsm_state = await get_or_create_fsm(test_user2)
    print(current_fsm, current_fsm_state)

    st = await update_state(test_user2, 'Маленькую')
    print(st)

    st = await update_state(test_user2, 'Картой')
    print(st)

if __name__ == "__main__":
    asyncio.run(testers())
