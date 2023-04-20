from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.background import BackgroundTasks
from redis_om import get_redis_connection, HashModel
from starlette.requests import Request
import requests
import time
import json
import consumers

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:3000'],
    allow_methods=['*'],
    allow_headers=['*']
)

# This should be a different database 
redis=get_redis_connection(
    host="redis-17668.c212.ap-south-1-1.ec2.cloud.redislabs.com",
    port=17668,
    password="MJQ0C8zqBPagdHxEcUxSejtcYIDMZzod",
    decode_responses=True
)

class Delivery(HashModel):
    budget: int = 0
    notes: str = ''

    class Meta:
        database = redis

class Event(HashModel):
    delivery_id: str = None
    type: str
    data: str

    class Meta:
        database = redis

@app.get('/deliveries/{pk}/status')
async def get_state(pk: str):
    state=redis.get(f'delivery:{pk}')
    if state is not None:
        return json.dumps(state)

    return {}

@app.post('/deliveries/create')
async def create(request: Request):
    body = await request.json()
    delivery= Delivery(budget=body['data']['budget'], notes=body['data']['notes']).save()
    event= Event(delivery_id=delivery.pk, type=body['type'], data=json.dumps(body['data'])).save()
    state=consumers.create_delivery({}, event)
    redis.set(f'delivery:{delivery.pk}', json.dumps(state))
    return state

@app.post('/event')
async def dispatch(request: Request):
    body = await request.json()
    delivery_id= body['delivery_id']
    event= Event(delivery_id=delivery_id, type=body['type'], data=json.dumps(body['data'])).save()
    state = await get_state(delivery_id)
    state['status']='active'
    return state

# @app.get('/orders/{pk}')
# def get(pk:str):
#     order=Order.get(pk)
#     redis.xadd('refund_order', order.dict(), '*')
#     return order

# @app.post('/orders')
# async def create(request: Request, background_tasks: BackgroundTasks): #id , quantity
#     body = await request.json()

#     req=requests.get('http://localhost:8000/products/%s' % body['id'])
    
#     product = req.json()

#     order=Order(
#         product_id=body['id'],
#         price=product['price'],
#         fee=0.2*product['price'],
#         total=1.2*product['price'],
#         quantity=body['quantity'],
#         status= 'pending',
#     )
#     background_tasks.add_task(order_completed, order)
#     order.save()
#     return order

# def order_completed(order: Order):
#     time.sleep(5)
#     order.status='completed'
#     order.save()
#     redis.xadd('order_completed', order.dict(), '*')
