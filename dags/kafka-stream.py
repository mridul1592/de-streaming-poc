from uuid import uuid4
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2026, 9, 27, 10, 00)  # airflow needs this in the past or it'll backfill like crazy
}

def get_data():
    # randomuser.me just gives us one fake person per call, good enough for testing the pipeline
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]

    return res

def format_data(res):
    # flattening the nested api response into something closer to our cassandra table columns
    data = {}
    location = res['location']
    data['id'] = str(uuid4())  # api doesn't give a stable id we care about, just generate our own
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    # 'broker' here is the service name from docker-compose, not localhost -
    # this only works when airflow and kafka are on the same docker network
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: #1 minute
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            # keep going - one bad request from the api shouldn't kill the whole run
            logging.error(f'An error occured: {e}')
            continue

with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

# stream_data()  # uncomment to test locally outside of airflow