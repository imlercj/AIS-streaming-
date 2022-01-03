import faust
import os
import json


KAFKAHOST = os.getenv('KAFKAHOST', 'localhost')
KAFKAPORT = os.getenv('KAFKAPORT', '9092')

app = faust.App('example', broker=f'kafka://{KAFKAHOST}:{KAFKAPORT}', store='memory://', key_serializer='json')

topic = app.topic('ais', value_type=bytes)

@app.agent(topic)
async def process(stream):
    async for event in stream:
        print(event)

