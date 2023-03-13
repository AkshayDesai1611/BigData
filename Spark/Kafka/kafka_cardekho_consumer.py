import argparse
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
#from confluent_kafka.schema_registry.json_schema import Jso
import pymongo
#from confluent_kafka.json.serializer.message_serializer import MessageSerializer as JSONSerde
import json
import pandas as pd

API_KEY = 'JAJAK7O7SRBU6SXO'
ENDPOINT_SCHEMA_URL  = 'https://psrc-0kywq.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'FZDTGskiFmfOawGqtbtszpBQI19L12Ik2JqfNy86lxcg7FzVoneeEQrY3bGChk1F'
BOOTSTRAP_SERVER = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'A4W4IWAFVL7FVACD'
SCHEMA_REGISTRY_API_SECRET = 'CpuNWx7Uxp1ztjUOJO7YrfKPfdd4QQ1qQWG2aQrngPKFUWL8wys9zsnqt+XrUY1s'

def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Car:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_car(data:dict,ctx):
        return Car(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):

    schema_str = """
    {
  "$id": "http://example.com/myURI.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "additionalProperties": false,
  "description": "Sample schema to help you get started.",
  "properties": {
    "brand": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "car_name": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "engine": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "fuel_type": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "km_driven": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "max_power": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "mileage": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "model": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "seats": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "seller_type": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "selling_price": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "transmission_type": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "vehicle_age": {
      "description": "The type(v) type is used.",
      "type": "number"
    }
  },
  "title": "SampleRecord",
  "type": "object"
}
    """

    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Car.dict_to_car)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])


    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            car = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            
            dbConn = pymongo.MongoClient("mongodb+srv://akshay_01:Guddu1898@cluster0.a2gsb8a.mongodb.net/?retryWrites=true&w=majority")
            dbName = "ReadfromKafka"
            db = dbConn[dbName]
            collection_name = 'cardekho_data'
            collection = db[collection_name]
           
            
                
            if car is not None:
                print("User record {}: car: {}\n"
                      .format(msg.key(), car))
                
            for message in car.record.values():
                car_dict = dict(zip(car.record.keys(),car.record.values()))
                collection.insert_one(car_dict)
                
        except KeyboardInterrupt:
            break

    consumer.close()

main("topic_ans")