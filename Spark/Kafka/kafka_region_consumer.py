import argparse

from confluent_kafka import Consumer
from kafka.consumer import KafkaConsumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
import pymongo


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



class Case:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_case(data:dict,ctx):
        return Case(record=data)

    def __str__(self):
        return f"{self.record}"
    
def main(topic):


    
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    mynewjson = schema_registry_client.get_schema(schema_id=100006).schema_str

    
    json_deserializer = JSONDeserializer(mynewjson,
                                         from_dict=Case.dict_to_case)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    
    
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(10.0)
            if msg is None:
                continue
      
            cas = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            dbConn = pymongo.MongoClient("mongodb+srv://akshay_01:Guddu1898@cluster0.a2gsb8a.mongodb.net/?retryWrites=true&w=majority")
            dbName = "ReadfromKafka"
            db = dbConn[dbName]
            collection_name = 'Region'
            collection = db[collection_name]
            
            #Mongo Collection accepts data in the dictionary form, hence creating
            #a new dict and inserting the reccords one by one
            
            for message in cas.record.values():
                new_dt = dict(zip(cas.record.keys(),cas.record.values()))
                collection.insert_one(new_dt)
                
            
     
        
     
            if cas is not None:
                print("User record {}: case: {}\n"
                      .format(msg.key(), cas))
            #print(case.record.values())
            #print(case.record.keys())
            #print(dict(zip(case.record.keys(),case.record.values())))
        except KeyboardInterrupt:
            break

    consumer.close()

main("topic_ansha")
    
    
    