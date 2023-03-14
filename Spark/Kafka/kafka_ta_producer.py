
import argparse
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
#from confluent_kafka.schema_registry import *
import pandas as pd
from typing import List

FILE_PATH = "E:/BigData/Spark/TimeAge.csv"
columns = ['date','time','age','confiremd','deceased']

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
            setattr(self, k, v)
            
        self.record = record
        
    @staticmethod
    def dict_to_case(data:dict,ctx):
        return Case(record=data)

    def __str__(self):
        return f"{self.record}"

def get_case_instance(file_path):
    df=pd.read_csv(file_path)
    df=df.iloc[:,:]
    case:List[Case]=[]
    for data in df.values:
        cas=Case(dict(zip(columns,data)))
        case.append(cas)
        yield cas

def cas_to_dict(cas:Case, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return cas.record

def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    
    
def main(topic):


    
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    mynewjson = schema_registry_client.get_schema(schema_id=100012).schema_str

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(mynewjson, schema_registry_client, cas_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    #while True:
        # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    
    try:
        for cas in get_case_instance(file_path=FILE_PATH):

            print(cas)
            producer.produce(topic=topic,
                            key=string_serializer(str(uuid4()), cas_to_dict),
                            value=json_serializer(cas, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)
            
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()

main("topic_aks")