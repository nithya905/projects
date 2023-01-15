import argparse

from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
import pandas as pd


API_KEY = 'EAMM5NCMTSFDU7KU'
ENDPOINT_SCHEMA_URL  = 'https://psrc-30dr2.us-central1.gcp.confluent.cloud'
API_SECRET_KEY = 'uPmNjotrkp5XnyyEq2yWRB/YJ2SjRoH34pdF51+IspGluxmdQY87zyF205LDAYrm'
BOOTSTRAP_SERVER = 'pkc-6ojv2.us-west4.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'W4MS2Z3MZH7G2NBS'
SCHEMA_REGISTRY_API_SECRET = 'sHSZ4HakLn+nttoF4Zix9EzagvcZLhIJ9fBPAVkpHhpf5tlfqZ0ZixLnbeMTDpqJ'


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
    "Order Number": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "Order Date": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "Item Name": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "Quantity": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "Product Price": {
      "description": "The type(v) type is used.",
      "type": "number"
    },
    "Total products": {
      "description": "The type(v) type is used.",
      "type": "number"
    }
  },
  "title": "SampleRecord",
  "type": "object"
}
    """
   #reading the latest version of schema and schema_str from schema registry and using it for data deserialization. 
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    schema_name = schema_registry_client.get_schema(100008).schema_str
    json_deserializer = JSONDeserializer(schema_name,
                                         from_dict=Car.dict_to_car)
    print(JSONDeserializer(schema_name,
                                         from_dict=Car.dict_to_car))                                     

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group2',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])


    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            # car = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            car = json_deserializer(msg.value(),SerializationContext(msg.topic(), MessageField.VALUE))
           
            
            # df = pd.read_json(car.json)
            # print(df,'this is json')
            #df.to_csv('output.csv', index=False, header=True)


            # if car is not None:
            #     print("User record {}: car: {}\n"
            #           .format(msg.key(), car))
            #     print(b, 'this is b')      
              
            
            

# returns JSON object as
# a dictionary


#converting it into dataframe
 
    
    
           
        except KeyboardInterrupt:
            break

    consumer.close()

main("restaurent-take-away-data")