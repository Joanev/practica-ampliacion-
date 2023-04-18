#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A small example subscriber
"""
import paho.mqtt.client as paho
import paho.mqtt.publish as publish
import time
import threading
import dictionary
import json
host = "localhost"

def process_message(mosq, obj, msg):
#def process_message():
    payload=json.loads(msg.payload)
    bloque=payload["bloque"]
    origen=payload["origen"]
    destino=payload["destino"]
    if origen != "MEM":
        if msg.topic == "msi/ptlec":
            print("recibo: msi/ptlec:", payload)
            process_ptlec(bloque,origen)
        elif msg.topic == "msi/ptlecex":
            print("recibo msi/ptlecex:", payload)
            process_ptlecex(bloque,origen)
        elif msg.topic == "msi/rpbloq":
            valor=payload["valor"]
            print("recibo msi/rqbloql:", payload)
            process_rpbloq(bloque,valor)
        else:
            print("recibo: ",msg.topic)  
    else:
        pass

def on_message(mosq, obj, msg):       
    proMsg = threading.Thread(target=process_message,args=[mosq, obj, msg])
    proMsg.start()
    mosq.publish('pong', 'ack', 0)

def on_publish(mosq, obj, mid):
    pass

def process_ptlec(bloque,destino):
    if dictionary.diccionario[bloque][1] == "I":
        pass
    else:
        #PUBLISH BLOQUE
        payload={
            "bloque": bloque,
            "valor": dictionary.diccionario[bloque][0],
            "origen": "MEM",
            "destino": destino
        }
        payload=json.dumps(payload)
        publish.single(topic="msi/rpbloq",payload=payload,hostname=host)
        print("envio: masi/rpbloq",payload)

def process_ptlecex(bloque,destino):
    if dictionary.diccionario[bloque][1] == "I":
        pass
    else:
        #MODIFICAMOS EL ESTADO DEL BLOQUE
        dictionary.diccionario[bloque][1] = "I"
        
        #PUBLISH BLOQUE
        payload={
            "bloque": bloque,
            "valor": dictionary.diccionario[bloque][0],
            "origen": "MEM",
            "destino": destino
        }
        payload=json.dumps(payload)
        publish.single(topic="msi/rpbloqex",payload=payload,hostname=host)
        print("envio: msi/rpbloq",payload)

def process_rpbloq(bloque,valor):   
    if dictionary.diccionario[bloque][1] == "I":
        #MODIFICAMOS EL VALOR DEL BLOQUE EN EL DICCIONARIO I EL ESTADO
        dictionary.diccionario[bloque][1] = "V"
        dictionary.diccionario[bloque][0] = valor        
    else:
        pass

if __name__ == '__main__':
    client = paho.Client()
    client.on_message = on_message
    client.on_publish = on_publish

    #client.tls_set('root.ca', certfile='c1.crt', keyfile='c1.key')
    client.connect("127.0.0.1", 1883, 60)
    client.subscribe("msi/#", 0)
    

    while client.loop() == 0:
        
        #time.sleep(60)
        #print("kk")
        pass

# vi: set fileencoding=utf-8 :