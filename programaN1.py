#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
A small example subscriber
"""
import paho.mqtt.client as paho
import paho.mqtt.publish as publish
import time
import threading
import dictionaryN1
import json
import random
import sys
from datetime import datetime
host = "localhost"
Arrive_rpbloque=False
procesador= sys.argv[2]
instant_time=0
archivo_completo=None
modo=None
def process_message(mosq, obj, msg):
    payload=json.loads(msg.payload)
    bloque=payload["bloque"]
    origen=payload["origen"]
    destino=payload["destino"]
    if origen != procesador:
        if msg.topic == "msi/ptlec":
            print("recibo: msi/ptlec", payload)
            process_ptlec(bloque,origen)
        elif msg.topic == "msi/ptlecex":
            print("recibo: msi/ptlecex", payload)
            process_ptlecex(bloque,origen)
        elif msg.topic == "msi/rpbloq" and destino == procesador:            
            valor=payload["valor"]
            print("recibo: msi/rqbloq", payload)
            process_rpbloq(bloque,valor)
        elif msg.topic == "msi/rpbloqex" and destino == procesador:      
            valor=payload["valor"]      
            print("recibo: msi/rqbloqex", payload)
            process_rpbloqex(bloque,valor)
        elif msg.topic == "msi/proclec":
            print("recibo: msi/proclec", payload)
            process_proclec(bloque)
        elif msg.topic == "msi/procesc":
            print("recibo: msi/procesc", payload)
            process_procesc(bloque)
        else:
            pass   
    else:
        pass

def on_message(mosq, obj, msg):     
    proMsg = threading.Thread(target=process_message,args=[mosq, obj, msg])
    proMsg.start()
    mosq.publish('pong', 'ack', 0)

def on_publish(mosq, obj, mid):
    pass

def process_ptlec(bloque,destino):
    if dictionaryN1.diccionario[bloque][1] == "I":
        pass      
    elif dictionaryN1.diccionario[bloque][1] == "M":
        #MODIFICAMOS EL VALOR DEL BLOQUE
        dictionaryN1.diccionario[bloque][1]="C"
        #PUBLISH BLOQUE
        payload={
            "bloque": bloque,
            "valor": dictionaryN1.diccionario[bloque][0],
            "origen": procesador,
            "destino": destino
        }
        payload=json.dumps(payload)
        publish.single(topic="msi/rpbloq",payload=payload,hostname=host)
        
    else:
        pass

def process_ptlecex(bloque,destino):   
    if dictionaryN1.diccionario[bloque][1] == "I":
        pass
    elif dictionaryN1.diccionario[bloque][1] == "M":
        #MODIFICAMOS EL VALOR DEL BLOQUE
        dictionaryN1.diccionario[bloque][1]="I"
        #PUBLISH BLOQUE
        payload={
            "bloque": bloque,
            "valor": dictionaryN1.diccionario[bloque][0],
            "origen": procesador,
            "destino": destino
        }
        payload=json.dumps(payload)
        publish.single(topic="msi/rpbloqex",payload=payload,hostname=host)    
                                                             
    else:
        #MODIFICAMOS EL ESTADO DEL BLOQUE
        dictionaryN1.diccionario[bloque][1] = "I"

def process_rpbloq(bloque,valor):   
    global Arrive_rpbloque
    dictionaryN1.diccionario[bloque][1]="C"
    dictionaryN1.diccionario[bloque][0]=valor
    Arrive_rpbloque=True  

def process_rpbloqex(bloque,valor):   
    global Arrive_rpbloque
    dictionaryN1.diccionario[bloque][1]="M"
    dictionaryN1.diccionario[bloque][0]=valor
    Arrive_rpbloque=True  

def read_line_to_json(filename):
    global archivo_completo
    if(archivo_completo==None):
        with open(filename, 'r') as f:
            archivo_completo = f.read()
    with open(filename, 'r') as f:
        line = f.readline().strip()
        remaining_lines = f.readlines()
    with open(filename, 'w') as f:
        f.writelines(remaining_lines)
    words = line.split()
    topic = words[0]
    block = words[1]
    value = 0
    if len(words) == 3:
        value = words[2]
    json_obj = {
        "topic": topic,
        "bloque": block,
        "value": value,   
        "destino": None
    }
    return json.dumps(json_obj)

def enviar(mensage,orden):
     #PUBLISH BLOQUE
        global Arrive_rpbloque
        global modo
        payload={
            "bloque": mensage["bloque"],
            "valor": mensage["value"],
            "origen": procesador,
            "destino": None
            }
        payload=json.dumps(payload)
        topic="msi/" + orden
        publish.single(topic=topic,payload=payload,hostname=host)
        Arrive_rpbloque=False     
        if(dictionaryN1.diccionario[mensage["bloque"]][1] =="M"):
            pass  
        else:
            while (Arrive_rpbloque == False):
                pass
        Arrive_rpbloque=False     
        print("antes de actualizar: ",dictionaryN1.diccionario[mensage["bloque"]][0])
        if modo == "PrEsc":           
            dictionaryN1.diccionario[mensage["bloque"]][0]=mensage["value"]
        print("despues de actualizar: ",dictionaryN1.diccionario[mensage["bloque"]][0])
        escribir_en_archivo(mensage["bloque"],dictionaryN1.diccionario[mensage["bloque"]][0],procesador+"time.txt",modo)
        print("caché: ",dictionaryN1.diccionario[mensage["bloque"]][0], dictionaryN1.diccionario[mensage["bloque"]][1])

def comprovar(mensage):
    global modo
    if dictionaryN1.diccionario[mensage["bloque"]][1]=="I":
        if mensage["topic"] == "PrEsc":
            modo="PrEsc"
            return "ptlecex"
        else:
            modo="PrLec"
            return "ptlec"
    if mensage["topic"] == "PrEsc" and dictionaryN1.diccionario[mensage["bloque"]][1]=="M":
        modo="PrEsc"
        return "ptlecex"
    else:
        if mensage["topic"] == "PrEsc":
            modo="PrEsc"
            return "ptlecex"
        else:
            escribir_en_archivo(mensage["bloque"],dictionaryN1.diccionario[mensage["bloque"]][0],procesador+"time.txt","PrLec")
            print("caché: ",dictionaryN1.diccionario[mensage["bloque"]][0], dictionaryN1.diccionario[mensage["bloque"]][1])
        return "nada"
    
def escribir_en_archivo(bloque,valor, archivo,mode):   
    instant_time=datetime.now()
    texto=str(instant_time) +": ["+ procesador + "] " + mode + " " + bloque + ": " + str(valor) + "\n"
    with open(archivo, 'a') as archivo_texto:
        archivo_texto.write(texto)

def init_send():
    global archivo_completo
    archivo=sys.argv[1]
    mensage=read_line_to_json(archivo)
    mensage=json.loads(mensage)
    while mensage["topic"] != "Fin":
        print("envio: ",mensage)
        orden=comprovar(mensage)
        print(orden)
        if orden=="nada":               
            pass
        else:
            enviar(mensage,orden)
        time.sleep(random.randint(1,6))
        mensage=read_line_to_json(archivo)
        mensage=json.loads(mensage)
    print("Fin del programa")   
    print(archivo_completo)
    with open(archivo, 'w') as fwrite:
        fwrite.write(archivo_completo)
if __name__ == '__main__':
    client = paho.Client()
    client.on_message = on_message
    client.on_publish = on_publish
    random.seed(1)
    
    #client.tls_set('root.ca', certfile='c1.crt', keyfile='c1.key')
    client.connect("127.0.0.1", 1883, 60)
    client.subscribe("msi/#", 0)
    p1_thread = threading.Thread(target=init_send)
    p1_thread.start()   
    while client.loop() == 0:
        pass
    
# vi: set fileencoding=utf-8 :