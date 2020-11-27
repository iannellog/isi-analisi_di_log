#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 15 12:20:04 2020

@author: francescaronci
"""

import json
import pandas as pd
from numpy import var
import datetime
import argparse
import time

parser=argparse.ArgumentParser()

# parser.add_argument("-i", "--input_data", help="Complete path to the file containing data (log)",
#                     type=str, default='./data/input_data.json')

parser.add_argument("-i", "--input_data", help="Complete path to the file containing data (log)",
                    type=str, default='./data/anonymous_logs_Fondamenti di informatica_20201118-1702.json')

parser.add_argument("-o", "--user_feature", help="Complete path to the file containing data output(.json)", 
                    type=str, default='./results/user_feature.json')

parser.add_argument("-o1", "--out_data1", help="Complete path il file input_data.json", 
                    type=str, default='./results/PythonExport.xlsx')

args=parser.parse_args()

#Leggo lista Log anonimizzati in fomrato json e memorizzo in data

fin=open(args.input_data,'r')
text=fin.read()
data=json.loads(text)

# #Inizio a contare il tempo di esecuzione 
start = time.perf_counter_ns()


# salvo i dati su una struttura di tipo DataFrame 

df=pd.DataFrame(data, columns=['Data/Ora', 'ID','Contesto Evento',
                               'Componente', 'Evento', 'Descrizione', 'Origine', 'IP'])



#Conto numero totale utenti

utenti=df['ID'].unique()
utenti.sort() #ordino gli utenti in ordine crescente
numero_utenti=df['ID'].unique().size


#Conto numero di log per utente (quindi il numero di eventi totali per utente)

numlog_utente=df['ID'].value_counts()
numlog_utente.index=utenti
  
#Creo lista di eventi, del numero di log per evento e conto gli eventi diversi

lista_eventi=list((df['Evento']))
numerolog_evento=df['Evento'].value_counts()
eventi_diversi=numerolog_evento.index


#Calcolo quanti log ci sono per ciascun giorno

dt= pd.to_datetime(df['Data/Ora'])
giorni= dt.dt.day
log_giorno= giorni.value_counts()

#Conto le settimane
lista_settimane=[]
for i in range(len(dt)):
    lista_settimane += [dt[i].strftime('%W')]

df['Settimana']= lista_settimane
numero_settimane= df['Settimana'].unique()

#Calcolo la media e la varianza del numero di eventi di ogni utente in 7 giorni

df_sett= pd.DataFrame(0,index=utenti, columns=numero_settimane)

for i in range(len(df)):
 df_sett.loc[(df.loc[i,'ID'],df.loc[i,'Settimana'])] += 1


mean=[]
varianza=[]
for user in utenti:
   mean.append(df_sett.loc[user].sum()/len(numero_settimane))
   varianza.append(var(df_sett.loc[user]))

#Assegno indici corrispondenti agli ID degli utenti alle righe di df 

df.index=[df['ID']]

   
#Conto il numero di volte in cui ciascun utente ha fatto un evento specifico
# e inserico i dati in una lista (tabella_evento)    

tabella_evento=[]

for user in utenti:
  
    tabella_evento.append((df.loc[user,'Evento'].value_counts()))


#Estraggo dal DataFrame la data del primo e dell'ultimo evento per ciascun utente

data_ultimo_ev=[]
data_primo_ev=[]

for utente in utenti:
    date_utente=df.loc [(df ['ID'] == utente)]  
    prima=date_utente['Data/Ora'].sort_values(ascending=True).iloc[0]  #ordino le date in ordine crescente e prendo la prima
    ultima=date_utente['Data/Ora'].sort_values(ascending=False).iloc[0] #ordino le date in ordine secrescente e prendo la prima
    data_primo_ev.append(prima)
    data_ultimo_ev.append(ultima)  

#Calcolo giorni tra primo e ultimo evento  e li salvo in una lista (distanza_primo_ultimo)

distanza_primo_ultimo=[]
date_primo=[] #lista contenete date_primo_ev
date_secondo=[] #lista contenete date_ultimo_ev
data=0

for data in range(len(data_primo_ev)):
    date_primo.append(datetime.datetime.strptime(data_primo_ev[data], '%d/%m/%Y %H:%M').date()) #trasformo data da str a obj
for data in range(len(data_ultimo_ev)):
    date_secondo.append(datetime.datetime.strptime(data_ultimo_ev[data], '%d/%m/%Y %H:%M').date())
        
for data in range(len(date_primo)):
  distanza_primo_ultimo.append(abs((date_primo[data]-date_secondo[data]).days)) 
  
              
                                                   
#Creo lista features utenti e lo salvo in un DataFrame (user_features) 

tabella_features=[]
j=0
for j in range(len(utenti)):
     tabella_features.append([utenti[j], numlog_utente[j], data_primo_ev[j], data_ultimo_ev[j], distanza_primo_ultimo[j], 
                     mean[j], varianza[j]])
    
     
user_features=pd.DataFrame(tabella_features, columns= [ 'ID','Eventi totali' , 'Data Primo Evento', 
                                                       'Data Ultimo Evento ', 'Distanza Prima-Ultima Data', 'Media', 'Varianza'])



# Inserisco nel DataFrame il numero di volte che ciascun utente ha fatto l'evento specifico 

u=0 #utente
e=0 #evento relativo ad un solo utente

for u in range(len(utenti)):    
    for evento in eventi_diversi:
     for e in range(len(tabella_evento[u])):
         if evento == tabella_evento[u].index[e]: # se l'evento corrisponde all'evento dell'utente...
            user_features.loc[u,evento]=tabella_evento[u].values[e] #...inserisco l'informazione nel DataFrame
   
    
   
#sostituisco i valori NaN con 0
user_features.fillna(0, inplace = True)


#Calcolo tempo di esecuzione
elapsed = time.perf_counter_ns() - start
print('*** elapsed ***', elapsed / 1000000000.0)

#salvo file Excel

user_features.to_excel(args.out_data1)

# salvo file .json

user_features.to_json(args.user_feature, orient='columns') 

























 