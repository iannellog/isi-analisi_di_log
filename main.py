# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 21:57:27 2020

@author: gaiad
"""

import json
import pandas as pd
import datetime
from numpy import var
import argparse

parser=argparse.ArgumentParser()

parser.add_argument("-i", "--in_data", help="Complete path il file input_data.json", 
                    type=str, default='./data/input_data.json')
parser.add_argument("-o", "--out_data", help="Complete path il file input_data.json", 
                    type=str, default='./results/output_data.json')
parser.add_argument("-o1", "--out_data1", help="Complete path il file input_data.json", 
                    type=str, default='./results/PythonExport.xlsx')

args=parser.parse_args()


#Leggo lista dei log anonimizzati in formato json e memorizzo su struttura dati python
fin=open(args.in_data,'r')
text=fin.read()
data1=json.loads(text)[0]

df=pd.DataFrame(data1, columns=['Data/Ora','ID','UC','contesto evento','componente','evento','descrizione','origine','IP'])


#Numero totale utenti 
users=df['ID'].unique()
tot_users=df['ID'].unique().size


#Numero log per utente
numlog_utente=df['ID'].value_counts()


#Quanti log ogni giorno
dt=pd.to_datetime(df['Data/Ora'])      #estraggo la serie di date e dalle date i giorni
lista_giorni=dt.dt.day
numlog_giorno=lista_giorni.value_counts()

#Creo le liste data_primo_ev e data_ultimo_ev
data_primo_ev=[]
data_ultimo_ev=[]
for user in users:
    date_utente=df.loc [(df ['ID'] == user)]  #riduco il df solo al singolo utente
    prima=date_utente['Data/Ora'].sort_values(ascending=True).iloc[0]  #ordino le date in ordine decrescente, prendo la prima
    ultima=date_utente['Data/Ora'].sort_values(ascending=True).iloc[len(date_utente)-1] #prendo l'ultima
    data_primo_ev.append(prima)
    data_ultimo_ev.append(ultima)  #le voglio mettere come colonne del dataframe
    

#calcolo giorni tra primo e ultimo evento e li salvo in dist_primo_ultimo
dist_primo_ultimo=[]
A=[]
B=[]
for date1 in range(len(data_primo_ev)):
    A.append(datetime.datetime.strptime(data_primo_ev[date1], '%d/%m/%Y %H:%M').date())
for date2 in range(len(data_ultimo_ev)):
    B.append(datetime.datetime.strptime(data_ultimo_ev[date2], '%d/%m/%Y %H:%M').date())
        
for date in range(len(A)):
  dist_primo_ultimo.append(abs((A[date]-B[date]).days)) 
  
dfout=pd.DataFrame({'eventi_tot':numlog_utente, 'data_primo_ev': data_primo_ev, 'data_ultimo_ev': data_ultimo_ev, 'giorni_primo_ultimo': dist_primo_ultimo}, index=users)



#Calcolo accessi utente per evento e li salvo in dfout
eventi=df['evento'].unique()

"""
criptico
"""
for evento in eventi:
    numlog_utente_evento=[]
    for user in users:
    
      a = df['evento'] == evento  #seleziono le righe con l'evento specifico
      b = df['ID'] == user        #seleziono le righe con l'utente specifico
      numlog_utente_evento.append(df[a & b].shape[0])  #Faccio un and, True solo se soddisfo utente e evento specifico, prendo con shape la dimensione (coincide con il # di volte in cui compare l'evento)
    dfout[evento]=numlog_utente_evento   #Aggiungo per ogni evento una nuova riga al df_out 



#Calcolo la media e la varianza del numero di eventi in una settimana (lunedì-domenica)

media_eventi_week=[]
varianza_eventi_week=[]
for user in users:
    giorni_utente=list(df.loc [(df ['ID'] == user),'Data/Ora'])
    date_utente=[]
    date_week=[]
    for data in range(len(giorni_utente)):
        
        date_utente.append(datetime.datetime.strptime(giorni_utente[data], '%d/%m/%Y %H:%M'))
        if (abs(date_utente[0]-date_utente[data]).days)<=6:          #prende solo i giorni che hanno distanza dal primo pari a 7 (da 0 a 6)
            date_week.append(abs(date_utente[0]-date_utente[data]).days)
    media_eventi_week.append(len(date_week) / (numlog_giorno.size/7))
    varianza_eventi_week.append(var(date_week))
dfout['media_eventi_week']=media_eventi_week               
dfout['varianza_eventi_week']=varianza_eventi_week
        

#Salvo in formato json e in formato excel
dfout.to_json(args.out_data, orient ='columns')  
dfout.to_excel(args.out_data1)






