
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


def OrdinaDate(df,utenti):
    '''La function restituisce la data del primo e dell'ultimo accesso alla pagina e-learning per ogni utente.'''
    #IN: dataframe iniziale e lista degli utenti.
    #OUT:due liste che contengono le informazioni relative agli accessi degli utenti.
    data_primo_ev=[]
    data_ultimo_ev=[]
    for user in utenti:
        date_utente=df.loc [(df ['ID'] == user)]  #riduco il df solo al singolo utente
        prima=min(date_utente['Data/Ora'])  #ordino le date in ordine decrescente, prendo la prima
        ultima=max(date_utente['Data/Ora']) #prendo l'ultima
        
        data_primo_ev.append(prima)
        data_ultimo_ev.append(ultima)  #le voglio mettere come colonne del dataframe
        
    return data_primo_ev,data_ultimo_ev


def DiffPrimoUltimo(data_primo_ev,data_ultimo_ev):
    '''La function calcola la differenza temporale tra il primo e l'ultimo accesso di ogni utente.'''
    #IN:liste contenenti le date di primo e ultimo accesso create nella function OrdinaDate.
    #OUT: lista contenente le distanze temporali.
    dist_primo_ultimo=[]
    A=[]
    B=[]
    for date1 in range(len(data_primo_ev)):
        A.append(datetime.datetime.strptime(data_primo_ev[date1], '%d/%m/%Y %H:%M').date())
    for date2 in range(len(data_ultimo_ev)):
        B.append(datetime.datetime.strptime(data_ultimo_ev[date2], '%d/%m/%Y %H:%M').date())
        
    for date in range(len(A)):
        dist_primo_ultimo.append(abs((A[date]-B[date]).days)) 
    
    return dist_primo_ultimo


#Calcolo la media e la varianza del numero di eventi di ogni utente in 7 giorni
def MediaVarianza(utenti, df, dt):
    '''La function calcola la media e la varianza del numero di eventi di ogni utente in 7 giorni.'''
    #IN: lista utenti, dataframe iniziale e dataframe delle date.
    #OUT: due liste contenenti rispettivamente le medie e le varianze degli utenti.
    #Conto le settimane
    lista_settimane=[]
    for i in range(len(dt)):
        lista_settimane += [dt[i].strftime('%W')]
    
    df['Settimana']= lista_settimane
    numero_settimane= df['Settimana'].unique()

    df_sett= pd.DataFrame(0,index=utenti, columns=numero_settimane)

    for i in range(len(df)):
        df_sett.loc[(df.loc[i,'ID'],df.loc[i,'Settimana'])] += 1
        
    mean=[]
    varianza=[]
    for user in utenti:
       mean.append(df_sett.loc[user].sum()/len(numero_settimane))
       varianza.append(var(df_sett.loc[user]))

    return mean, varianza


def AccessoUtentePerEvento(utenti, eventi_diversi, tabella_evento):
    '''La function calcola , per ogni utente, il numero di accessi a ogni singola attività della pagina del corso.''' 
    #IN: lista utenti, lista degli eventi, lista del numero accessi per ogni evento di ogni utente
    #OUT: dataframe finale con l'aggiunta delle informazioni di accesso.   
    # Inserisco nel DataFrame il numero di volte che ciascun utente ha fatto l'evento specifico.
    for u in range(len(utenti)):    
        for evento in eventi_diversi:
            for i in range(len(tabella_evento[u])):
             if evento == tabella_evento[u].index[i]: # se l'evento corrisponde all'evento dell'utente...
                user_features.loc[u,evento] = tabella_evento[u].values[i] #...inserisco l'informazione nel DataFrame
   
    #sostituisco i valori NaN con 0
    user_features.fillna(0, inplace = True)

    return user_features


#######################################################################################################################

#Definizione degli input
parser=argparse.ArgumentParser()

parser.add_argument("-i", "--input_data", help="Complete path to the file containing data (log)",
                    type=str, default='./data/anonymous_logs_Fondamenti di informatica_20201118-1702.json')

parser.add_argument("-o", "--user_feature", help="Complete path to the file containing data output(.json)", 
                    type=str, default='./user_feature.json')

parser.add_argument("-o1", "--out_data1", help="Complete path il file input_data.json", 
                    type=str, default='./PythonExport.xlsx')

args=parser.parse_args()


#Leggo lista Log anonimizzati in fomrato json e memorizzo in data
fin=open(args.input_data,'r')
text=fin.read()
data=json.loads(text)

#Inizio a contare il tempo di esecuzione 
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


#Calcolo della media e della varianza
media, varianza = MediaVarianza(utenti, df, dt)


#Assegno indici corrispondenti agli ID degli utenti alle righe di df 
df.index=[df['ID']]

   
#Conto il numero di volte in cui ciascun utente ha fatto un evento specifico
# e inserico i dati in una lista (tabella_evento)    
tabella_evento=[]
for user in utenti:
    tabella_evento.append((df.loc[user,'Evento'].value_counts()))


#Estraggo dal DataFrame la data del primo e dell'ultimo evento per ciascun utente
data_primo_ev, data_ultimo_ev = OrdinaDate(df, utenti)


#Calcolo giorni tra primo e ultimo evento  e li salvo in una lista (distanza_primo_ultimo)
distanza_primo_ultimo = DiffPrimoUltimo(data_primo_ev,data_ultimo_ev)
        
                                                   
#Creo lista features utenti e lo salvo in un DataFrame (user_features) 
tabella_features=[]
j=0
for j in range(len(utenti)):
     tabella_features.append([utenti[j], numlog_utente[j], data_primo_ev[j], data_ultimo_ev[j], distanza_primo_ultimo[j], 
                     media[j], varianza[j]])
        
user_features=pd.DataFrame(tabella_features, columns= [ 'ID','Eventi totali' , 'Data Primo Evento', 
                                                       'Data Ultimo Evento ', 'Distanza Prima-Ultima Data', 'Media', 'Varianza'])


#Aggiungo gli accessi per evento specifico di ogni utente
user_features = AccessoUtentePerEvento(utenti, eventi_diversi, tabella_evento)
user_features = user_features.set_index('ID')


#Calcolo tempo di esecuzione
elapsed = time.perf_counter_ns() - start
print('*** elapsed ***', elapsed / 1000000000.0)


#salvo file Excel
user_features.to_excel(args.out_data1)
# salvo file .json
user_features.to_json(args.user_feature, orient='columns', indent=2) 













































 