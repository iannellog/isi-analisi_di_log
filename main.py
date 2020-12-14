# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 21:57:27 2020

@author: gaiad, all
"""

import argparse
from os.path import splitext
import json
import pandas as pd
from abc import ABC, abstractmethod
import datetime


parser = argparse.ArgumentParser()

parser.add_argument("-i", "--in_data", help="Complete path il file input_data.json",
                    type=str, default='./data/input_data.json')
parser.add_argument("-o", "--out_data", help="Complete path il file input_data.json",
                    type=str, default='./results/output_data.xlsx')

args = parser.parse_args()


class LogReader(ABC):
    """
    interface
    """

    @abstractmethod
    def get_list_of_log(self):
        """
        abstract method
        """
        pass

    @staticmethod
    def create_instance(filename):
        suffix = splitext(filename)[1][1:].lower()
        if suffix == 'json':
            return JSONReader(filename)
        elif suffix == 'csv':
            return CSVReader(filename)
        else:
            raise ValueError('unknown file type')


class JSONReader(LogReader):

    def __init__(self, filename):
        self.filename = filename

    def get_list_of_log(self):
        fin = open(self.filename, 'r')
        text = fin.read()
        data = json.loads(text)
        # converto le date da stringa di caratteri a 'datetime'
        for log in data:
            log[0] = datetime.datetime.strptime(log[0], '%d/%m/%Y %H:%M')

        df = pd.DataFrame(data, columns=[
            'Data/Ora',
            'ID',
            'UC',
            'contesto evento',
            'componente',
            'evento',
            'descrizione',
            'origine',
            'IP'])
        return df


class CSVReader(LogReader):

    def __init__(self, filename):
        self.filename = filename

    def get_list_of_log(self):
        raise ValueError('CSV Reader not yet implemented')


class FeatureExtractor:

    def __init__(self):
        pass

    def extract_features(self, log_list):
        # estraggo la lista degli utenti
        users = log_list['ID'].unique()
        # creo il df di output
        df_out = pd.DataFrame(0, index=users, columns=[
            'eventi_tot',
            'data_primo_ev',
            'data_ultimo_ev',
            'giorni_primo_ultimo',
            'media',
            'varianza'
        ])
        # Numero log per utente
        numlog_utente = log_list['ID'].value_counts()
        df_out['eventi_tot'] = numlog_utente
        # Data primo/ultimo evento
        for user in users:
            date_utente = log_list.loc[(log_list['ID'] == user)]  # riduco il df_out solo al singolo utente
            prima = date_utente['Data/Ora'].sort_values(ascending=True).iloc[0]  # ordino le date in ordine decrescente, prendo la prima
            ultima = date_utente['Data/Ora'].sort_values(ascending=True).iloc[len(date_utente) - 1]  # prendo l'ultima
            df_out.loc[user, 'data_primo_ev'] = prima
            df_out.loc[user, 'data_ultimo_ev'] = ultima
        # calcolo giorni dal primo all'ultimo evento
        dist_primo_ultimo = []
        A = []
        B = []
        for date1 in df_out['data_primo_ev']:
            A.append(date1.date())
        for date2 in df_out['data_ultimo_ev']:
            B.append(date2.date())
        for i in range(len(A)):
            dist_primo_ultimo.append(abs((A[i] - B[i]).days))
        df_out['giorni_primo_ultimo'] = pd.Series(dist_primo_ultimo, index=users)
        df_temp = self.extract_event_count_per_user(log_list,users)
        return pd.concat([df_out, df_temp], axis=1)

    def extract_event_count_per_user(self, log_list, users):
        eventi = log_list['evento'].unique()
        df_temp = pd.DataFrame(0, index=users, columns=eventi)
        for i in range(log_list.shape[0]):
            df_temp.loc[log_list.loc[i, 'ID'], log_list.loc[i, 'evento']] += 1
        return df_temp


# acquisisci lista di log e mmeorizzalo in un data frame
reader = LogReader.create_instance(args.in_data)
df_logs = reader.get_list_of_log()

# calcola le features a memorizzale in un data frame
extractor = FeatureExtractor()
df_out = extractor.extract_features(df_logs)

# salva le features calcolate
#df_out.to_json(args.out_data, orient='columns', indent=3)
df_out.to_excel(args.out_data)






