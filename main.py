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

parser = argparse.ArgumentParser()

parser.add_argument("-i", "--in_data", help="Complete path il file input_data.json",
                    type=str, default='./data/input_data.json')
parser.add_argument("-o", "--out_data", help="Complete path il file input_data.json",
                    type=str, default='./results/output_data.json')

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
        return pd.DataFrame(0, index=list(range(1)), columns=[
            'eventi_tot',
            'data_primo_ev',
            'data_ultimo_ev',
            'giorni_primo_ultimo',
            'media',
            'varianza'
        ])


# acquisisci lista di log e mmeorizzalo in un data frame
reader = LogReader.create_instance(args.in_data)
df_logs = reader.get_list_of_log()

# calcola le features a memorizzale in un data frame
extractor = FeatureExtractor()
df_out = extractor.extract_features(df_logs)

# salva le features calcolate
df_out.to_json(args.out_data, orient='columns', indent=3)






