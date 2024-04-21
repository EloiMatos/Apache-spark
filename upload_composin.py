import os
import time
import csv
import numpy
import pandas as pd

def upload(files, date):
    path_nf = []

    for file in files:
        origin = os.path.abspath('.')
        file_path = os.path.join(origin, 'EXTRACTION', file)

        if not os.path.exists(file_path):
            path_nf.append(f'ARQUIVO NÃO ENCONTRADO {file_path}')
            print(f'ARQUIVO NÃO ENCONTRADO {file_path}')
        else:
            state = file.split('_')
            state = state[5]
            dataframe = pd.read_excel(file_path)
            info = convert_data(dataframe)
            send_info(info, state, date)

    return path_nf


def convert_data(dataframe):

    dataframe = dataframe.drop(index=range(6))
    lines = dataframe.shape[0]
    dataframe = dataframe.drop(index=range(lines-3, lines))

    info = []

    for i in range(len(dataframe)-2):

        line = dataframe.iloc[i]
        line[6] = fix_number(line[6], True)
        line[0] = fix_text(line[0])
        line[2] = fix_text(line[2])
        line[7] = fix_text(line[7])
        line[8] = fix_text(line[8])
        line[10] = fix_number(line[10], False)

        info.append(composin(line[6], line[0],
                    line[2], line[7], line[8], line[10]))
    return info


def fix_text(value):
    value = str(value).replace(',', '')
    value = value.replace('  ', '')
    value = value.replace("'", "")
    return value


def fix_number(value, isint):
    value = str(value).replace('.', '')
    value = value.replace(',', '.')

    if isint:
        value = int(value)
    else:
        value = float(value)

    return value


class composin:
    def __init__(self, idsis, classe, tipo, descri, un, preco):
        self.idsis = idsis
        self.classe = classe
        self.tipo = tipo
        self.descri = descri
        self.un = un
        self.preco = preco

    def to_dict(self):
        return {
            "idsis": self.idsis,
            "classe": self.classe,
            "tipo": self.tipo,
            "descri": self.descri,
            "un": self.un,
            "preco": self.preco
        }

def send_info(info, state, date):

    print("info", info)

    date = int(date)
    file_name = f'composin_{state}_{date}.csv'
    file_header = ['idsis', 'classe', 'tipo', 'descri', 'un', 'preco']

    with open(file_name, 'w', newline='') as csv_file:
        to_write_csv = csv.DictWriter(csv_file, fieldnames=file_header)
        to_write_csv.writeheader()
        for data in info:
            to_write_csv.writerow(data.to_dict())
