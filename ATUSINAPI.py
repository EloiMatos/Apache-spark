# UPDATE PRICES ON DB

import os
import shutil

import dowload_files as dwf
import extract_files as ef
import upload_composin as usin


def ATUSINAPI(yearmonth):

    states = [
        'PR','RS','SC'
    ]

    names = dwf.dowload_files(yearmonth, states)
    files = ef.filter_files(names, yearmonth, states)
    
    usin.upload(files.composin, yearmonth)

    print(f'SINAPI UPDATE DONE FOR {yearmonth}')


ATUSINAPI('202311')
