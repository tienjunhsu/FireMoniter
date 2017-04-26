#! /user/bin/env python
from unittest import TestCase

import datetime

delivery_dates = ['2017-01-20', '2017-02-17', '2017-03-17', '2017-04-21', '2017-05-19', '2017-06-16', '2017-07-21',
                  '2017-08-18', '2017-09-15', '2017-10-20', '2017-11-17', '2017-12-15']
today = datetime.date.today().strftime('%Y-%m-%d')
month = int(today[5:7])
year = int(today[2:4])
if today > delivery_dates[month -1]:
    month += 1
if month > 12:
    month %= 12
    year += 1
contract =  str(year) + '%02d'% month
print([prefix + contract+'.CFE' for prefix in ['IC','IF','IH']])


