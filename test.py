from datetime import datetime
from datetime import timedelta

a = datetime.today().date() - datetime(2017,6,2).date()

if a > timedelta(days=5):
    print(a)