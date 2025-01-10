from datetime import datetime, timedelta

'''
You can set dates for last week start - end that
allows weekly partitions to run for all new found
DPX projects (based on retrieving dates from dB).
To be construncted
'''

START_DATE = str(datetime.now() - timedelta(days=10))[:10]
END_DATE = str(datetime.now() - timedelta(days=3))[:10]
