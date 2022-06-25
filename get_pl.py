import mysql.connector
import pandas as pd
import os
from datetime import datetime, timedelta

today = datetime.today() - timedelta(days=1)
day = today.strftime("%Y-%m-%d")

# DB CONFIG
conn = mysql.connector.connect(
    host="10.47.150.144",
    user="rto_jkt",
    password="Rt0_J4k4Rt4@ts3L",
    database="capmon"
)
# QUERY
query = '''SELECT '3G', tanggal, siteid, IFNULL(AVG(packetloss),0) PL
FROM `capmon`.`sum_packetloss_hourly_3g`
WHERE `tanggal` BETWEEN "'''+day+'''" AND "'''+day+'''" AND LEFT(siteid,3) IN (
'BDG','BDK','BDS','CMI','COD',
'BDB','IND','SUB','CRB','CMS',
'KNG','MJL','CJR','SMD',
'BJR','TSK','GRT','PAN','BDX')
GROUP BY tanggal, siteid
UNION
SELECT '4G',tanggal, siteid, IFNULL(AVG(packetloss),0) PL
FROM `capmon`.`sum_packetloss_hourly_4g`
WHERE `tanggal` BETWEEN "'''+day+'''" AND "'''+day+'''" AND LEFT(siteid,3) IN (
'BDG','BDK','BDS','CMI','COD',
'BDB','IND','SUB','CRB','CMS',
'KNG','MJL','CJR','SMD',
'BJR','TSK','GRT','PAN','BDX')
GROUP BY tanggal, siteid;'''

name_file = 'pl_'+day+'.csv'
df = pd.read_sql(query, conn)
pivot = pd.pivot_table(df, values='PL', index=[
                       'tanggal', 'siteid'], columns=['3G'])

pivot.to_csv(
    '''F:/KY/packetloss/download/'''+name_file)
