import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import os

today = datetime.today() - timedelta(days=1)
day = today.strftime("%Y-%m-%d")

engine = create_engine('mysql://erikgin:V4Cged64@10.47.150.170/jabar_performance')

engine.execute('''DELETE FROM `temp_dataplan_pl`;''')

name_file = 'pl_'+day+'.csv'
df = pd.read_csv("F:/KY/packetloss/download/"+name_file,sep=',',quotechar='\'',encoding='utf8')
df.to_sql('temp_dataplan_pl',con=engine,index=False,if_exists='append')

engine.execute('''INSERT INTO jabar_performance.`jabar_data_plan_2022` (`date_id`,`site_id`,`pl_3g`,`pl_4g`)
SELECT `tanggal`,`siteid`,`3G`,`4G`
FROM jabar_performance.`temp_dataplan_pl`
ON DUPLICATE KEY UPDATE
jabar_performance.`jabar_data_plan_2022`.`pl_3g`=VALUES(`pl_3g`),
jabar_performance.`jabar_data_plan_2022`.`pl_4g`=VALUES(`pl_4g`);
''')

engine.execute('''DELETE FROM `temp_dataplan_pl`;''')

os.environ["https_proxy"] = "https://10.59.66.1:8080"
os.system("telegram-send Update_PL_Done")
