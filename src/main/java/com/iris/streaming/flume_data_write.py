# # -*- coding: utf-8 -*-
# import random
# import time
# import pandas as pd
# import json
#
# writeFileName="./flume_exec_test.txt"
# cols = ["order_id","user_id","eval_set","order_number","order_dow","hour","day"]
# df1 = pd.read_csv('../order_data/orders.csv')
# df1.columns = cols
# df = df1.fillna(0)
# with open(writeFileName,'a+')as wf:
# 	for idx,row in df.iterrows():
# 		d = {}
# 		for col in cols:
# 			d[col]=row[col]
# 		js = json.dumps(d)
# 		wf.write(js+'\n')
# 	#	rand_num = random.random()
#             #	time.sleep(rand_num)
#
