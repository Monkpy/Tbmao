# -*- coding: utf-8 -*-
import csv
import time

import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, date, timedelta

# 检查数据库数据重复数据---返回相应ID
with open('./tlx.csv', 'w') as f:
	f = csv.writer(f)
	f.writerow(('INFO_ID', 'TEXT'))

# engine = create_engine('mysql+pymysql://用户名:密码@ip:3306/数据库')
engine = create_engine('mysql+pymysql://test:123456@127.0.0.1:3306/tb')

ltx = []

while True:
	RELEASE_DATE = (date.today() + timedelta(days=-0)).strftime("%Y-%m-%d")  # 创建时间
	print(RELEASE_DATE)
	sql = '''select ID from t_tender_info  where RELEASE_DATE = "%s"''' % RELEASE_DATE
	print(sql)
	dfx = pd.read_sql_query(sql, engine)
	arrayx = dfx.values  # 获取表一ID的值
	df2 = pd.read_csv('./tlx.csv', usecols=['INFO_ID'])  # 读取存储表二数据的表格
	if df2.notnull:
		array = df2.values
		for i in arrayx:
			if i in array:
				pass
			else:
				sql2 = '''select INFO_ID, TEXT from t_tender_info_detail where INFO_ID = "%s"''' % str(i)[1:-1]
				print(sql2)
				df1 = pd.read_sql_query(sql2, engine)
				print('send sql to mysql')
				df1.to_csv('./tlx.csv', index=0, header=False, mode='a', encoding='utf-8')
				print('save to tlx.csv')
				tb2 = pd.read_csv('./tlx.csv')  # 打开存储表二数据的表格
				df2 = tb2.drop_duplicates(subset=['TEXT'], keep='last')  # 根据TEXT筛选重复数据，保留重复出现的最后一个
				df2.to_csv('./tl2.csv', index=0)
				print('save to tl2.csv')
				dl = pd.read_csv('./tl2.csv', usecols=['INFO_ID'])  # 打开根据INFO_ID遍历筛选后的表格
				lt2 = []
				arryl = dl.values  # 获取去重处理后的数据的INFO_ID的值
				for i2 in arryl:
					i2 = list(i2)
					lt2.append(i2)
				dl2 = pd.read_csv('./tlx.csv', usecols=['INFO_ID'])  # 根据INFO_ID遍历总数据
				arryl2 = dl2.values  # 获取总数据的INFO_ID
				for i3 in arryl2:
					if i3 in lt2:
						pass
					else:
						i3 = list(i3)
						if i3[0] in ltx:
							pass
						else:
							# print(i3)
							i = list(i3)
							ltx.append(i3[0])
						print(ltx)
						T = time.strftime('%H:%M:%S')
						if T == '00:00:00':
							break

	else:
		pass


