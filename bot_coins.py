import datetime
import itertools
import os
import time
import psycopg2
from time import sleep
from time import gmtime, strftime
from datetime import timedelta
from binance.client import Client
import pandas as pd
from string import Template
import mysql.connector as mariadb
mariadb_connection = mariadb.connect(user='crypdxzp_datauser', password='BE5zXImvh-7zi3bKOmxp', database='crypdxzp_data', host='127.0.0.1', port='3306')
cursor = mariadb_connection.cursor()


api_key = 'aqui debes poner la api_key'
api_secret = 'api_secret'






client = Client(api_key,api_secret)
def clear():
    os.system('clear')
    return
def verif_pos(data_comp, carry_obj, c_obj):
	carry = 1
	c1 = 0
	hab2 = False
	
	down_stat =0
	up_stat =0
	
	for x in data_comp:
					
		if carry == carry_obj and hab2 == False:
			print('Hemos encontrado un cambio en la posicion : ', carry_obj)
			
			if x == True:
				print(c1)
				print(x)
				print(c_obj)
				if c1 > c_obj:
					print("DOWN")
					down_stat=c1
				elif c1 < c_obj:
					print("UP")
					up_stat=c1
			hab2 = True
		c1 = c1 + 1	

		if c1 > 10:
			c1 = 1
			carry = carry + 1
			if hab2 == True:
				hab2 = False
		elif hab2 == True:
			if x == True:
				print(c1)
				print(x)
				print(c_obj)
				if c1 > c_obj:
					print("DOWN")
					down_stat=c1
				elif c1 < c_obj:
					print("UP")
					up_stat=c1

	return (down_stat, up_stat)

def verif_pos_25(data_comp2, data_porcen, carry_obj2, c_obj2):
	carry = 1
	c1 = 0
	hab2 = False
	
	down_stat2 =0
	up_stat2 =0
	up_porc2 =0
	up_porc2v=0.0
	down_porc2 =0
	down_porc2v=0.0
	
	for x in data_comp2:
					
		if carry == carry_obj2 and hab2 == False:
			print('Hemos encontrado un cambio en la posicion : ', carry_obj2)
			
			if x == True:
				print(c1)
				print(x)
				print(c_obj2)
				if c1 > c_obj2:
					print("DOWN")
					down_stat2=c1
					carry_por = 1
					c_por = 0
					hab_por = False
					for y in data_porcen:
						c_por = c_por + 1
						if c_por > 25:
							c_por = 1
							carry_por = carry_por + 1
						
						if carry_por == carry_obj2 and c_por == c1:
							print("Update porcentaje in ")
							print(c1)
							print(y)
							down_porc2=c_por
							down_porc2v=y
					
				elif c1 < c_obj2:
					print("UP")
					up_stat2=c1
					carry_por = 1
					c_por = 0
					hab_por = False
					for y in data_porcen:
						c_por = c_por + 1
						if c_por > 25:
							c_por = 1
							carry_por = carry_por + 1
						
						if carry_por == carry_obj2 and c_por == c1:
							print("Update porcentaje in ")
							print(c1)
							print(y)
							up_porc2=c_por
							up_porc2v=y
												
						
						
						
						
			hab2 = True
		c1 = c1 + 1	

		if c1 > 25:
			c1 = 1
			carry = carry + 1
			if hab2 == True:
				hab2 = False
		elif hab2 == True:
			if x == True:
				print(c1)
				print(x)
				print(c_obj2)
				if c1 > c_obj2:
					print("DOWN")
					down_stat2=c1
					carry_por = 1
					c_por = 0
					hab_por = False
					for y in data_porcen:
						c_por = c_por + 1
						if c_por > 25:
							c_por = 1
							carry_por = carry_por + 1
						
						if carry_por == carry_obj2 and c_por == c1:
							print("Update porcentaje in ")
							print(c1)
							print(y)
							down_porc2=c_por
							down_porc2v=y
					
				elif c1 < c_obj2:
					print("UP")
					up_stat2=c1
					carry_por = 1
					c_por = 0
					hab_por = False
					for y in data_porcen:
						c_por = c_por + 1
						if c_por > 25:
							c_por = 1
							carry_por = carry_por + 1
						
						if carry_por == carry_obj2 and c_por == c1:
							print("Update porcentaje in ")
							print(c1)
							print(y)
							up_porc2=c_por
							up_porc2v=y

	return (down_stat2, up_stat2, up_porc2, up_porc2v, down_porc2, down_porc2v)

if __name__ == "__main__":
    while True:
		
		query = "SELECT * FROM top_coins;"
		df  = pd.read_sql_query(query,mariadb_connection)
		query2 = "SELECT * FROM top_25_coins_new;"
		df_25  = pd.read_sql_query(query2,mariadb_connection)		
		print('Recopilando datos iniciales.....')
		
		for x in range(0,510):
			if x == 0:
				countx = 0
				seg = 0
				minute = 0
			elif seg >= 1:
				countx = seg
			print('---------------------Tiempo de Actualizacion-----------------------')
			print('--------------------------------------------')
			print ('Minutes:' + '%.2f' % minute + ' Segundos :' + '%.2f' % seg)
			seg = seg + 1
			
			
			if seg >=60:
				seg = 0
				minute = minute + 1
			time.sleep(1)
			clear()
			
		
			
		query = "SELECT * FROM top_coins;"
		df1  = pd.read_sql_query(query,mariadb_connection)
		query2 = "SELECT * FROM top_25_coins_new;"
		df1_25  = pd.read_sql_query(query2,mariadb_connection)
		print('---------------------2 list-----------------------')
		print(df_25)
		print('--------------------------------------------')
		print(df1_25)
		sleep(1)
		

		comparisons = [a == b for (a, b) in itertools.product(df['coin_name'], df1['coin_name'])]

		comparisons2 = [a == b for (a, b) in itertools.product(df_25['coin_name'], df1_25['coin_name'])]

		comparisons1 = [b - a for (a, b) in itertools.product(df_25['porcentaje'], df1_25['porcentaje'])]
		
		carry = 1
		compresult =[]
		stable_stat =[]
		up_stat_com =[]
		down_stat_com =[]
		c = 0
		hab = 1

		for x in comparisons:
			c = c + 1
			if (c == carry) and (hab == 1):
				compresult.append(x)
				if x == 0:
					(down_stat, up_stat) = verif_pos(comparisons,carry,c)
					up_stat_com.append(up_stat)
					down_stat_com.append(down_stat)
					
					
				else:
					stable_stat.append(carry)
				carry = carry + 1				
				hab = 0
			elif c > 10:
				c = 1
				hab = 1
		
		print(compresult)
		print(stable_stat)
		print(up_stat_com)
		print(down_stat_com)

		carry2 = 1
		compresult2 =[]
		stable_stat2 =[]
		up_stat_com2 =[]
		down_stat_com2 =[]
		up_porc2_com = []
		up_porc2v_com = []
		down_porc2_com = []
		down_porc2v_com = []
		c2 = 0
		hab2 = 1

		for x2 in comparisons2:
			c2 = c2 + 1
			if (c2 == carry2) and (hab2 == 1):
				compresult2.append(x2)
				if x2 == 0:
					(down_stat2, up_stat2, up_porc2, up_porc2v, down_porc2, down_porc2v) = verif_pos_25(comparisons2,comparisons1,carry2,c2)
					up_stat_com2.append(up_stat2)
					down_stat_com2.append(down_stat2)
					up_porc2_com.append(up_porc2)
					#if up_porc2v != 0.0:
					up_porc2v_com.append(up_porc2v)
					down_porc2_com.append(down_porc2)
					down_porc2v_com.append(down_porc2v)
					
				else:
					stable_stat2.append(carry2)
				carry2 = carry2 + 1				
				hab2 = 0
			elif c2 > 25:
				c2 = 1
				hab2 = 1
		
		print(compresult2)
		print(stable_stat2)
		print(up_stat_com2)
		print(down_stat_com2)
		print(up_porc2_com)
		print(up_porc2v_com)
		print(down_porc2_com)
		print(down_porc2v_com)
		
		count = 0
		for valor in df1['coin_name']:
			count = count + 1
			cursor.execute("""UPDATE top_10_comp SET coin_name = %s WHERE coin_id = %s""",(valor,count))
			mariadb_connection.commit()
			
			
		count1 = 0
		for valor1 in df1['porcentaje']:
			count1 = count1 + 1
			cursor.execute("""UPDATE top_10_comp SET porcentaje = %s WHERE coin_id = %s""",(valor1,count1))
			mariadb_connection.commit()
		
		count2 = 0
		for valor2 in compresult:
			count2 = count2 + 1
			cursor.execute("""UPDATE top_10_comp SET status = %s WHERE coin_id = %s""",(valor2,count2))
			mariadb_connection.commit()

		count3 = 0
		for valor3 in stable_stat:
			cursor.execute("""UPDATE top_10_comp SET tend = %s WHERE coin_id = %s""",("STABLE",valor3))
			mariadb_connection.commit()
			

		count4 = 0
		for valor4 in up_stat_com:
			cursor.execute("""UPDATE top_10_comp SET tend = %s WHERE coin_id = %s""",("UP",valor4))
			mariadb_connection.commit()
			

		count5 = 0
		for valor5 in down_stat_com:
			cursor.execute("""UPDATE top_10_comp SET tend = %s WHERE coin_id = %s""",("DOWN",valor5))
			mariadb_connection.commit
			
			
		count6 = 0
		for valor6 in stable_stat2:
			cursor.execute("""UPDATE top_25_coins_new SET status = %s WHERE coin_id = %s""",("STABLE",valor6))
			mariadb_connection.commit()

		count7 = 0
		for valor7 in up_stat_com2:
			cursor.execute("""UPDATE top_25_coins_new SET status = %s WHERE coin_id = %s""",("UP",valor7))
			mariadb_connection.commit()

		count8 = 0
		for valor8 in down_stat_com2:
			cursor.execute("""UPDATE top_25_coins_new SET status = %s WHERE coin_id = %s""",("DOWN",valor8))
			mariadb_connection.commit

		count9 = -1
		for valor9 in up_porc2_com:
			count9 = count9 + 1
			cursor.execute("""UPDATE top_25_coins_new SET porcomp = %s WHERE coin_id = %s""",(up_porc2v_com[count9],valor9))
			mariadb_connection.commit
			
		count10 = -1
		for valor10 in down_porc2_com:
			count10 = count10 + 1
			cursor.execute("""UPDATE top_25_coins_new SET porcomp = %s WHERE coin_id = %s""",(down_porc2v_com[count10],valor10))
			mariadb_connection.commit

		query = "SELECT * FROM top_10_comp;"
		dat_1  = pd.read_sql_query(query,mariadb_connection)
		print('---------------------10 comlist-----------------------')
		print(dat_1)
		sleep(15)
		
		query2 = "SELECT * FROM top_25_coins_new;"
		dat_2  = pd.read_sql_query(query2,mariadb_connection)
		print('---------------------25 comlist-----------------------')
		print(dat_2)
		sleep(15)

