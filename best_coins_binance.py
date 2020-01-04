import datetime
from time import sleep
from time import gmtime, strftime
from datetime import timedelta
from binance.client import Client
import pandas as pd
import psycopg2
from string import Template
import mysql.connector as mariadb
mariadb_connection = mariadb.connect(user='crypdxzp_datauser', password='BE5zXImvh-7zi3bKOmxp', database='crypdxzp_data', host='127.0.0.1', port='3306')
cursor = mariadb_connection.cursor()


import twitter
api = twitter.Api(consumer_key='QJ9CyPJ1W2lvfKvRn88COS7SS',
                      consumer_secret='6SiwdIV0ye4baDzQBN6FsrpJwSaE0VZK8bqiJcy1FRLoLtmmze',
                      access_token_key='1017820859464060928-Bq3iABtXQOnoX3xo98VmtQ4dfjB8Nd',
                      access_token_secret='BjkBmG1Tkz2o3CsnCcAVLR6i1eFHbk93GG9w8aEufZFyZ')

api_key = 'j2vIU7x8rPrpQDgoazh9NqzNklHk6nQJInVLFQcgbOdUx29dZSs4X11CwIjz4Qyf'
api_secret = '56LFFpQe6pJjbxCth0q219tn97f5dE3RSo0arZFQOKhjpO7RGX3vgEt7TlmEZyLh' 

client = Client(api_key,api_secret)

if __name__ == "__main__":
    while True:


			cryptos= client.get_products()
			crypto_df= pd.DataFrame(cryptos['data'])
			

			crypto_df= crypto_df[['baseAsset', 'baseAssetName', 'quoteAsset', 'symbol']]
			crypto_df.set_index('symbol')

			crypto_df= crypto_df[crypto_df['quoteAsset'] == 'BTC']
			crypto_df.set_index('baseAsset')
			dict_cryptos={}
			for i in range(len(crypto_df.baseAsset)):
				
				print(crypto_df.symbol.values[i])
				if crypto_df.symbol.values[i] != "MATICBTC" and crypto_df.symbol.values[i] != "ATOMBTC" and crypto_df.symbol.values[i] != "PHBBTC" and crypto_df.symbol.values[i] != "ONEBTC" and crypto_df.symbol.values[i] != "ALGOBTC" and crypto_df.symbol.values[i] != "ERDBTC" and crypto_df.symbol.values[i] != "WINBTC" and crypto_df.symbol.values[i] != "COSBTC" and crypto_df.symbol.values[i] != "TOMOBTC" and crypto_df.symbol.values[i] != "BANDBTC" and crypto_df.symbol.values[i] != "XTZBTC":
					dict_cryptos[crypto_df.baseAsset.values[i]]= client.get_historical_klines(symbol=crypto_df.symbol.values[i], interval= '4h', start_str= '17 days ago UTC')
				sleep(0.1)
##Sharpe Ratio
			sharpe_dict={}
			for i in range(len(dict_cryptos)):
				vars() [list(dict_cryptos.keys())[i]]= pd.DataFrame(list(dict_cryptos.values())[i],  columns=['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'])
				vars() [list(dict_cryptos.keys())[i]]['Open time']= pd.to_datetime(vars() [list(dict_cryptos.keys())[i]]['Open time'], unit='ms')
				vars() [list(dict_cryptos.keys())[i]].set_index('Open time',inplace=True)
				vars() [list(dict_cryptos.keys())[i]]['Close']= vars() [list(dict_cryptos.keys())[i]]['Close'].astype(float, copy=False)
				vars() [list(dict_cryptos.keys())[i]]['Norm']= vars() [list(dict_cryptos.keys())[i]]['Close']/vars() [list(dict_cryptos.keys())[i]]['Close'][0]
				vars() [list(dict_cryptos.keys())[i]]['Daily Return']= vars() [list(dict_cryptos.keys())[i]]['Norm'].pct_change(1)
				vars() [list(dict_cryptos.keys())[i]]['Daily Return'].dropna(inplace=True)
				sharpe_dict[list(dict_cryptos.keys())[i]]= vars() [list(dict_cryptos.keys())[i]]['Daily Return'].mean()/vars() [list(dict_cryptos.keys())[i]]['Daily Return'].std()*(252**0.5)

			sharpe_sr= pd.Series(sharpe_dict)

			best = sharpe_sr.nlargest(10)
			best2 = sharpe_sr.nlargest(25) 
			
			#Actualizar BD 
			col1 = best.index
			count1 = 0
			for valor2 in best:
				
				count1 = count1 + 1
				print (valor2)
				print (count1)
				cursor.execute("""UPDATE top_coins SET porcentaje = %s WHERE coin_id = %s""",(valor2,count1))
				mariadb_connection.commit()
       

			count = 0
			
			for valor in col1:
				
				count = count + 1
				print (valor)
				print (count)
				cursor.execute("""UPDATE top_coins SET coin_name = %s WHERE coin_id = %s""",(valor,count))
				mariadb_connection.commit()
				
			col2 = best2.index
			count3 = 0
			for valor3 in best2:
				
				count3 = count3 + 1
				print (valor3)
				print (count3)
				cursor.execute("""UPDATE top_25_coins_new SET porcentaje = %s WHERE coin_id = %s""",(valor3,count3))
				mariadb_connection.commit()
       

			count4 = 0
			
			for valor4 in col2:
				
				count4 = count4 + 1
				print (valor4)
				print (count4)
				cursor.execute("""UPDATE top_25_coins_new SET coin_name = %s WHERE coin_id = %s""",(valor4,count4))
				mariadb_connection.commit()
				
		
			
			#Seleccionar tiempo actualizado
			query = "SELECT twitter_time FROM top_coins;"
			df  = pd.read_sql_query(query,mariadb_connection)
			d1 = pd.DataFrame(df)
			time1 =d1['twitter_time'][0]
			time2 = pd.to_timedelta(time1)
			
			#Tiempo seleccionado + 1 hs
			time3 = pd.Timedelta(time2)
			time4 = time3.total_seconds()
			print 'hora actualizaci0n:',(time4)
			hoursd = (time4 // 3600) + 1				
			minutesd = (time4 % 3600) // 60
			secondsd = time4 % 60
			timeact = timedelta(hours=hoursd,minutes=minutesd,seconds=secondsd)
			timeactdelta = pd.Timedelta(timeact)
			timeactseconds = timeactdelta.total_seconds()
			
			print 'hora Nueva:',(timeactseconds)
			
			
			#Tiempo actual a comparar
			
			d = datetime.datetime.now()
			#for attr in [ 'hour', 'minute', 'second']:
			#	print attr, ':', getattr(d, attr)
			
			hoursact = getattr(d, 'hour')
			minutesact = getattr(d, 'minute')
			secondsact = getattr(d, 'second')
			
			timeactual = timedelta(hours=hoursact,minutes=minutesact,seconds=secondsact)
			timeactualdelta = pd.Timedelta(timeactual)
			timeactualseconds = timeactualdelta.total_seconds()
			
			print 'hora actual:',(timeactualseconds)
			
			if (timeactualseconds <= timeactseconds) and (timeactualseconds >= time4) :
				print('Estamos recopilando datos /n')
				print(best2.to_string)
				
			
			else:
				print('Actualizando twitter')
				#send_twit = best.to_string()
				s = Template('Top 10 coins 1 hour \n $what')
				send_twit = s.substitute(what=best.to_string())
				#print(api.VerifyCredentials())
				status = api.PostUpdate(send_twit)
				
				#Tiempo actualizar
				valor3 = strftime("%H:%M:%S", gmtime())
				count3 = 1
				cursor.execute("""UPDATE top_coins SET twitter_time = %s WHERE coin_id = %s""",(valor3,count3))
				mariadb_connection.commit()
			

       
			
			
			
		
		
			
		
			



