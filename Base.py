import sys
import os
import requests
import time
import argparse
import json
from datetime import datetime, timedelta


START_DATE = "1900-01-01"

class Base():
	def get_args(self):
		parser = self.createParser()
		namespace =  parser.parse_args()
		arguments = namespace.__dict__
		return arguments
			
	def createParser(self):
		parser = argparse.ArgumentParser()
		parser.add_argument("-d", "--date_from", default="1900-01-01")
		parser.add_argument("-e", "--date_to", default="1900-01-01")
		parser.add_argument("-c", "--config", default="config_test.json")
		parser.add_argument("-f", "--file_name", default="results.csv")
		return parser
    
	def get_parent_path(self):
		"""
		
		"""
		path = os.path.dirname(self.path)
		path = os.path.split(path)
		return path[0]
	
	def read_config(self, path):
		with open(path, 'r', encoding='utf-8') as f:
			return json.load(f)

	def recalculate_load_period(self):
			"""
			Если в конфигурационном файле указан сдвиг в днях daydiff, а период не передавался, то
			date_from, date_to определяются как текущий день минус daydiff
			:return:
			"""
			self.date_from = self.args["date_from"]
			self.date_to = self.args["date_to"]

			if "daydiff" in self.config:
				daydiff = self.config["daydiff"]
			else:
				daydiff = 0

			if self.date_from == START_DATE:
				self.date_from = str(datetime.now().date() - timedelta(days=daydiff))

			if self.date_to == START_DATE:
				self.date_to = str(datetime.now().date() - timedelta(days=daydiff))

	def __init__(self, path):
		'''
		Инициация загрузчика.
		Определяет параметры, передаваемые через  get_args:
		период - date_from, date_to,
		конфигурационный файл - config (по умолчанию - conf/config.json),
		имя файла - file_name

		Если в конфигурационном файле указан сдвиг в днях daydiff, а период не передавался, то
		date_from, date_to определяются как текущий день минус daydiff

		Запускает стандартный логгер. Параметры логеры обязательны и указываются в конфигурационном файле, в секции logger.
		Имена элементов logger должны быть - "log_name", "full_log_path", "logger_name".
		Стандартный лог будет писаться в файл с именем log_name_[текущая дата время] в директорию logs. Имя логера - logger_name.
		"""
		# пусть вызываемого файла. Того, который наследует этот класс '''
		self.path = path

		self.args = self.get_args()
		self.date_from = self.args["date_from"]
		self.date_to = self.args["date_to"]
		self.config = self.read_config(os.path.join(self.get_parent_path(),
                                                           "conf",
                                                           self.args["config"]))
		self.file_name = self.args["file_name"]
		self.success_codes = self.config["success_codes"]

		self.recalculate_load_period()

		self.start_dttm = datetime.now()
		
		print("Инициация загрузчика завешена.")
	
	def exp_delay(self, retry_counter, start_delay=20):
		"""
		Функция вычисления экспоненциальной задержки
		:return: int
		"""
		if retry_counter == 0:
			sleep_time_source = 0
		else:
			sleep_time_source = int(start_delay * 3 ** retry_counter)
		time.sleep(sleep_time_source)

	def load_request(self, params):
		if "n_tries" in self.config:
			n_tries = self.config["n_tries"]
		else:
			n_tries = 0

		if 'request_params' not in params:
			raise Exception("params must contain the 'request_params' element")
		

		retry_counter = 0

		while retry_counter < n_tries:
			self.exp_delay(retry_counter)
			retry_counter += 1

			try:
				response = requests.get(url = params["url"], headers = params["headers"], params = params["request_params"]) # Загрузка данных из API
			
				if response.status_code in self.success_codes:
					print(u'Запрос завершился успешно, инициируем поток загрузки. Код: {code}'.format(code=response.status_code))
					data = response.json()
					return data
				else:
					print(u'Ошибка в ответе сервера, код: %i, текст: %s' % (
						response.status_code, response.text))                    
			except Exception as e:
				u'Cервер не ответил в заданное время: {} секунд'.format(self.timeout)
		
		raise Exception(
			u'После {} запросов источник {} не вернул никаких данных'.format(n_tries, params['url']))

		

		

	