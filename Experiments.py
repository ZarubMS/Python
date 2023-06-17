import json
import csv
import os
import sys
import argparse
import time
from datetime import datetime
import requests


path = "config_test.json"

def read_config(path):
	with open(path, 'r', encoding='utf-8') as f:
		return json.load(f)
#получаем данные из конфига, имя конфига из командной строки
def get_config_data(param):
	config = read_config(get_arg("config"))
	return config[param]

def createParser():
	parser = argparse.ArgumentParser()
	parser.add_argument("-d", "--datestart", default="2023-06-16")
	parser.add_argument("-e", "--dateend", default="2023-06-16")
	parser.add_argument("-c", "--config", default="config.json")
	parser.add_argument("-f", "--file_name", default="Experiments")
	return parser

def get_arg(name):
	parser = createParser()
	namespace =  parser.parse_args(sys.argv[1:])
	arguments = namespace.__dict__
	return arguments[name]

def exp_delay(retry_counter, start_delay=20):
	"""
	Функция вычисления экспоненциальной задержки
	:return: int
	"""
	if retry_counter == 0:
		sleep_time_source = 0
	else:
		sleep_time_source = int(start_delay * 3 ** retry_counter)
	time.sleep(sleep_time_source)

def add_date(params):
	params["date1"] = get_arg("datestart")
	params["date2"] = get_arg("dateend")

def load_request():
	#получаем имя результирующего файла из командной строки
	file_name = get_arg("file_name")
	print(file_name)
	#получаем данные из config_test
	success_codes = [200]
	url = get_config_data("url")
	headers_param = get_config_data("headers_params")
	request_params = get_config_data("request_params")
	#добавляем дату в параметры
	add_date(request_params)
	n_tries = 3
	retry_counter = 0
	while retry_counter < n_tries:
		exp_delay(retry_counter)
		retry_counter += 1
		print("Попытка запроса")
		response = requests.get(url, headers = headers_param, params = request_params)
		print(response.url)
		if response.status_code in success_codes:
			print("Успешный запрос, начинаем загрузку")
			data = response.json()
			retry_counter = 3
		else:
			print("Неудачный запрос")
			print(response.status_code)	
			break
	return data

#парсим JSON
def pars_json(data):
	print("Парсинг:")
	a = []
	for x in data:
			name = x["dimensions"]
			for k in name:
				temp = k
			temp["metrics"] = x["metrics"][0]
			a.append(temp)
	print("Парсинг завершен")		
	return a

#Запись в файл
def write_csv(path, columns_names, data):
	with open (path, 'w', newline='') as f:
		wr = csv.DictWriter(f, fieldnames = columns_names)
		wr.writeheader()
		wr.writerows(pars_json(data))
	print("Запись завершена в ", path)

def load():
	load_data = load_request()
	data = load_data["data"]
	write_csv(get_config_data("result_path")+get_arg("file_name"), get_config_data("json_headers"), data)
	
	
	

load()
