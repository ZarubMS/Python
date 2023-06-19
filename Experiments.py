import sys
import os
import json
from datetime import datetime, timedelta
import csv
import requests
from Base import Base

path_file = os.path.realpath(__file__)


class Experiments(Base):
	
	def get_params_loader(self):
		headers = self.config["headers"]
		url = self.config["url"]
		request_params = self.config["request_params"]
		self.recalculate_load_period()
		self.timeout = self.config["timeout"]
		request_params["date1"] = self.date_from
		request_params["date2"] = self.date_to
		main_params = dict(url = url, headers = headers, request_params = request_params)
		
		return main_params

	#парсим JSON
	def parse_json(self, data):
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
	def write_csv(self, data):
		print(data)
		columns_names = self.config["json_headers"]
		path_csv = self.get_parent_path()+"/"+"DATA/"+self.file_name
		with open (path_csv, 'w', newline='') as f:
			wr = csv.DictWriter(f, fieldnames = columns_names)
			wr.writeheader()
			wr.writerows(data)
		print("Запись завершена в ", path_csv)

	def run(self):
		all_params = self.get_params_loader()
		print(all_params['url'])
		print(all_params['headers'])
		print(all_params['request_params'])
		
		load_data = self.load_request(all_params)
		data = load_data["data"]
		data = self.parse_json(data)
		self.write_csv(data)


		

if __name__ == '__main__':
    Main = Experiments(path_file)
    Main.run()
