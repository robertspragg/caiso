# Script that pulls the Daily Renewable's Watch (with hourly information) from the following page
# example URL: http://content.caiso.com/green/renewrpt/20190205_DailyRenewablesWatch.txt
from bs4 import BeautifulSoup
import pandas as pd
import re
import requests
import time

def pull_txt():
	## USER-SPECIFIED SEARCH RANGE 
	year =  '2018'
	start_day = 1
	start_month = 1
	end_month   = 12
	####################
	
	Hourly_Renewable_Breakdown = pd.DataFrame(columns =['placeholder'])
	Hourly_GenBy_Resource      = pd.DataFrame(columns =['placeholder'])

	# put this in a for loop
	for month in range(start_month, end_month + 1):
		print('On month {}...'.format(month))
		# figure out how many days are in each month
		p = pd.Period(year + "-" + "{:02d}".format(month) + "-01")
		end_day = p.daysinmonth
		print(end_day)
		for day in range(start_day, end_day + 1):
			
			url = 'http://content.caiso.com/green/renewrpt/' + year + "{:02d}".format(month) + "{:02d}".format(day) + '_DailyRenewablesWatch.txt'
			print(url)
			# Request Data
			resp = requests.get(url)
			# make response data pretty
			soup = BeautifulSoup(resp.content, 'lxml')
			#print('test:', soup)

			text = soup.get_text()
			#print('text \n', text)
			lines = text.splitlines()
			#print('lines \n', lines)
			part1 = []
			part2 = []
			count = 0
			for line in lines:
				# Find header lines, do not add them to dataframe 
				if re.search('\(', line): 
					#print(line)
					count += 1
					if count == 1: # first part of file for a given day
						date = line.split('\t')[0]
						#print('date entry: ', date)
					#headers.append(line)
					continue

				# Find empty lines (all whitespace characters), do not add them to dataframe
				if line.isspace():
					#print('whitespace line only')
					#print(line)
					continue

				# split line by tabs 
				parts = line.split('\t')
				for i in range(len(parts)):
					# remove leading and trailing whitespace
					parts[i] = parts[i].strip()

				# Add date entry to list
				parts.insert(0, date)
				# Remove empty entries in list, append to appropriate list
				if count == 1:
					part1.append([x for x in parts if x])
				elif count == 2:
					part2.append([x for x in parts if x])
				else:
					print('ERROR')
				#print(parts)
			
			df1 = pd.DataFrame(part1[1:], columns = part1[0])
			df2 = pd.DataFrame(part2[1:], columns = part2[0])
			# Rename first column of dataframes
			df1.columns = ['Date' if '/' in x else x for x in df1.columns]
			df2.columns = ['Date' if '/' in x else x for x in df2.columns]
			#print('data frame1 \n', df1)
			#print('\n data frame 2 \n ', df2)


			# Concatenate dataframes for multiple days
			if Hourly_Renewable_Breakdown.empty:
				Hourly_Renewable_Breakdown = df1
				Hourly_GenBy_Resource      = df2
			else:
				Hourly_Renewable_Breakdown = [Hourly_Renewable_Breakdown, df1]
				Hourly_GenBy_Resource      = [Hourly_GenBy_Resource, df2]
				Hourly_Renewable_Breakdown = pd.concat(Hourly_Renewable_Breakdown)
				Hourly_GenBy_Resource = pd.concat(Hourly_GenBy_Resource)

			# Ten second pause (so we don't get blocked)
			time.sleep(5)
	
	# Reset DataFrame index, delete index column
	Hourly_Renewable_Breakdown = Hourly_Renewable_Breakdown.reset_index(drop=True)
	#Hourly_Renewable_Breakdown.drop('index', 1)
	Hourly_GenBy_Resource = Hourly_GenBy_Resource.reset_index(drop=True)
	#Hourly_GenBy_Resource.drop('index', 1)
	#print(Hourly_GenBy_Resource)
	# convert to CSV
	Hourly_Renewable_Breakdown.to_csv('Renewable_Breakdown_2018.csv')
	Hourly_GenBy_Resource.to_csv('GenByResource_2018.csv')

pull_txt()

