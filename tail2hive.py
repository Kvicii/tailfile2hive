#!/usr/bin/env python
# -*- coding:UTF-8 -*-
from __future__ import print_function
import datetime
import time as tm
import json
import glob
from user_agents import parse
import sys 
import re
import urllib
import os,sys

# Assumes kafka module is in a folder parallel to the folder this script exists in.
# Once we can install pykafka as a separate module, this will be removed.
sys.path.append(os.path.join(os.path.split(sys.argv[0])[0],'..'))
from pykafka import KafkaClient
from optparse import OptionParser
import subprocess
import atexit


should_stop = False
pending_messages = []



def send_to_kafka(line):
	client = KafkaClient(hosts="10.10.10.176:9092")
	topic = client.topics['dreamtvlog']
	with topic.get_sync_producer() as producer:
		producer.produce(line)


def log_lines_generator(logfile,delay_between_iterations=None):
	global should_stop
	cmd = ['tail','-n','10000000000','-F']
	if delay_between_iterations is not None:
		cmd.append('-s')
		cmd.append(delay_between_iterations)
	cmd.append(logfile)
	print ('cmd',cmd)
	process = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=None)
	while not should_stop:
		line = process.stdout.readline().strip()
		yield line
	
def main():
	reload(sys)
	sys.setdefaultencoding('utf-8')
	parser = OptionParser(usage="""
	%prog -l <log-file>  [other-options]


	set -l to the log file to be tailed. The log tailing supports log rotation.

	Simple batching is supported (use -b to choose the batch size, default is 10).

	Advanced: If needed, use -d <delay> in order to control the tail delay - Unneeded in almost all cases.

""")
	parser.add_option("-l","--log-file",dest="logfile",default=None,
					help="REQUIRED: Log file to tail")
	parser.add_option("-b","--batch-size",dest="batch_size",default="10",
					help="Size of message batches")
	parser.add_option("-d","--delay",dest="delay",default=None,
					help="tail delay between iterations")
	
	(options,args) = parser.parse_args()
	processline=0
	processnums=1000
	dict={}
	os.system('rm -rf  /data/zsl/tail2hive/newaccesswrong.txt')
	os.system('rm -rf  /data/zsl/tail2hive/newaccessmaphive.txt')
	

	file=open('/data/zsl/tail2hive/newaccesswrong.txt','a')
	if options.logfile is None:
		parser.print_help()
		sys.exit(1)
	if options.batch_size is not None:
		processnums=options.batch_size

	try:
		maphivefile=open('/data/zsl/tail2hive/newaccessmaphive.txt','w+')
		for url in log_lines_generator(options.logfile):
			processline=processline+1
			#send_to_kafka(url)
			print('processnums',processnums)
			print ('linenum:'+str(processline)+'\t'+'message:',url)
			line =urllib.unquote(url)+'\n'
			dict.clear()
			printline=''
			if not line.startswith("NOTICE"):
				if not line[0].isdigit():
					file.write('ip:'+line)
					continue
				line_item = line.strip().split('\t')#
			   # print (line_item,len(line_item))
				if len(line_item)!=10:
					
					file.write('time_local:'+line)
					continue					  
				
				_time_local = line_item[1].strip().split('[')[-1].strip(']').split(' ')[0]#
				if len(_time_local)<3:
					
					file.write('time_local:'+line)
					continue
				if not _time_local[2]=='/':
					
					file.write('time_local:'+line)
					continue
				timeArray = tm.strptime(_time_local, "%d/%b/%Y:%H:%M:%S")
				otherStyleTime = tm.strftime("%Y%m%d%H%M%S", timeArray)
				processtime=datetime.datetime.strptime(otherStyleTime, "%Y%m%d%H%M%S") 
				nowtime=datetime.datetime.now()
				if((nowtime-processtime).seconds/60<=3):
					processnums=500
				dict["_time"]=otherStyleTime
				printline=printline+otherStyleTime+'\t'
				
				ip = line_item[0]#
				dict["ip"]=ip
				printline=printline+ip+'\t'
				dict["_method"]=line_item[2].split('/')[0]
				printline=printline+line_item[2].split('/')[0]+'\t'					  
			   # splits=line_item[2].split('/')[1].split(' ')
			   # print('splits',splits)
				_request=line_item[2][line_item[2].find('/')+1:line_item[2].find('HTTP/')-1]
				# if len(splits)<2:
					# 
					# file.write('MethodRequestHttpversion:'+line)
					# continue
				# elif(len(splits)==2) :
					# _request=splits[0]
				# elif(len(splits)>2):
					# _request="".join(splits[0:-2])
				dict["_request"]=_request
				printline=printline+_request+'\t'
				dict["_status"]=line_item[3]
				printline=printline+line_item[3]+'\t'
				dict["_body_bytes_sent"]=line_item[4]
				printline=printline+line_item[4]+'\t'
				dict["_http_referer"]=line_item[5]
				printline=printline+line_item[5]+'\t'
				_user_agent = line_item[6].strip()
				printline=printline+line_item[6].strip()+'\t'
				if not "_user_agent" in dict.keys():
					dict["_user_agent"]=_user_agent
				user_agent = parse(_user_agent)
				dict["browser"]=user_agent.browser
				printline=printline+str(user_agent.browser)+'\t'
				dict["browerfamily"]=user_agent.browser.family
				printline=printline+str(user_agent.browser.family)+'\t'
				dict["browerversion"]=user_agent.browser.version_string
				printline=printline+str(user_agent.browser.version_string)+'\t'
				dict["os"]=user_agent.os
				printline=printline+str(user_agent.os)+'\t'
				dict["osfamily"]=user_agent.os.family
				printline=printline+str(user_agent.os.family)+'\t'
				dict["osversion"]=user_agent.os.version_string
				printline=printline+str(user_agent.os.version_string)+'\t'
				dict["device"]=user_agent.device
				printline=printline+str(user_agent.device)+'\t'
				dict["devicefamily"]=user_agent.device.family
				printline=printline+str(user_agent.device.family)+'\t'
				dict["devicebrand"]=user_agent.device.brand
				printline=printline+str(user_agent.device.brand)+'\t'
				dict["devicemodel"]=user_agent.device.model
				printline=printline+str(user_agent.device.model)+'\t'
				dict['request_time'] = line_item[7].strip()
				printline=printline+str(line_item[7].strip())+'\t'				   
				dict['request_body'] = line_item[8].strip()
				printline=printline+str(line_item[8].strip())+'\t'
				dict['http_x_forwarded_for'] = line_item[9].strip()
				printline=printline+str(line_item[9].strip())+'\t'
			  #  print (dict)

				if  _request.find("?")!=-1:
					dict['act'] = _request.split('?')[0]
					printline=printline+_request.split('?')[0]+'\t'
					splist = _request.split('?')[1].split('&')
					for i in splist:
						if '=' not in i:
							continue
						if len(i.split('='))!=2:
							
							file.write('line:'+line)
							file.write('not splited:'+i)
							continue
						k,v=i.split('=')
						if "time"==k:
							if not v.isdigit():
								file.write('time字段不是数字:'+v)
								file.write('line:'+line)
								continue
							timestamp=0
							if(len(v)==13):
								timestamp=int(int(v)/1000)   
							else:
								timestamp=int(int(v))
							try :
								x = tm.localtime(timestamp)
								v=tm.strftime('%Y-%m-%d %H:%M:%S',x)
							except  Exception:
								pass
						elif  "extends"==k:
							try :
								splits2==v.strip('{').strip('}').split(',')							 
								for l in splits2:
									k2,v2 = l.split(':')
									dict[k2]=v2
									printline=printline+str(k2)+':'+str(v2)+','
							except  Exception:
								pass
						dict[k]=v
						if(splist.index(i)+1==len(splist)):
							printline=printline+str(k)+':'+str(v)
						else:
							printline=printline+str(k)+':'+str(v)+','							
					   # print ("%s=%s\n"%(k,v))
				#print ("-----------dict:%s"%(dict))
				#print('printline:',printline)
				#maphivefile.writelines('1122334455')
				maphivefile.writelines(printline+'\t'+str(processline)+'\n')
				maphivefile.flush()
				if(processline%int(processnums)==0): 
					os.system('sh /data/zsl/tail2hive/realtime2hive.sh')
					maphivefile.seek(0)
					maphivefile.truncate(0)
					
			
	except KeyboardInterrupt,e:
		pass

if __name__ == '__main__':
	main()



