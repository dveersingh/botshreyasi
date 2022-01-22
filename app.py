import uuid
import os , shutil
import time

import mysql.connector
import config as cfg

processing = "Processing/"
queue = "queue/"
processed = "process/"


def create_file():
	file_name = str(uuid.uuid4().hex)
	f = open(processing+file_name+".txt","w")
	f.close()
	print("file created")
	sql = "INSERT INTO fileHistory (filename, moved)\
	VALUES (%s, %s)"
	val = (file_name, "0")

	cursorObject.execute(sql, val)
	dataBase.commit()
	
	
def move_to_queue():
	source = processing
	destination = queue
	allfiles = os.listdir(source)
	print(allfiles)
	for f in allfiles:
		print("moving_to_queue")
		shutil.move(source + f, destination + f)
		
		

def move_to_process():
	source = queue
	destination = processed
	allfiles = os.listdir(source)
	#print(allfiles)
	print("moving from queue to processed")
	for f in allfiles:
		query = "UPDATE fileHistory SET moved = 1 WHERE filename ='"+f[:-4]+"'"
		#print(query)
		cursorObject.execute(query)
		dataBase.commit()
		shutil.move(source + f, destination + f)
	
	
	
if __name__ == "__main__":
		
	time_count = 0	

	dataBase = mysql.connector.connect(
									host =cfg.mysql["host"],
									user =cfg.mysql["user"],
									passwd =cfg.mysql["passwd"],
									database = cfg.mysql["database"]
								)

	cursorObject = dataBase.cursor()

	while True:
		create_file() # creating files
		time.sleep(1)
		if time_count % 5 == 0 and time_count != 0:
			print("moving filews")
			if not os.listdir("queue/"):
				move_to_queue() # move to queue if empty
			move_to_process() #move to process from queue
		time_count = time_count+1
		#print(time_count)
		
	dataBase.close()