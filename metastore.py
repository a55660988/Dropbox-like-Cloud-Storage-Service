import rpyc
import sys
import time
import threading
import pingparsing
from multiprocessing.pool import ThreadPool

'''
A sample ErrorResponse class. Use this to respond to client requests when the request has any of the following issues -
1. The file being modified has missing blocks in the block store.
2. The file being read/deleted does not exist.
3. The request for modifying/deleting a file has the wrong file version.

You can use this class as it is or come up with your own implementation.
'''
class ErrorResponse(Exception):
	def __init__(self, message):
		super(ErrorResponse, self).__init__(message)
		self.error = message

	def missing_blocks(self, hashlist):
		self.error_type = 1
		self.missing_blocks = hashlist

	def wrong_version_error(self, version):
		self.error_type = 2
		self.current_version = version

	def file_not_found(self):
		self.error_type = 3

	def file_already_exist(self):
		self.error_type = 4



'''
The MetadataStore RPC server class.

The MetadataStore process maintains the mapping of filenames to hashlists. All
metadata is stored in memory, and no database systems or files will be used to
maintain the data.
'''
class MetadataStore(rpyc.Service):
	# fileHashListMap = {}
	# deleteFiles = []
	"""
		Initialize the class using the config file provided and also initialize
		any datastructures you may need.
	"""
	def __init__(self, config):
		self.config_dict = self.parseConfig(config)
		self.algorithm = self.config_dict['algo']
		self.eprint("algorithm decided in metastore: ", self.algorithm)
		self.conn_blockStore = []
		for i in range(0, int(self.config_dict["B"])):
			self.conn_blockStore.append(rpyc.connect(self.config_dict["block" + str(i)]["host"], self.config_dict["block" + str(i)]["port"]))
			self.eprint("connected to blockStore" + str(i) + ": ", self.config_dict["block" + str(i)]["host"] + ":" + self.config_dict["block" + str(i)]["port"])
		# fileHashListMap = {"filename": {fileVer: 0, hashList: (("HashValue1", self.config_dict["blockX"]["host"]), ("HashValue2", self.config_dict["blockX"]["host"]))}}
		self.fileHashListMap = {}
		self.deleteFiles = []
		self.read_lock = threading.Lock()
		# self.eprint("fileHashListMap: ", self.fileHashListMap)



	"""
		ModifyFile(f,v,hl): Modifies file f so that it now contains the
		contents refered to by the hashlist hl.  The version provided, v, must
		be exactly one larger than the current version that the MetadataStore
		maintains.

		As per rpyc syntax, adding the prefix 'exposed_' will expose this
		method as an RPC call
	"""
	def exposed_modify_file(self, filename, version, hashlist):
		self.read_lock.acquire()
		# check version first
		if filename in self.fileHashListMap:
			if version != self.fileHashListMap[filename]["fileVer"] + 1:
				self.eprint("client try upload file, but version smaller than server")
				self.read_lock.release()
				return "NOT ALLOW"

		missingBlockHashList = []
		for h in hashlist:
			foundServer = h[1]
			# if not foundServer:
			# 	foundServer = self.findServer(h[0])
			self.eprint("found server type: ", foundServer)
			self.eprint("found server: ", foundServer)
			if foundServer != "" and self.conn_blockStore[foundServer].root.has_block(h[0]):
				self.eprint("blockstore has block: ", h[0])
			else:
				missingBlockHashList.append(h[0])
		self.eprint("length of missing block : ", len(missingBlockHashList))
		# no missing block
		if len(missingBlockHashList) == 0:
			self.eprint("No missingBlockHashList")
			# client has finished upload NEW file
			if filename not in self.fileHashListMap:
				self.fileHashListMap[filename] ={"fileVer": 1, "hashList": tuple(hashlist)}
				# update deletefiles list
				if filename in self.deleteFiles:
					self.deleteFiles.remove(filename)
				self.eprint(self.fileHashListMap[filename])
				self.eprint("=====Client has finished upload NEW file=====")
				self.eprint("INFO: filename: ", filename, self.fileHashListMap[filename])
				self.eprint("==========")
			# client has finished upload OVERWRITE file, add 1 to ver
			elif filename in self.fileHashListMap:
				self.fileHashListMap[filename]["fileVer"] = self.fileHashListMap[filename]["fileVer"] + 1
				self.fileHashListMap[filename]["hashList"] = tuple(hashlist)
				# update deletefiles list
				if filename in self.deleteFiles:
					self.deleteFiles.remove(filename)
				self.eprint("=====Client has finished upload and OVERWRITE file=====")
				self.eprint("INFO: filename: ", filename, self.fileHashListMap[filename])
				self.eprint("==========")
			self.read_lock.release()
			return "OK"
		else:
			# return missingBlockHashList which client needs to upload block to
			self.eprint("Find missingBlockHashList for file: ", filename, " return missingBlockHashList", missingBlockHashList)
			self.read_lock.release()
			return missingBlockHashList

	"""
		DeleteFile(f,v): Deletes file f. Like ModifyFile(), the provided
		version number v must be one bigger than the most up-date-date version.

		As per rpyc syntax, adding the prefix 'exposed_' will expose this
		method as an RPC call
		"""
	def exposed_delete_file(self, filename, version):
		verNum = self.fileHashListMap[filename]["fileVer"]
		if version <= verNum:
			self.eprint("Version not allowed")
		else:
			self.read_lock.acquire()
			self.eprint("delete lock acquired")
			self.fileHashListMap[filename]["fileVer"] = version
			self.fileHashListMap[filename]["hashList"] = ()
			self.deleteFiles.append(filename)
			self.eprint(filename, "with updated version number: ", version, " is deleted")
			self.eprint(self.deleteFiles)
			self.read_lock.release()
			self.eprint("delete lock released")

	"""
		(v,hl) = ReadFile(f): Reads the file with filename f, returning the
		most up-to-date version number v, and the corresponding hashlist hl. If
		the file does not exist, v will be 0.

		As per rpyc syntax, adding the prefix 'exposed_' will expose this
		method as an RPC call
	"""
	def exposed_read_file(self, filename):
		## test for download
		#################################################
		### return 1, ["12344", "testtest", "test1"]  ###
		#################################################
		# print("!!!!!!!!!!=============hashList id: " + str(self.fileHashListMap))
		self.read_lock.acquire()
		if filename not in self.deleteFiles:
			self.eprint("not deleted file")
			if filename in self.fileHashListMap:
				self.eprint("not deleted ")

				fileVer = self.fileHashListMap[filename]["fileVer"]
				self.eprint("file version is: ", fileVer)
				# time.sleep(10)
				fileHashList = self.fileHashListMap[filename]["hashList"]
				# userinput = input('start')
				# self.eprint("userinput: ", userinput)
				# if userinput == "download":

				self.eprint("lock released")
				# self.eprint("file hash list is: ", fileHashList)
			else:
				self.eprint("can upload")
				fileVer = 0
				fileHashList = ()
		else:
			if filename in self.fileHashListMap:
				fileVer = self.fileHashListMap[filename]["fileVer"]
				fileHashList = ()
			else:
				fileVer = 0
				fileHashList = ()
		# file not exist
		self.read_lock.release()
		return fileVer, fileHashList

	def findServer(self, h):
		self.eprint("find server called with algo: ", self.algorithm)
		if self.algorithm == 0:
			self.eprint("algorithm hash ")
			return self.findServerWithHash(h)
		elif self.algorithm == 1:
			self.eprint("algorithm nearest to client")
			return self.findServerNTC()
		self.eprint("didn't go in both ")

	def findServerWithHash(self, h):
		self.eprint("find server with hashvalue ", h)
		return int(h, 16) % int(self.config_dict["B"])

	def findServerNTC(self):
		minipindex = self.getMinIp()
		self.eprint(minipindex)
		return minipindex

	def getRtt(self, ip):
	    transmitter = pingparsing.PingTransmitter()
	    transmitter.destination_host = ip
	    result = transmitter.ping()
	    pingparser = pingparsing.PingParsing()
	    result_dict = pingparser.parse(result).as_dict()
	    return result_dict['rtt_avg']

	def getMinIp(self):
	    pool = ThreadPool(processes = 4)
	    ips = [self.config_dict["block0"]["host"], self.config_dict["block1"]["host"], self.config_dict["block2"]["host"], self.config_dict["block3"]["host"]]
	    multiple_result = [pool.apply_async(self.getRtt, (ip,)) for ip in ips]
	    multiple_result = [result.get() for result in multiple_result]
	    self.eprint("average rtt to datacenters: ", multiple_result)
	    return multiple_result.index(min(multiple_result))

	def eprint(*args, **kwargs):
		print(*args, file=sys.stderr, **kwargs)

	def parseConfig(self, config):
		dict = {}
		with open(config, 'r') as file:
			lines = [line.strip('\n') for line in file]
			# get number of block
			temp = lines[0].split(":")
			dict[temp[0]] = temp[1]
			alg = lines[1].split(":") # algorithm for block location algorithm
			dict[alg[0]] = int(alg[1])
			for line in lines[2:]:
				temp = line.split(":")
				dict[temp[0]] = {"host": temp[1].strip(), "port": temp[2].strip()}
		return dict

if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	server = ThreadPoolServer(MetadataStore(sys.argv[1]), port = 6000)
	print("Start metastore...")
	server.start()
