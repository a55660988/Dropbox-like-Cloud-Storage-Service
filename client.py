import rpyc
import hashlib
import os
import sys
from pathlib import Path
from blockstore import BlockStore
from metastore import *
import pingparsing
from multiprocessing.pool import ThreadPool

"""
A client is a program that interacts with SurfStore. It is used to create,
modify, read, and delete files.  Your client will call the various file
modification/creation/deletion RPC calls.  We will be testing your service with
our own client, and your client with instrumented versions of our service.
"""

class SurfStoreClient():
	"""
	Initialize the client and set up connections to the block stores and
	metadata store using the config file
	"""
	def __init__(self, config):
		self.config_dict = self.parseConfig(config)
		self.algorithm = self.config_dict['algo']
		self.eprint("algorithm decided: ", self.algorithm)
		self.conn_metaStore = rpyc.connect(self.config_dict["metadata"]["host"], self.config_dict["metadata"]["port"])
		self.eprint("connected to metaStore: ", self.config_dict["metadata"]["host"] + ":" + self.config_dict["metadata"]["port"])
		self.conn_blockStore = []
		for i in range(0, int(self.config_dict["B"])):
			self.conn_blockStore.append(rpyc.connect(self.config_dict["block" + str(i)]["host"], self.config_dict["block" + str(i)]["port"]))
			self.eprint("connected to blockStore" + str(i) + ": ", self.config_dict["block" + str(i)]["host"] + ":" + self.config_dict["block" + str(i)]["port"])


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

	"""
	upload(filepath)) : Reads the local file, creates a set of
	hashed blocks and uploads them onto the MetadataStore
	(and potentially the BlockStore if they were not already present there).
	"""
	def upload(self, filepath):
		# check local file exist
		# call exposed_read_file(filename): CL check file with metaData and get ver, hl
		# split file into block and blockHash
		# call exposed_modify_file(filename, version, hashlist) to metaData to get missingBlockHashList
		# 	metaData check all block status in blockHashList with BS
		# 	BS response missing block to metaData, metaData create missingBlockHashList
		# 	metaData response missingBlockHashList to CL
		# start uploading, call exposed_store_block(h, block)
		# When finishing upload, call exposed_modify_file to check with metaData and get response OK

		UH = UploadHelper(self.conn_metaStore, self.conn_blockStore, self.config_dict)
		# check local file exist
		filepath, checkExist = UH.checkFileExist(filepath)
		if checkExist:
			self.eprint("Local file exist")
			filename = UH.filepathGetFilename(filepath)
			# for missingBlockHashList not empty, upload again
			response = ""
			while response != "OK":
				# call exposed_read_file(filename): CL check file with metaData and get fileVer, fileHashList
				fileVer, fileHashList = self.conn_metaStore.root.read_file(filename)
				self.eprint("filever: " + str(fileVer) + " fileHashlist: " + str(fileHashList))
				# split file into block and blockHash
				blockHashList, blockList = UH.splitFileToBlockAndHash(filepath)
				self.eprint("local slipt to blocklist: ", blockHashList)
				# call exposed_modify_file(filename, version, hashlist) to metaData to get missingBlockHashList
				self.eprint("call ModifyFile to get missingBlockHashList")
				fileVer = fileVer + 1
				missingBlockHashList = self.conn_metaStore.root.modify_file(filename, fileVer, blockHashList)

				if missingBlockHashList == "OK":
					# no need to upload
					self.eprint("After checking missingBlockHashList, no missing, no need to upload")
					response = "OK"
					print("OK")

				elif missingBlockHashList == "NOT ALLOW":
					self.eprint("NOT ALLOW UPLOAD due to file version smaller than server")
					break

				else:
					# start uploading, call exposed_store_block(h, block)
					self.eprint("Get missingBlockHashList (first)")
					self.eprint("missingBlockHashList (first): ", missingBlockHashList)
					UH.startUpload(blockList, blockHashList, missingBlockHashList)

					# When finishing upload, check file
					self.eprint("Finish upload, check uploaded file")
					response = UH.checkUpload(filename, fileVer, blockHashList)
					if response == "OK":
						print(response)
					else:
						self.eprint("check uploaded file ERROR, need to get newest ver and upload again")

		else:
			self.eprint("Local file not exist")
			print("Not Found")


	"""
	delete(filename) : Signals the MetadataStore to delete a file.
	"""
	def delete(self, filename):
		fileVer, fileHashList = self.conn_metaStore.root.read_file(filename)
		if not fileHashList and fileVer == 0:
			print("Not Found")
		elif fileVer > 0 and not fileHashList:
			print("Not Found")
		else:
			self.conn_metaStore.root.delete_file(filename, fileVer+1)
			newFileVer, newFileHashList = self.conn_metaStore.root.read_file(filename)
			if newFileVer == fileVer + 1 and not newFileHashList:
				print("OK")
			else:
				self.eprint("delete fail")

	"""
		download(filename, dst) : Downloads a file (f) from SurfStore and saves
		it to (dst) folder. Ensures not to download unnecessary blocks.
	"""
	def download(self, filename, location):
		UH = UploadHelper(self.conn_metaStore, self.conn_blockStore, self.config_dict)
	# check if filename exists in location
		if location == "":
			file = Path(filename)
		else:
			file = Path("/".join([location, filename]))
		self.eprint("searched file path: ", file)

	# ask metadata for hashlist
		self.eprint("finished checking if the directory is avaliable")
		# ver, hashList = self.conn_metaStore.root.read_file(filename)
		self.eprint("file name: ", filename)

		fileVer, hashList = self.conn_metaStore.root.read_file(filename)
		if file.is_file(): #file exists locally
			blockHashList, blockList = UH.splitFileToBlockAndHash(file)
		else:
			blockHashList = []

	# getBlock() from blockstore
		if hashList:
			blocks = []
			for h in hashList:
				if h in blockHashList:
					self.eprint("block already exist ", blockHashList.index(h))
					blocks.append(blockList[blockHashList.index(h)])
				else:
					blocks.append(self.conn_blockStore[self.findServer(h)].root.get_block(h))
		# merge blocks to form file & write out file
			if location != "":
				fname = location + "/"
			else:
				fname = ""
			fout = open(file, 'wb')
			for block in blocks:
				fout.write(block)
			fout.close()
			print("OK")
		else:
			print("Not Found")

	def findServer(self, h):
		if self.algorithm == 0:
			return self.findServerWithHash(h)
		elif self.algorithm == 1:
			return self.findServerNTC()

	def findServerWithHash(self, h):
		return int(h, 16) % int(self.config_dict["B"])

	def findServerNTC(self):
		return self.getMinIp()


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
	    print(multiple_result)
	    return multiple_result.index(min(multiple_result))
	"""
	 Use eprint to print debug messages to stderr
	 E.g -
	 self.eprint("This is a debug message")
	"""
	def eprint(*args, **kwargs):
		print(*args, file=sys.stderr, **kwargs)


class UploadHelper():

	def __init__(self, conn_metaStore, conn_blockStore, config_dict):
		self.config_dict = config_dict
		self.algorithm = self.config_dict["algo"]
		self.eprint("algorithm picked parsed in uploadhelper")
		self.conn_metaStore = conn_metaStore
		self.conn_blockStore = conn_blockStore

	def checkFileExist(self, filepath):
		# self.eprint("File path = ", filepath)
		# check filepath file exist or not
		if os.path.isfile(filepath):
			return filepath, True
		else:
			return filepath, False

	def filepathGetFilename(self, filepath):
		if "/" in filepath:
			filepathList = filepath.split("/")
			filename = filepathList[-1]
		else:
			filename = filepath
		return filename

	def splitFileToBlockAndHash(self, filepath):
		self.eprint("At client, split local file into block")
		blockList = []
		blockHashList = []
		fp = open(filepath, "rb")
		# FIX: change back to 4096
		# block = fp.read(3)
		block = fp.read(4096)
		while block:
			blockList.append(block)
			blockHashList.append(hashlib.sha256(block).hexdigest())
			block = fp.read(4096)
			# blockHashList.append("@@" + str(block) + "@@")
			# block = fp.read(3)
		# self.eprint("blockList: ", blockList)
		# self.eprint("blockHashList: ", blockHashList)
		# self.eprint("At client, split local file into block DONE")
		return blockHashList, blockList

	def startUpload(self, blockList, blockHashList, missingBlockHashList):
		# all missing, upload all
		if len(missingBlockHashList) == len(blockList):
			self.eprint("Start upload missing block (file total change)")
			for h, b in zip(blockHashList, blockList):
				# to avoid other client has finish uploaded file, same block upload again
				# thus, when start upload each block, check again whether hashblock exist in server
				# if exist, no need to upload the same block again
				if self.conn_blockStore[self.findServer(h)].root.exposed_has_block(h):
					self.eprint("Block exists in server, no need to upload hashblock: ", h)
					continue
				# if not, upload
				else:
					self.eprint("hashblock not exists in server, need to upload block and hashblock: ", h)
					self.conn_blockStore[self.findServer(h)].root.exposed_store_block(h, b)
		# partial missing, upload missing block
		else:
			self.eprint("Start upload missing block (file partial change)")
			for missingBlockHashListElement in missingBlockHashList:
				self.conn_blockStore.root.exposed_store_block(blockHashList[blockHashList.index(missingBlockHashListElement)], missingBlockHashListElement)

	def checkUpload(self, filename, fileVer, blockHashList):
		# call exposed_modify_file to check with metaData and get response OK
		missingBlockHashList = self.conn_metaStore.root.exposed_modify_file(filename, fileVer, blockHashList)
		self.eprint("missingBlockHashList (second): ", missingBlockHashList)
		if missingBlockHashList == "OK":
			return "OK"
		return "need to upload again"

	def eprint(*args, **kwargs):
		print(*args, file=sys.stderr, **kwargs)

	def findServer(self, h):
		if self.algorithm == 0:
			return self.findServerWithHash(h)
		elif self.algorithm == 1:
			return self.findServerNTC()

	def findServerWithHash(self, h):
		return int(h, 16) % int(self.config_dict["B"])

	def findServerNTC(self):
		return self.getMinIp()


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
	    self.eprint(multiple_result)
	    return multiple_result.index(min(multiple_result))

if __name__ == '__main__':
	client = SurfStoreClient(sys.argv[1])
	operation = sys.argv[2]
	if operation == 'upload':
		client.upload(sys.argv[3])
	elif operation == 'download':
		client.download(sys.argv[3], sys.argv[4])
	elif operation == 'delete':
		client.delete(sys.argv[3])
	else:
		print("Invalid operation")
