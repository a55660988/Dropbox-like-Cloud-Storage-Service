import rpyc
import hashlib
import os
import sys
from pathlib import Path
from blockstore import BlockStore
from metastore import *

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
		self.conn_metaStore = rpyc.connect("localhost", self.config_dict['metadata'])
		self.conn_blockStore = rpyc.connect("localhost", self.config_dict['block1'])
		print("connected to metaStore root ", str(self.conn_metaStore.root))
		print("connected to blockStore root ", str(self.conn_blockStore.root))
		# pass
	def parseConfig(self, config):
		# TODO: parse all config
		dict = {}
		with open(config, 'r') as file:
			lines = [line.strip('\n') for line in file]
			lines = lines[1:]
			for line in lines:
				temp = line.split(":")
				dict[temp[0]] = temp[2]
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

		UH = UploadHelper(self.conn_metaStore, self.conn_blockStore)
		# check local file exist
		filepath, checkExist = UH.checkFileExist(filepath)
		if checkExist:
			self.eprint("Local file exist")

			# call exposed_read_file(filename): CL check file with metaData and get fileVer, fileHashList
			fileVer, fileHashList = self.conn_metaStore.root.exposed_read_file(filepath)

			# split file into block and blockHash
			blockHashList, blockList = UH.splitFileToBlockAndHash(filepath)

			# call exposed_modify_file(filename, version, hashlist) to metaData to get missingBlockHashList
			self.eprint("call ModifyFile to get missingBlockHashList")
			fileVer = fileVer + 1
			missingBlockHashList = self.conn_metaStore.root.exposed_modify_file(filepath, fileVer, blockHashList)

			if missingBlockHashList == "OK":
				# no need to upload
				self.eprint("After checking missingBlockHashList, no missing, no need to upload")
				print("OK")

			elif missingBlockHashList == "NOT ALLOW":
				self.eprint("NOT ALLOW UPLOAD due to file version smaller than server")

			else:
				# start uploading, call exposed_store_block(h, block)
				self.eprint("Get missingBlockHashList (first)")
				self.eprint("missingBlockHashList (first): ", missingBlockHashList)
				UH.startUpload(blockList, blockHashList, missingBlockHashList)

				# When finishing upload, check file
				self.eprint("Finish upload, check uploaded file")
				# TODO: missingBlockHashList not empty, upload again
				response = UH.checkUpload(filepath, fileVer, blockHashList)
				print(response)

		else:
			self.eprint("Local file not exist")
			print("Not Found")


	"""
	delete(filename) : Signals the MetadataStore to delete a file.
	"""
	def delete(self, filename):
		pass

	"""
		download(filename, dst) : Downloads a file (f) from SurfStore and saves
		it to (dst) folder. Ensures not to download unnecessary blocks.
	"""
	def download(self, filename, location):
	# check if filename exists in location
		if location == "":
			file = Path(filename)
		else:
			file = Path("/".join([location, filename]))
		print("searched file path: ", file)

		if file.is_file():
			raise Exception("File already exists")
		else:
			print("not existed")
	# ask metadata for hashlist
		ver, hashList = self.conn_metaStore.root.read_file(filename)
	# getBlock() from blockstore
		blocks = []
		for h in hashList:
			blocks.append(self.conn_blockStore.root.get_block(h))
	# merge blocks to form file & write out file
		# blocks = [b'hello', b'cse 224', b'hw5']
		if location != "":
			fname = location + "/"
		else:
			fname = ""
		fout = open(file, 'wb')
		for block in blocks:
			fout.write(block)
		fout.close()

		''' singal user '''
		print("File " + str(filename) + " Downloaded successfully")

	"""
	 Use eprint to print debug messages to stderr
	 E.g -
	 self.eprint("This is a debug message")
	"""
	def eprint(*args, **kwargs):
		print(*args, file=sys.stderr, **kwargs)


class UploadHelper():

	def __init__(self, conn_metaStore, conn_blockStore):
		self.conn_metaStore = conn_metaStore
		self.conn_blockStore = conn_blockStore

	def checkFileExist(self, filepath):
		# self.eprint("File path = ", filepath)
		# check filepath file exist or not
		if os.path.isfile(filepath):
			return filepath, True
		else:
			return filepath, False

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
				self.conn_blockStore.root.exposed_store_block(h, b)
		# partial missing, upload missing block
		else:
			self.eprint("Start upload missing block (file partial change)")
			for missingBlockHashListElement in missingBlockHashList:
				self.conn_blockStore.root.exposed_store_block(blockHashList[blockHashList.index(missingBlockHashListElement)], missingBlockHashListElement)

	def checkUpload(self, filepath, fileVer, blockHashList):
		# call exposed_modify_file to check with metaData and get response OK
		missingBlockHashList = self.conn_metaStore.root.exposed_modify_file(filepath, fileVer, blockHashList)
		self.eprint("missingBlockHashList (second): ", missingBlockHashList)
		if missingBlockHashList == "OK":
			return "OK"
		return "need to upload again"

	def eprint(*args, **kwargs):
		print(*args, file=sys.stderr, **kwargs)



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
