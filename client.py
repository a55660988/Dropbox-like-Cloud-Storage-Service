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
		self.config = config
		self.metaData = MetadataStore(self.config)
		self.blockStore = BlockStore()
		# pass

	"""
	upload(filepath) : Reads the local file, creates a set of
	hashed blocks and uploads them onto the MetadataStore
	(and potentially the BlockStore if they were not already present there).
	"""
	def upload(self, filepath):
		# check local file exist
		# call exposed_read_file(filename): CL check file with metaData and get ver, hl
		# split file into block and blockHash
		# call exposed_modify_file(filename, version, hashlist) to metaData to get missingBlockList
		# 	metaData check all block status in blockHashList with BS
		# 	BS response missing block to metaData, metaData create missingBlockList
		# 	metaData response missingBlockList to CL
		# start uploading, call exposed_store_block(h, block)
		# When finishing upload, call exposed_modify_file to check with metaData and get response OK

		UH = UploadHelper()
		# check local file exist
		filepath, checkExist = UH.checkFileExist(filepath)
		if checkExist:
			self.eprint("Local file exist")

			# call exposed_read_file(filename): CL check file with metaData and get fileVer, fileHashList
			fileVer, fileHashList = self.metaData.exposed_read_file(filepath)

			# split file into block and blockHash
			blockList, blockHashList = UH.splitFileToBlockAndHash(filepath)

			# call exposed_modify_file(filename, version, hashlist) to metaData to get missingBlockList
			self.eprint("call ModifyFile to get missingBlockList")
			fileVer = fileVer + 1
			missingBlockList = self.metaData.exposed_modify_file(filepath, fileVer, blockHashList)
			self.eprint("missingBlockList1: ", missingBlockList)

			# start uploading, call exposed_store_block(h, block)
			self.eprint("Get missingBlockList, start upload")
			for h, b in zip(blockHashList, blockList):
				self.blockStore.exposed_store_block(h, b)

			# When finishing upload, call exposed_modify_file to check with metaData and get response OK
			self.eprint("Finish upload, checking uploaded file")
			missingBlockList = self.metaData.exposed_modify_file(filepath, fileVer, blockHashList)
			self.eprint("missingBlockList2: ", missingBlockList)
			if len(missingBlockList) == 0:
				print("OK")
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
		pass
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
		ver, hashList = self.metaData.exposed_read_file(filename)
	# getBlock() from blockstore
		blocks = []
		for h in hashList:
			blocks.append(self.blockStore.exposed_get_block(h))
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
		block = fp.read(4096)
		while block:
			blockList.append(block)
			blockHashList.append(hashlib.sha256(block).hexdigest())
			block = fp.read(4096)
		# self.eprint("blockList: ", blockList)
		# self.eprint("blockHashList: ", blockHashList)
		# self.eprint("At client, split local file into block DONE")
		return blockHashList, blockList

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
