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
		# call exposed_read_file(filename): CL check file with MDS and get ver, hl
		# split file into chunk and chunkHash
		# call exposed_modify_file(filename, version, hashlist) to MDS to get missingBlockList
		# 	MDS check all block status in chunkHashList with BS
		# 	BS response missing block to MDS, MDS create missingBlockList
		# 	MDS response missingBlockList to CL
		# start uploading, call exposed_store_block(h, block)
		# When finishing upload, call exposed_modify_file to check with MDS and get response OK

		UH = UploadHelper()
		# check local file exist
		filepath, checkExist = UH.checkFileExist(filepath)
		if checkExist:
			self.eprint("Local file exist")

			# call exposed_read_file(filename): CL check file with MDS and get ver, hl
			MDS = MetadataStore("config")
			ver, hl = MDS.exposed_read_file(filepath)

			# split file into chunk and chunkHash
			chunkList, chunkHashList = UH.splitFileToChunkAndHash(filepath)

			# call exposed_modify_file(filename, version, hashlist) to MDS to get missingBlockList
			self.eprint("call ModifyFile to get missingBlockList")
			ver = ver + 1
			status, missingBlockList = MDS.exposed_modify_file(filepath, ver, chunkHashList)

			# start uploading, call exposed_store_block(h, block)
			self.eprint("Get missingBlockList, start upload")
			BS = BlockStore()
			for c, ch in zip(chunkList, chunkHashList):
				BS.exposed_store_block(ch, c)

			# When finishing upload, call exposed_modify_file to check with MDS and get response OK
			self.eprint("Finish upload, checking uploaded file")
			status, missingBlockList = MDS.exposed_modify_file(filepath, ver, chunkHashList)
			self.eprint("MDS Response: ", status)
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
		# hashList = metaData.exposed_read_file(filename)
	# getBlock() from blockstore
		# blocks = []
		# for h in hashList:
			# blocks.append(blockStore.exposed_get_block())
	# merge blocks to form file & write out file
		blocks = [b'hello', b'cse 224', b'hw5']
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
		self.eprint("File path = ", filepath)
		# check filepath file exist or not
		if os.path.isfile(filepath):
			return filepath, True
		else:
			return filepath, False

	def splitFileToChunkAndHash(self, filepath):
		self.eprint("split file into block")
		chunkList = []
		chunkHashList = []
		fp = open(filepath, "rb")
		chunk = fp.read(4096)
		while chunk:
			chunkList.append(chunk)
			chunkHashList.append(hashlib.sha256(chunk).hexdigest())
			chunk = fp.read(4096)
		self.eprint("chunkList: ", chunkList)
		self.eprint("chunkHashList: ", chunkHashList)
		self.eprint("split file into block done")
		return chunkList, chunkHashList

	def eprint(*args, **kwargs):
		print(*args, file=sys.stderr, **kwargs)



if __name__ == '__main__':
	# client = SurfStoreClient()
	client = SurfStoreClient(sys.argv[1])
	client.download("test.txt","/Users/joy/Documents/cse 224/hw5")
	# operation = sys.argv[2]
	# if operation == 'upload':
	# 	client.upload(sys.argv[3])
	# elif operation == 'download':
	# 	client.download(sys.argv[3], sys.argv[4])
	# elif operation == 'delete':
	# 	client.delete(sys.argv[3])
	# else:
	# 	print("Invalid operation")
