import rpyc
import sys


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

	"""
		Initialize the class using the config file provided and also initialize
		any datastructures you may need.
	"""
	def __init__(self, config):
		self.config_dict = self.parseConfig(config)
		self.connBlockStore = rpyc.connect("localhost", self.config_dict['block1'])
		# fileHashListMap = {"filename": {fileVer: 0, hashList: ["HashValue1", "HashValue2"]}}
		self.fileHashListMap = {}
		self.fileHashListMap["/Users/Danny/Desktop/test/a.txt"] = {"fileVer": 1, "hashList": ["HashABC", "HashDEF"]}
		self.fileHashListMap["b.txt"] = {"fileVer": 1, "fileHashListIndex": ["HashGHI", "HashJKL"]}
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
		# TODO: check version first
		missingBlockList = []
		for h in hashlist:
			if self.connBlockStore.root.exposed_has_block(h):
				# TODO: handle block exist
				self.eprint("blockstore has block: ", h)
			else:
				missingBlockList.append(h)
		if len(missingBlockList) == 0:
			# client doesn't need to upload block
			# add 1 to ver
			return "OK"
		else:
			# return missingBlockList which client needs to upload block to
			self.eprint("return file: ", filename, " missingBlockList", missingBlockList)
			return missingBlockList

	"""
		DeleteFile(f,v): Deletes file f. Like ModifyFile(), the provided
		version number v must be one bigger than the most up-date-date version.

		As per rpyc syntax, adding the prefix 'exposed_' will expose this
		method as an RPC call
		"""
	def exposed_delete_file(self, filename, version):
		pass


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
		self.eprint("Checking file: ", filename)
		if filename in self.fileHashListMap:
			fileVer = self.fileHashlistMap[filename]["fileVer"]
			fileHashList = self.fileHashListMap[filename]["hashList"]
			self.eprint("Get file: ", filename, " and return fileVer: ", fileVer, " and fileHashList")
			return fileVer, fileHashList

		# file not exist
		self.eprint("file: ", filename, " doesn't exist in server")
		# we return fileVer = 0, fileHashList = []
		return 0, []

	def eprint(*args, **kwargs):
		print(*args, file=sys.stderr, **kwargs)

	def parseConfig(self, config):
		dict = {}
		with open(config, 'r') as file:
			lines = [line.strip('\n') for line in file]
			lines = lines[1:]
			for line in lines:
				temp = line.split(":")
				dict[temp[0]] = temp[2]
		return dict

if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	server = ThreadPoolServer(MetadataStore(sys.argv[1]), port = 6000)
	print("Start metastore...")
	# server = ThreadPoolServer(MetadataStore("localhost"), port = 6000)
	server.start()
