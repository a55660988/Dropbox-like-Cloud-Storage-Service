import rpyc
import sys

"""
The BlockStore service is an in-memory data store that stores blocks of data,
indexed by the hash value.  Thus it is a key-value store. It supports basic
get() and put() operations. It does not need to support deleting blocks of
data–we just let unused blocks remain in the store. The BlockStore service only
knows about blocks–it doesn’t know anything about how blocks relate to files.
"""
class BlockStore(rpyc.Service):


	"""
	Initialize any datastructures you may need.
	"""
	def __init__(self):
		# blockMap = {"HashValue1": block0, "HashValue2": block1}
		self.blockMap = {}
		self.blockList = []
		# self.eprint("blockList: ", self.blockList)

	"""
		store_block(h, b) : Stores block b in the key-value store, indexed by
		hash value h

		As per rpyc syntax, adding the prefix 'exposed_' will expose this
		method as an RPC call
	"""
	def exposed_store_block(self, h, block):
		# TODO: worry about concurrency problem for count len(self.blockList)
		self.eprint("storing block ", block, " to blockMap with index hashValue: ", h)
		self.blockList.append(block)
		self.blockMap[h] = len(self.blockList) - 1

	"""
	b = get_block(h) : Retrieves a block indexed by hash value h

		As per rpyc syntax, adding the prefix 'exposed_' will expose this
		method as an RPC call
	"""
	def exposed_get_block(self, h):
		if self.exposed_has_block(h):
			self.eprint("Get blockMap with index hashValue: ", h)
			return self.blockMap[h]
		else:
			self.eprint("blockMap doesn't exist with index hashValue: ", h)

	"""
		True/False = has_block(h) : Signals whether block indexed by h exists
		in the BlockStore service

		As per rpyc syntax, adding the prefix 'exposed_' will expose this
		method as an RPC call



	"""
	def exposed_has_block(self, h):
		if h not in self.blockMap:
			self.eprint("hashValue ", h, " doesn't exist in blockMap")
			return False
		else:
			self.eprint("hashValue ", h, " exist in blockMap")
			return True

	def eprint(*args, **kwargs):
		print(*args, file=sys.stderr, **kwargs)


if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	# port = int(sys.argv[1])
	print("Start blockstore...")
	port = int(5000)
	server = ThreadPoolServer(BlockStore(), port=port)
	server.start()
