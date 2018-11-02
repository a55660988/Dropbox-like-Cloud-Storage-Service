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
		self.blockList.append(block)
		self.blockMap[h] = len(self.blockList) - 1
		# self.eprint("In exposed_store_block")
		# print("block map: ", self.blockMap)
		return "exposed_store_block DONE"

	"""
	b = get_block(h) : Retrieves a block indexed by hash value h

		As per rpyc syntax, adding the prefix 'exposed_' will expose this
		method as an RPC call
	"""
	def exposed_get_block(self, h):
		if self.exposed_has_block(h):
			return self.blockMap[h]
		self.eprint("block doesn't exist with hashValue: ", h)
		return "Block doesn't exist"

	"""
		True/False = has_block(h) : Signals whether block indexed by h exists
		in the BlockStore service

		As per rpyc syntax, adding the prefix 'exposed_' will expose this
		method as an RPC call



	"""
	def exposed_has_block(self, h):
		if h not in self.blockMap:
			return False
		return True

	def eprint(*args, **kwargs):
		print(*args, file=sys.stderr, **kwargs)


if __name__ == '__main__':
	bs = BlockStore()
	bs.exposed_store_block("fnwefnlekf", b'123')
	bs.exposed_store_block("snjkdfnsj",b'34545')
	print(bs.exposed_has_block("fnwefnlekf"))
	# from rpyc.utils.server import ThreadPoolServer
	# port = int(sys.argv[1])
	# server = ThreadPoolServer(BlockStore(), port=port)
	# server.start()
