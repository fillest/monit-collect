#coding: utf-8
import time
import pickle
import pymongo
import pprint
import socket
import calendar
import struct
import datetime
import errno
import os
import argparse
import logging


def main ():
	parser = argparse.ArgumentParser()
	parser.add_argument('--uri', default = 'mongodb://localhost:27017/admin', help = "mongo uri")
	parser.add_argument('--timeout', default = 30 * 1000, type = int)
	parser.add_argument('--graphite-host', default = 'localhost')
	parser.add_argument('--graphite-port', default = 2004, type = int)
	parser.add_argument('--graphite-timeout', default = 15.0, type = float)
	parser.add_argument('--interval', default = 1.0, type = float)
	parser.add_argument('--prefix', default = "{fqdn}.mongo.")
	args = parser.parse_args()

	logging.basicConfig(level = logging.INFO, format = '%(asctime)s %(levelname)-5s %(filename)s:%(funcName)s:%(lineno)d  %(message)s')
	log = logging.getLogger(__name__)


	ps = [
		'connections.current',
		'opcounters.*',
		'backgroundFlushing.flushes',
		'backgroundFlushing.total_ms',
		'dur.commitsInWriteLock',
		'metrics.document.*',
		'metrics.record.moves',
		'locks.buzzoola.timeLockedMicros.*',
		'locks.buzzoola.timeAcquiringMicros.*',
		'globalLock.lockTime',
	]
	#"MongoDB uses a read lock on each database to return recordStats. To minimize this overhead, you can disable this section"
	cmd = {'network': 0, 'cursors': 0, 'repl': 0, 'opcountersRepl': 0, 'recordStats': 0, 'mem': 0, 'asserts': 0, 'indexCounters': 0}


	local_fqdn = socket.getfqdn()
	log.info('Starting. Using fqdn: %s' % local_fqdn)

	log.info("connecting to mongo %s" % args.uri)
	client = pymongo.MongoClient(args.uri, socketTimeoutMS = args.timeout)
	db = client.get_default_database()

	def connect_graphite ():
		log.info("connecting to graphite %s:%s" % (args.graphite_host, args.graphite_port))
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
		s.settimeout(args.graphite_timeout)
		s.connect((args.graphite_host, args.graphite_port))
		return s
	s = connect_graphite()

	pref = args.prefix.format(fqdn = local_fqdn)

	try:
		log.info("starting loop")
		while True:
			t1 = time.time()
			while True:
				try:
					res = db.command('serverStatus', **cmd)
					break
				except pymongo.errors.AutoReconnect as e:
					log.warning('pymongo.errors.AutoReconnect: %s' % e)
					time.sleep(1)
			assert res['ok']
			# print time.time()-t1, pprint.pformat(res)
			# return

			# print datetime.datetime.utcnow(), res['localTime']

			metrics = []
			ts = calendar.timegm(res['localTime'].utctimetuple())

			for path in ps:
				z = path.split('.')
				last_i = len(z) - 1
				ptr = res
				for i, p in enumerate(z):
					if p == '*':
						assert i == last_i, path
						for n, v in ptr.iteritems():
							metrics.append((pref + path[:-(len('.*'))] + '.' + n, (ts, v)))
					else:
						if i == last_i:
							metrics.append((pref + path, (ts, ptr[p])))
						else:
							ptr = ptr[p]
			# print metrics; return

			#http://graphite.readthedocs.org/en/latest/feeding-carbon.html#the-pickle-protocol
			data = pickle.dumps(metrics) #version?
			header = struct.pack("!L", len(data))
			message = header + data
			
			need_reconn = False
			while True:
				try:
					if need_reconn:
						s = connect_graphite()
						need_reconn = False
					n = s.send(message)
					break
				except socket.timeout:
					log.warning("send timed out")
					#TODO think
					s.close() #shutdown?
					need_reconn = True
					time.sleep(1)
				except socket.error as e:
					s.close()
					if e.errno in (errno.EPIPE, errno.ECONNREFUSED):
						log.warning("errno %s: %s" % (errno.errorcode[e.errno], os.strerror(e.errno)))
						need_reconn = True
						time.sleep(1)
					else:
						raise
			assert n == len(message), n #"if only some of the data was transmitted, the application needs to attempt delivery of the remaining data."

			iteration_latency = time.time() - t1
			delay = args.interval - iteration_latency
			# print delay

			if delay >= 0.0:
				time.sleep(delay)
	finally:
		try:
			s.shutdown(socket.SHUT_RDWR)
		except Exception as e:
			log.warning("failed to shut sock: %s" % e)
		s.close()


if __name__ == '__main__':
	main()