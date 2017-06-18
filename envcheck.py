#! /usr/bin/env python3
# -*- coging: utf-8 -*-

import copy
import datetime
import threading

######################################################################
# ロギング と デバッグ機能

logger = None
def __setLogger():
	from logging import getLogger, StreamHandler, Formatter, DEBUG
	global logger

	logger = getLogger(__name__)
	handle = StreamHandler()
	handle.setLevel(DEBUG)
	handle.setFormatter(Formatter('%(asctime)s [%(threadName)-10s] %(message)s'))
	logger.addHandler(handle)
__setLogger()


def debug(flag):
	from logging import DEBUG, NOTSET
	global logger

	if(flag):
		logger.setLevel(DEBUG)
		logger.debug("debug on")
	else:
		logger.debug("debug off")
		logger.setLevel(NOTSET)


######################################################################
# 指定されたデータをDBに書き込むスレッド

def _writer_thread(db, obj, data):
	logger.debug("write start.");

	for (name, rows) in data.items():
		if obj.newsize != -1:
			# リサイズオプションが指定されている場合は
			# 書き込む前にサイズを縮小するように指示
			args = {
				'name':name,
				'newsize':obj.newsize + obj.index_max - len(rows),
			}
			if('resizeSortkeys' in obj.option[name]):
				args['key'] = obj.option[name]['resizeSortkeys']

			resize = False
			if obj.option[name]['resizeTiming'] == 'always':
				# 毎回リサイズを実施
				resize = True

			elif obj.option[name]['resizeTiming'] == 'daily':
				# 1回/日リサイズを実施
				now = datetime.datetime.now()
				if '_resizeTiming' in obj.option[name]:
					delta = now - obj.option[name]['_resizeTiming']
					if delta.days >= 1:
						resize = True
						obj.option[name]['_resizeTiming'] = now
				else:
					resize = True
					obj.option[name]['_resizeTiming'] = now

			if resize:
				logger.debug("    resizing ..... " + name);
				db.resizeTable(**args)

		logger.debug("    writing ..... " + name + ", count=" + str(len(rows)))
		for row in rows:
			db.insertTable(name=name, data=row)

	db.commit()

	# スレッド完了待ちをしている処理に完了を伝える
	obj.writer_cond.acquire()
	obj.writer -= 1
	obj.writer_cond.notify()
	obj.writer_cond.release()

	logger.debug("write done.");



######################################################################


class envcheck:
	'''
	指定されたファイル群を読み込み、DBに格納するためのclass

	setTarget()で読み込むファイルとDBのテーブルを定義

	update()で実際にファイルを読み込み、読み込み量が
	定義されたバッファサイズに達したら、DBへ書き込む

	flush()は読み込み量がバッファサイズに達しなくても
	DBへ書き込む

	close()はflushおよびDBのclose処理

	__init__のパラメータ:
		dbAPI:		DBを制御するためのオブジェクト(dbAPI.MySQLなど)
		user:		DBアクセス用ユーザ
		password: 	DBアクセス用のパスワード
		host:		DBが存在するホスト名(デフォルト:localhost)
		database:	データを格納するデーターベース名(デフォルト:envcheck)
		buffersize:	メモリ上に保持するデータ数。update()で読み出した量がこの数に達するとDBに書き込まれる(デフォルト:1)
		limit:		DBに保持するデータ数の最大(デフォルト:-1(制限しない))
		上記以外:	DB用のパラメータとしてdbAPIに渡す
	'''

	def __init__(self, dbAPI, user, password, host='localhost', database='envcheck', buffersize=1, limit=-1, **kwargs):
		self.__db = dbAPI(user=user, password=password, host=host, database=database, **kwargs)

		if buffersize < 1:
			buffersize = 1
		if limit == 0:
			limit = 1

		if limit < 0:
			self.index_max = buffersize
			self.newsize = -1

		elif buffersize < limit:
			self.index_max = buffersize
			self.newsize = limit - buffersize

		else:
			self.index_max = limit
			self.newsize = 0

		self.option = {}
		self.__target = {}
		self.__static = {}
		self.__buffer = {}
		self.__workmem = {}
		self.wid = 0
		self.update_lock = threading.Lock()
		self.writer_cond = threading.Condition(lock = threading.Lock())
		self.writer = 0


	def close(self):
		self.flush()
		self.__db.close()


	def setTarget(self, name, target, static=False, **kwargs):
		# 読み出すファイルなどを定義する
		# nameがDB上のテーブル名となる
		clone = []
		columns = []
		for t in target:
			if len(t.keys()) != 1:
				raise SyntaxError('targetの指定が不正です')
			col = list(t.keys())[0]
			if 'type' in t[col]:
				columns.append( {col:t[col]['type']} )
			if ('file' in t[col]) or ('callback' in t[col]):
				c = copy.deepcopy(t)
				if 'type' in c[col]:
					del c[col]['type']
				clone.append(c)

		self.__db.createTable(name=name, columns=columns)
		self.__target[name] = clone
		self.__static[name] = static
		self.__buffer[name] = []
		self.__workmem[name] = {}
		self.option[name] = {
				'resizeTiming':'always',
			}

		for (k, v) in kwargs.items():
			if k == 'resizeSortkeys':
				self.option[name][k] = v
			elif k == 'resizeTiming':
				self.option[name][k] = v
			else:
				raise KeyError('不正なオプションです')



	def __update(self, name):
		# update処理の実体
		# 指定されたルールに従ってファイルの内容を収集

		data = {}
		for ent in self.__target[name]:
			col = list(ent.keys())[0]
			param = ent[col]
			lines = []

			if 'file' in param:
				f = open(param['file'], "r")
				lines = f.readlines()
				f.close()

			if 'callback' in param:
				if 'use_workmem' not in param or (not param['use_workmem']):
					if 'multi_param' not in param or (not param['multi_param']):
						data[col] = param['callback'](lines)
					else:
						data.update(param['callback'](lines))
				else:
					if 'multi_param' not in param or (not param['multi_param']):
						data[col] = param['callback'](lines, self.__workmem[name])
					else:
						data.update(param['callback'](lines, self.__workmem[name]))

			elif len(lines) == 1:
				data[col] = lines[0].replace("\n", "")

			else:
				data[col] = "".join(lines)

		self.__buffer[name].append(data)



	def update(self, block=False):
		# デフォルトはupdate処理中に再度updateを呼んだ場合は
		# update処理をせずにFalseを戻す

		r = self.update_lock.acquire(blocking=block)
		if not r:
			logger.debug("update locking")
			return False
		#logger.debug("update start")

		flush = {}
		for name in self.__target.keys():
			# 静的データ/動的データに応じて読み出しルールを切り替え

			if(self.__static[name]):
				if(len(self.__buffer[name]) > 0):
					continue

				if(self.__db.countTable(name)):
					# 処理高速化のため既にデータが存在する場合
					# ダミーデータを設定してcountTableが
					# 実行されないようにする
					self.__buffer[name].append("dummy")
					continue

				self.__update(name)
				flush[name] = self.__buffer[name]

			else:
				self.__update(name)
				self.writer_cond.acquire()

				if self.writer == 0 and len(self.__buffer[name]) >= self.index_max:
					# writerスレッドが動作中の場合は書き込み要求を保留
					# 蓄積用buffer最大値まで溜まったら書き込み用バッファへ移動
					flush[name] = self.__buffer[name]
					self.__buffer[name] = []

				self.writer_cond.release()

		# 書き込み用バッファにデータが存在する場合は書き込み実施
		if(len(flush.keys()) > 0):
			self.__write(flush)

		#logger.debug("update done")
		self.update_lock.release()
		return True



	def flush(self):
		# 蓄積用バッファに溜まっている場合は強制的に書き出す

		logger.debug("flush buffer")

		# 書き込みスレッドが動作中は待つ
		self.writer_cond.acquire()
		while self.writer != 0:
			self.writer_cond.wait()
		self.writer_cond.release()

		buff = {}
		for name in self.__target.keys():
			if self.__static[name] == False and len(self.__buffer[name]):
				buff[name] = self.__buffer[name]
				self.__buffer[name] = []

		if(len(buff.keys()) > 0):
			self.__write(buff, sync=True)



	def __write(self, data, sync=False):

		''' 書き込み処理は時間がかかるため別スレッドで実行 '''

		# スレッド動作の認識用フラグ
		self.writer_cond.acquire()
		self.writer += 1
		self.writer_cond.release()

		th = threading.Thread(target=_writer_thread, name='ECwriter#'+str(self.wid), kwargs={
					'db':self.__db,
					'obj':self,
					'data':data,
				})
		self.wid = (self.wid + 1) % 10
		if sync:
			th.daemon = False
		else:
			th.daemon = True

		th.start()
		if sync:
			th.join()
