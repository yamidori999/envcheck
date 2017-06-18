#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import mysql.connector

# TINYINT :     1B        -128 -        127 /           255
# SMALLINT :    2B      -32768 -      32767 /        65,535
# MEDIUMINT :   3B    -8388608 -    8388607 /    16,777,215
# INT :         4B -2147483648 - 2147483647 / 4,294,967,295
# BIGINT :      8B -9223372036854775808 - 9223372036854775807
#                                       / 18,446,744,073,709,551,615

_colType = {
	'autonumber':(0, 'BIGINT UNSIGNED AUTO_INCREMENT UNIQUE'),
	'timestamp':(1, 'DATETIME'),
	'varchar255':(1, 'VARCHAR(255)'),

	'tinyint':(1, 'TINYINT'),
	'smallint':(1, 'SMALLINT'),
	'mediumint':(1, 'MEDIUMINT'),
	'int':(1, 'INT'),
	'bigint':(1, 'BIGINT'),

	'utinyint':(1, 'TINYINT UNSIGNED'),
	'usmallint':(1, 'SMALLINT UNSIGNED'),
	'umediumint':(1, 'MEDIUMINT UNSIGNED'),
	'uint':(1, 'INT UNSIGNED'),
	'ubigint':(1, 'BIGINT UNSIGNED'),

	'float':(1, 'FLOAT'),
	'double':(1, 'DOUBLE'),
	'ufloat':(1, 'FLOAT UNSIGNED'),
	'udouble':(1, 'DOUBLE UNSIGNED'),
}

class Error(Exception):
	def __init__(self, msg=None):
		self.msg = msg


class SyntaxError(Error):
	pass


class KeyError(Error):
	pass


class dbAPI:
	""" DBが変わってもアクセス方法を統一するためのラッパー """

	def __init__(self, user, password, database, host="localhost", port=3306, charset="utf8", collation=None):
		self.__conn = mysql.connector.connect(host=host, port=port, user=user, password=password, autocommit=False)
		self.__conn.set_charset_collation(charset, collation)
		self.createDatabase(database)
		self.selectDatabase(database)
		self.__insertSQL = {}


	def close(self):
		self.__conn.close()


	def start_transaction(self):
		self.__conn.start_transaction()


	def commit(self):
		self.__conn.commit()


	def rollback(self):
		self.__conn.rollback()


	def createDatabase(self, name):
		csr = self.__conn.cursor()
		csr.execute("CREATE DATABASE IF NOT EXISTS " + name)
		csr.close()


	def dropDatabase(self, name):
		csr = self.__conn.cursor()
		csr.execute("DROP DATABASE IF EXISTS " + name)
		csr.close()


	def selectDatabase(self, name):
		self.__conn.config(database=name)
		self.__conn.reconnect()


	def createTable(self, name, columns=(), extend=()):
		SQL = "CREATE TABLE IF NOT EXISTS " + name + "("
		COL = []
		INS = []
		for col in columns:
			if len(col.keys()) != 1:
				raise(SyntaxError('1つのカラムに対して複数の名称が指定されています - ' + str(col)))
			c = list(col.keys())[0]
			t = col[c].lower()
			if t not in _colType:
				raise(KeyError('カラムの種類が不正です - ' + str(col)))
			COL.append(c + " " + _colType[t][1])
			if _colType[t][0]:
				INS.append(c)

		for ext in extend:
			if len(ext.keys()) != 1:
				raise(SyntaxError('1つの定義に対して複数の要求が指定されています - ' + str(ext)))
			e = list(ext.keys())[0]
			n = "" if ('name' not in ext[e]) else ext[e]['name']
			c = ",".join(ext[e]['columns'])
			if e.lower() == "unique":
				COL.append("UNIQUE " + n + "(" + c + ")")
			elif e.lower() == "index" or e.lower() == "key":
				COL.append("INDEX " + n + "(" + c + ")")
			else:
				raise(KeyError('サポートされていない要求です - ' + str(ext)))

		csr = self.__conn.cursor()
		csr.execute(SQL + ",".join(COL) + ")")
		csr.close()

		self.__insertSQL[name] = "INSERT " + name + "(" + ','.join(INS) + ") VALUES (%(" + ')s, %('.join(INS) + ")s)"


	def dropTable(self, name):
		csr = self.__conn.cursor()
		csr.execute("DROP TABLE IF EXISTS " + name)
		csr.close()


	def truncateTable(self, name):
		csr = self.__conn.cursor()
		csr.execute("TRUNCATE TABLE " + name)
		csr.close()


	def insertTable(self, name, data={}, commit=False):
		csr = self.__conn.cursor()
		csr.execute(self.__insertSQL[name], data)
		csr.close()
		if commit:
			self.__conn.commit()


	def countTable(self, name):
		csr = self.__conn.cursor()
		csr.execute("SELECT count(*) FROM " + name);
		r = csr.fetchone()
		csr.close()

		return r[0]


	# keyとnewsizeの関係は size=10, key=({'col1':-1},)
	# とすると、colを降順にして上から10行までが残る
	def resizeTable(self, name, newsize, key=(), commit=False):
		SQL = "DELETE QUICK FROM " + name
		ORDER = []
		for k in key:
			if len(k.keys()) != 1:
				raise(KeyError('1つのカラム(key)に対して複数の名称が指定されています - ' + str(k)))
			col = list(k.keys())[0]
			if k[col] < 0:
				ORDER.append(col + " ASC")
			else:
				ORDER.append(col + " DESC")
		if len(ORDER):
			SQL += " ORDER BY " + ','.join(ORDER)

		csr = self.__conn.cursor()
		csr.execute("SELECT count(*) FROM " + name);
		r = csr.fetchone()
		if r[0] <= newsize:
			csr.close()
			return

		csr.execute(SQL + " LIMIT " + str(r[0] - newsize));
		csr.close()
		if commit:
			self.__conn.commit()


