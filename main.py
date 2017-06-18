#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import datetime
import time
import signal

import envcheck
import dbAPI.MySQL

######################################################################
# Callback用関数

def local_int(data):
	return int(data[0])

def local_float(data):
	return float(data[0])

def timestamp(data):
	return datetime.datetime.utcnow()

def gethostname(data):
	from socket import getfqdn
	return getfqdn()

def hwmon_temp(data):
	return float(data[0])/1000



# 変動しないCPUの情報
def cpuinfo(data):
	procs = 0
	for s in (data):
		r = re.match(r"processor\s*:\s*\d+", s)
		if r:
			procs += 1

		r = re.match(r"model name\s*:\s*(.+)", s)
		if r:
			model = r.group(1)

		r = re.match(r"siblings\s*:\s*(\d+)", s)
		if r:
			siblings = int(r.group(1))

		r = re.match(r"cpu cores\s*:\s*(\d+)", s)
		if r:
			cores = int(r.group(1))

	return model + " x " + str(procs) + " processors (" + str(int(procs/siblings)) + " sockets, " + str(cores) + " cores, " + str(int(siblings/cores)) + " threadings)"



# CPU使用率/(初回はすべて0)
def cpustat(data, work):
	pat = re.compile(r"(?P<name>cpu\d*)\s+(?P<user>\d+)\s+(?P<nice>\d+)\s+(?P<system>\d+)\s+(?P<idle>\d+)\s+(?P<iowait>\d+)\s+(?P<irq>\d+)\s+(?P<softirq>\d+)\s+(?P<steal>\d+)")

	rval = {}
	work['cpu_cur'] = {}
	for s in data:
		r = re.match(pat, s)
		if r is None:
			break

		d = r.groupdict()
		cpu = d['name']
		delta = {}
		total = 0

		del d['name']
		if 'cpu_old' in work:
			for (k, v) in d.items():
				label = cpu + '_' + k
				work['cpu_cur'][label] = int(v)
				delta[label] = int(v) - work['cpu_old'][label]
				total += delta[label]
		else:
			for (k, v) in d.items():
				label = cpu + '_' + k
				work['cpu_cur'][label] = int(v)
				delta[label] = 0
			total = 1

		for k in delta.keys():
			rval[k] = 100.0 * delta[k] / total

	work['cpu_old'] = work['cpu_cur']

	return rval



# メモリ使用率
def memorystat(data):
	mem = {}
	pat = re.compile(r"\s*(?P<name>.+):\s*(?P<value>\d+)\s+kB")
	for s in data:
		r = re.match(pat, s)
		if r:
			g = r.groupdict()
			mem[g['name']] = int(g['value'])

	mem['_cache'] = mem['Cached'] + mem['Slab']
	mem['_used'] = mem['MemTotal'] - mem['MemFree'] - mem['Buffers'] - mem['_cache']

	rval = {
		'mem_total':	mem['MemTotal'],
		'mem_used':		mem['_used'],
		'mem_free':		mem['MemFree'],
		'mem_shared':	mem['Shmem'],
		'mem_buffers':	mem['Buffers'],
		'mem_cache':	mem['_cache'],
		'mem_available':	mem['MemAvailable'],
		'mem_slabReclaim':	mem['SReclaimable'],
		'mem_slabUnReclaim':mem['SUnreclaim'],
		'mem_actAnon':		mem['Active(anon)'],
		'mem_inactAnon':	mem['Inactive(anon)'],
		'mem_actFile':		mem['Active(file)'],
		'mem_inactFile':	mem['Inactive(file)'],
		'mem_unevictable':	mem['Unevictable'],
	}

	return rval



######################################################################
# 採取データ定義部

# 1回しか採取しないデータ ##########################
data_statics = (
	{'sid':{
		'type':'autonumber',
		}
	},
	{'hostname':{
		'type':'varchar255',
		'callback':gethostname,
		}
	},
	{'version':{
		'type':'varchar255',
		'file':'/proc/version',
		}
	},
	{'cpuinfo':{
		'type':'varchar255',
		'file':'/proc/cpuinfo',
		'callback':cpuinfo,
		}
	},
	{'cpufreq_min':{
		'type':'umediumint',
		'file':'/sys/bus/cpu/devices/cpu0/cpufreq/cpuinfo_min_freq',
		'callback':local_int
		}
	},
	{'cpufreq_max':{
		'type':'umediumint',
		'file':'/sys/bus/cpu/devices/cpu0/cpufreq/cpuinfo_max_freq',
		'callback':local_int
		}
	},
	{'temp1_label':{
		'type':'varchar255',
		'file':'/sys/class/hwmon/hwmon1/temp1_label',
		}
	},
	{'temp2_label':{
		'type':'varchar255',
		'file':'/sys/class/hwmon/hwmon1/temp2_label',
		}
	},
	{'temp3_label':{
		'type':'varchar255',
		'file':'/sys/class/hwmon/hwmon1/temp3_label',
		}
	},
)

# 毎回採取するデータ/後で組み立てる ################
data_dynamics = [
	{'did':{
		'type':'autonumber',
		}
	},
]

data_memory = (
	{'MEMORY_STATUS':{
		'multi_param':True,
		'file':'/proc/meminfo',
		'callback':memorystat,
		}
	},
	{'mem_total':{
		'type':'uint',
		}
	},
	{'mem_used':{
		'type':'uint',
		}
	},
	{'mem_free':{
		'type':'uint',
		}
	},
	{'mem_shared':{
		'type':'uint',
		}
	},
	{'mem_buffers':{
		'type':'uint',
		}
	},
	{'mem_cache':{
		'type':'uint',
		}
	},
	{'mem_available':{
		'type':'uint',
		}
	},
	{'mem_slabReclaim':{
		'type':'uint',
		}
	},
	{'mem_slabUnReclaim':{
		'type':'uint',
		}
	},
	{'mem_actAnon':{
		'type':'uint',
		}
	},
	{'mem_inactAnon':{
		'type':'uint',
		}
	},
	{'mem_actFile':{
		'type':'uint',
		}
	},
	{'mem_inactFile':{
		'type':'uint',
		}
	},
	{'mem_unevictable':{
		'type':'uint',
		}
	},
)

data_hwmon = (
	{'cpufreq_cur':{
		'type':'umediumint',
		'file':'/sys/bus/cpu/devices/cpu0/cpufreq/cpuinfo_cur_freq',
		'callback':local_int
		}
	},
	{'temp1':{
		'type':'float',
		'file':'/sys/class/hwmon/hwmon1/temp1_input',
		'callback':hwmon_temp, 
		}
	},
	{'temp2':{
		'type':'float',
		'file':'/sys/class/hwmon/hwmon1/temp2_input',
		'callback':hwmon_temp, 
		}
	},
	{'temp3':{
		'type':'float',
		'file':'/sys/class/hwmon/hwmon1/temp3_input',
		'callback':hwmon_temp, 
		}
	},
	{'fan2':{
		'type':'umediumint',
		'file':'/sys/class/hwmon/hwmon2/fan2_input',
		'callback':local_int,
		}
	},
)


######################################################################
def create_dynamics():
	# CPUは個数に応じたエントリを生成
	cpuname = []
	pat = re.compile(r"\s*(cpu\d*)")

	f = open('/proc/stat', 'r')
	s = f.readline();
	while s:
		r = re.match(pat, s)
		if r:
			cpuname.append(r.group(1))
		s = f.readline();
	f.close()

	data_dynamics.append({
		'CPU_STATUS':{
			'multi_param':True,
			'use_workmem':True,
			'file':'/proc/stat',
			'callback':cpustat,
		}
	})
	for name in (cpuname):
		data_dynamics.extend((
			{name+'_user':	{'type':'float'}},
			{name+'_system':{'type':'float'}},
			{name+'_nice':	{'type':'float'}},
			{name+'_idle':	{'type':'float'}},
			{name+'_iowait':{'type':'float'}},
			{name+'_irq':	{'type':'float'}},
			{name+'_softirq':{'type':'float'}},
			{name+'_steal':	{'type':'float'}},
		))

	# メモリのエントリを追加
	data_dynamics.extend(data_memory)

	# H/Wモニタのエントリを追加
	data_dynamics.extend(data_hwmon)

	# 最後にタイムスタンプを追加
	data_dynamics.append({
		'time':{
			'type':'timestamp',
			'callback':timestamp,
		},
	})



######################################################################
env = None

def update(signum, frame):
	global env
	env.update()


def flush(signum, frame):
	global env
	signal.setitimer(signal.ITIMER_REAL, 0, 0)
	env.flush()
	env.close()
	exit(0)

def main():
	global env

	create_dynamics()

	env = envcheck.envcheck(dbAPI=dbAPI.MySQL.dbAPI, user='root', password='pass', buffersize=60, limit=365*24*60*60)

	env.setTarget(static=True, name='statics', target=data_statics)
	env.setTarget(static=False, name='dynamics', target=data_dynamics, resizeSortkeys=({'time':-1}, {'did':-1}), resizeTiming='daily')

	# INT か TERM を受け取った場合 flush して終了
	signal.signal(signal.SIGINT, flush)
	signal.signal(signal.SIGTERM, flush)

	# 毎秒情報を更新する
	signal.signal(signal.SIGALRM, update)
	signal.siginterrupt(signal.SIGALRM, False)
		# システムコール実行時に割り込んだ場合、割り込み終了後に再開
	signal.setitimer(signal.ITIMER_REAL, 0.1, 0.1)

	while 1:
		time.sleep(1)

	env.close()



######################################################################

if __name__ == "__main__":
	envcheck.debug(True)
	main()
