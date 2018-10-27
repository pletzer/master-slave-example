"""
"""
import numpy
import time
from mpi4py import MPI

READY, BUSY, BYE = 1, 2, 3
STATUS_LINE, WORK_LINE = 100, 101


def performTask(data):
    time.sleep(2) # simulates a long task


comm = MPI.COMM_WORLD
pe = comm.Get_rank()
nprocs = comm.Get_size()
master = 0
worker = 1

ntasks = 3

if pe == master:

	# all the workers are ready
	workerStatus = {worker: READY for worker in range(1, nprocs)}

	for taskId in range(ntasks):

		# find a worker who is ready to take on some new tasks
		for worker, status in workerStatus.items():

			if status == READY:

				comm.send(taskId, dest=worker, tag=WORK_LINE)
				workerStatus[worker] = BUSY
				print('worker {} is now busy...'.format(worker))
				break

	# tell the workers to shutdown
	for worker in range(1, nprocs):
		comm.send(None, dest=worker, tag=WORK_LINE)


else:

	# workers

	status = READY
	while status != BYE:
		# get order
		data = comm.recv(source=master, tag=WORK_LINE)
		if data is None:
			status = BYE
			print('[{}] is shutting down...'.format(pe))
		else:
			print('[{}] performs task {}'.format(pe, data))
			performTask(data)
			#comm.send(READY, dest=master, tag=STATUS_LINE)


