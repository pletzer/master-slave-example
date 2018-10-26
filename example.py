"""
"""
import numpy
from mpi4py import MPI

TODO, PENDING, DONE, INDEFINITE = 0, 1, 2, 3


def task(xs):
	return [x**2 for x in xs]

comm = MPI.COMM_WORLD
pe = comm.Get_rank()
nprocs = comm.Get_size()
master = 0

ntasks = 10
ndims = 3

statusData = numpy.array([INDEFINITE]*ntasks, numpy.int)

statusWindow = MPI.Win.Create(statusData, 1, MPI.INFO_NULL, comm=comm)

if pe == master:
    statusData[:] = TODO

statusWindow.Fence()
statusWindow.Get(statusData, master)
statusWindow.Fence()

print('[{}] status = {}'.format(pe, statusData))

statusWindow.Free()

