#!/usr/bin/env python
"""Demonstrate the task-pull paradigm for high-throughput computing
using mpi4py. Task pull is an efficient way to perform a large number of
independent tasks when there are more tasks than processors, especially
when the run times vary for each task. 

This code is over-commented for instructional purposes.

This example was contributed by Craig Finch (cfinch@ieee.org).
Inspired by http://math.acadiau.ca/ACMMaC/Rmpi/index.html
"""
from mpi4py import MPI
import random
import time
import argparse
import numpy

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('-t', type=int, dest='numTasks', default=0, help='Number of random tasks')

args = parser.parse_args()
ntasks = args.numTasks


random.seed(1234)

# Define MPI message tags
READY, DONE, EXIT, START = 0, 1, 2, 3

# Initializations and preliminaries
comm = MPI.COMM_WORLD   # get MPI communicator object
size = comm.size        # total number of processes
rank = comm.rank        # rank of this process
status = MPI.Status()   # get MPI status object


def workerFunction(task):
	# simulates a function that takes a random time to execute
	tic = time.time()
	time.sleep(10*random.random())
	toc = time.time()
    # return the amout of time is takes to run the task
	return toc - tic

if rank == 0:
    results = []
    # Master process executes code below
    tasks = [i for i in range(ntasks)]
    task_index = 0
    num_workers = size - 1
    closed_workers = 0
    print("Master starting with {} workers".format(num_workers))
    while closed_workers < num_workers:
        data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        source = status.Get_source()
        tag = status.Get_tag()
        if tag == READY:
            # Worker is ready, so send it a task
            if task_index < len(tasks):
                comm.send(tasks[task_index], dest=source, tag=START)
                print("Sending task %d to worker %d" % (task_index, source))
                task_index += 1
            else:
                comm.send(None, dest=source, tag=EXIT)
        elif tag == DONE:
            results.append(data)
            print("Got {} from worker {}".format(data, source))
        elif tag == EXIT:
            print("Worker {} exited.".format(source))
            closed_workers += 1

    print("Master: results: = {} sum = {}".format(results, numpy.sum(results)))
else:
    # Worker processes execute code below
    name = MPI.Get_processor_name()
    print("I am a worker with rank {} on {}.".format(rank, name))
    while True:
        comm.send(None, dest=0, tag=READY)
        task = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        tag = status.Get_tag()
        
        if tag == START:
            # Do the work here
            result = workerFunction(task)
            comm.send(result, dest=0, tag=DONE)
        elif tag == EXIT:
            break

    comm.send(None, dest=0, tag=EXIT)


