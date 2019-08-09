#!/usr/sbin/dtrace -s 

#pragma D option bufpolicy=ring

// This script measures the runtime of jobs executed. After cancelling the execution of this
// script a detailed summary of jobs ran will be printed to the console as a histogram. It provides the same
// functionality as `job_start_end.stp`.
// NOTE: the job_start probe needs some time to retrieve the job's callers. If there are many short running ones
//       it might lead to a crash of this script, since the event handling overhead is too high (it always has to
//       look on the stack). You can simply comment it out if you want. Of course, you will lose the information
//       about the job caller.


// Job started event
hyrise$target:::job_start
{
    /*
      arg0: _id (int)
      arg1: _description  (string)
      arg2: this  (long, needed as identifier)
    */

    // Get first 3 elements of call stack
	self->job_start[arg2] = timestamp;
	description = copyinstr(arg1);
    printf("\nStarting job with id %d\n\tRunning on cpu %i\n\tDescription: \"%s\"\n", arg0, cpu, description);
}

// Job finished event
hyrise$target:::job_end
{
    /*
      arg0: _id (int) 
      arg1: this  (long, needed as identifier)
    */

    end = timestamp;
    start_time = self->job_start[arg1];
    execution_time_us = (end - start_time) / 1000;
    if (start_time != NULL) {
		@asd[ustack(3)] = quantize(execution_time_us);
        @runtimes = quantize(execution_time_us); //
    }
    printf("Job with id %i finished within %i μs\n", arg0, execution_time_us);
}
