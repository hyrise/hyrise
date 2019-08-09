#!/usr/sbin/dtrace -s 

hyrise$target:::query_start
{
	self->start[arg2, copyinstr(arg0)] = timestamp;
}

hyrise$target:::query_end
{
    end = timestamp;
    printf("\n============================\nQuery: %s\nTasks spawned: %i\nExecution time: %iμs\n============================\n",
    	copyinstr(arg0), arg1, (end - self->start[arg2, copyinstr(arg0)]) / 1000);
}
