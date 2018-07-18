provider hyrise {
        probe job_start(long id, char* description, long this_pointer);
        probe job_end(long id, long this_pointer);
        probe schedule_tasks(int task_size);
        probe schedule_tasks_and_wait(int task_size);
        probe query_start(char* query, int task_size);
        probe query_end(char* query, int task_size);
};
