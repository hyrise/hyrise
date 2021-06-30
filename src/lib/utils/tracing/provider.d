// This file contains probe definitions required for tracing via dtrace.

provider hyrise {
        probe job_start(long id, char* description, uintptr_t this_pointer);
        probe job_end(long id, uintptr_t this_pointer);
        probe schedule_tasks(long task_size);
        probe schedule_tasks_and_wait(long task_size);
        probe tasks_per_statement(uintptr_t tasks, char* query_string, uintptr_t this_pointer);
        probe sql_parsing(char* query_string, long parsing_time);
        probe create_pipeline(uintptr_t this_pointer);
        probe pipeline_creation_done(size_t number_of_statements, char* query_string, uintptr_t this_pointer);
        probe tasks(uintptr_t tasks, uintptr_t single_task);
        probe operator_tasks(uintptr_t abstract_operator, uintptr_t operator_task);
        probe operator_started(char* operator_name);
        probe operator_executed(char* operator_name, long execution_time, long output_rows, long output_chunks, uintptr_t this_pointer);
        probe summary(char* query_string, long translation_time, long cache_time, long optimization_time, long compile_time, long execution_time, int query_plan_cached, size_t tasks_size, uintptr_t this_pointer);
};
