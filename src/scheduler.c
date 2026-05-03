#include <stdio.h>
#include <sys/stat.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>

#include "scheduler.h"

int run_scheduler_single_cpu(const sim_config_t *cfg) {
    (void)cfg;
    fprintf(stderr,
            "TODO: implement the single-CPU scheduler in src/scheduler.c\n"
            "Suggested order:\n"
            "- parse jobs in the format JOB_ID ARRIVAL PRIORITY CPU_TIME\n"
            "- implement FCFS for --cpus 1\n"
            "- add SJF, SRTF, and RR\n"
            "- verify trace and stats output\n");

    // Initialize then parse jobs
    workload_t wl = {0};
    wl.jobs = calloc(MAX_JOBS, sizeof(job_t));
    if(parse_jobs(cfg->input_path, &wl) != 0) {
        fprintf(stderr, "Error parsing jobs\n");
        return -1;
    }

    // Implement FCFS, RR scheduling - Franky

    // Implemement SJF, SRTF scheduling - Brandon

    // Verify trace and stats output - Franky

    return -1;
}

int run_scheduler_multi_cpu(const sim_config_t *cfg) {
    (void)cfg;
    fprintf(stderr,
            "TODO: implement the multi-CPU threaded scheduler in src/scheduler.c\n"
            "Required behavior:\n"
            "- preserve the single-CPU scheduling semantics\n"
            "- create one scheduler thread and N CPU worker threads\n"
            "- protect shared state with mutexes\n"
            "- sleep on condition variables instead of busy waiting\n");
    return -1;
}