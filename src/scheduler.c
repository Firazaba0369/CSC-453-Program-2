#include <stdio.h>
#include <sys/stat.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>

#include "scheduler.h"

void dump_stats(workload_t *wl, FILE *stats_fp){
    int response_times[wl->njobs];
    int turnaround_times[wl->njobs];
    int waiting_times[wl->njobs];

    fprintf(stats_fp, "STATS\n");

    // Calculate individual times for each job and write to stats file
    for (int i = 0; i < wl->njobs; i++){
        job_t *job = &wl->jobs[i];
        response_times[i] = job->first_run_time - job->arrival_time;
        turnaround_times[i] = (job->completion_time + 1) - job->arrival_time;
        waiting_times[i] = job->total_wait_time;
        fprintf(stats_fp, "%s response %d turnaround %d waiting %d\n", 
            job->id, response_times[i], turnaround_times[i], waiting_times[i]);
    }

    int total_response_time = 0;
    int total_turnaround_time = 0;
    int total_waiting_time = 0;

    // Calculate average times
    for (int i = 0; i < wl->njobs; i++){
        total_response_time += response_times[i];
        total_turnaround_time += turnaround_times[i];
        total_waiting_time += waiting_times[i];
    }
    fprintf(stats_fp, "AVG response %.2f turnaround %.2f waiting %.2f\n", 
        (float)total_response_time / wl->njobs, (float)total_turnaround_time / wl->njobs, 
        (float)total_waiting_time / wl->njobs);

    return;
}

int fcfs(const sim_config_t *cfg, workload_t *wl){
    FILE *trace_fp = stdout;
    FILE *stats_fp = stdout;

    // Output trace to file path when provided
    if (cfg->trace_path != NULL){
        trace_fp = fopen(cfg->trace_path,"w");
        if(trace_fp == NULL){
            perror("Couldn't open trace file");
            return -1;
        }
    }

    // Output stats to file path when provided
    if (cfg->stats_path != NULL){
        stats_fp = fopen(cfg->stats_path, "w");
        if (stats_fp == NULL) {
            if (cfg->trace_path != NULL) fclose(trace_fp);
            perror("Couldn't open stats file");
            return -1;
        }
    }

    int tick = 0;
    int completed_jobs = 0;
    int total_jobs = wl->njobs;
    queue_t rq = {0}; //ready queue
    job_t *cpu_job = NULL; //job currently running on CPU
    // Run until all jobs have completed
    while (completed_jobs < total_jobs) {

        // Admit jobs to the ready queue when their arrival time is reached 
        for(int i = 0; i < wl->njobs; i++){
            job_t *job = &wl->jobs[i];
            if(job->state == JOB_NEW && job->arrival_time == tick){
                job->state = JOB_READY;
                job->ready_enqueue_time = tick;
                if (rq.back - rq.front >= MAX_JOBS) {
                    fprintf(stderr, "Ready queue overflow\n");
                    if (cfg->trace_path != NULL) fclose(trace_fp);
                    if (cfg->stats_path != NULL) fclose(stats_fp);
                    return -1;
                }
                rq.data[rq.back++] = job; //enqueue
                fprintf(trace_fp, "%d ARRIVE %s\n", tick, job->id); 
            }
        }

        // Dispatch when CPU is idle and ready queue isn't empty
        if(cpu_job == NULL && rq.front != rq.back){
            cpu_job = rq.data[rq.front++]; // Dispatch and dequeue
            if (rq.front == rq.back) {
                rq.front = 0;
                rq.back = 0;
            }
            cpu_job->total_wait_time += tick - cpu_job->ready_enqueue_time;
            cpu_job->state = JOB_RUNNING;
            if (!cpu_job->started) {
                cpu_job->first_run_time = tick;
                cpu_job->started = 1;
            }
            fprintf(trace_fp, "%d DISPATCH %s\n", tick, cpu_job->id); 
        }

        // Execute job
        if (cpu_job != NULL) {
            cpu_job->remaining_time--;
            if(cpu_job->remaining_time == 0){   
                cpu_job->completion_time = tick;
                fprintf(trace_fp, "%d COMPLETE %s\n", tick, cpu_job->id);
                completed_jobs++;
                cpu_job->state = JOB_DONE;
                cpu_job = NULL;
            }
        }
    
        tick++;
    }

    // Finish trace write and close
    fprintf(trace_fp, "END\n");

    // Write stats 
    dump_stats(wl, stats_fp);

    // Close files if not writing to stdout
    if (cfg->trace_path != NULL) fclose(trace_fp);
    if (cfg->stats_path != NULL) fclose(stats_fp);

    return 0;   
}

int RR(const sim_config_t *cfg, workload_t *wl){
    FILE *trace_fp = stdout;
    FILE *stats_fp = stdout;

    // Output trace to file path when provided
    if (cfg->trace_path != NULL){
        trace_fp = fopen(cfg->trace_path,"w");
        if(trace_fp == NULL){
            perror("Couldn't open trace file");
            return -1;
        }
    }

    // Output stats to file path when provided
    if (cfg->stats_path != NULL){
        stats_fp = fopen(cfg->stats_path, "w");
        if (stats_fp == NULL) {
            if (cfg->trace_path != NULL) fclose(trace_fp);
            perror("Couldn't open stats file");
            return -1;
        }
    }

    int tick = 0;
    int completed_jobs = 0;
    int total_jobs = wl->njobs;
    queue_t rq = {0}; //ready queue
    job_t *cpu_job = NULL; //job currently running on CPU
    // Run until all jobs have completed
    while (completed_jobs < total_jobs) {

        // Admit jobs to the ready queue when their arrival time is reached 
        for(int i = 0; i < wl->njobs; i++){
            job_t *job = &wl->jobs[i];
            if(job->state == JOB_NEW && job->arrival_time == tick){
                job->state = JOB_READY;
                job->ready_enqueue_time = tick;
                if (rq.back - rq.front >= MAX_JOBS) {
                    fprintf(stderr, "Ready queue overflow\n");
                    if (cfg->trace_path != NULL) fclose(trace_fp);
                    if (cfg->stats_path != NULL) fclose(stats_fp);
                    return -1;
                }
                rq.data[rq.back++] = job; //enqueue
                fprintf(trace_fp, "%d ARRIVE %s\n", tick, job->id); 
            }
        }

        // Dispatch when CPU is idle and ready queue isn't empty
        if(cpu_job == NULL && rq.front != rq.back){
            cpu_job = rq.data[rq.front++]; // Dispatch and dequeue
            if (rq.front == rq.back) {
                rq.front = 0;
                rq.back = 0;
            }
            cpu_job->rr_ticks_used = 0;
            cpu_job->total_wait_time += tick - cpu_job->ready_enqueue_time;
            cpu_job->state = JOB_RUNNING;
            if (!cpu_job->started) {
                cpu_job->first_run_time = tick;
                cpu_job->started = 1;
            }
            fprintf(trace_fp, "%d DISPATCH %s\n", tick, cpu_job->id); 
        }

        // Execute job
        if (cpu_job != NULL) {
            cpu_job->remaining_time--;
            cpu_job->rr_ticks_used++;
            if(cpu_job->remaining_time == 0){   
                cpu_job->completion_time = tick;
                fprintf(trace_fp, "%d COMPLETE %s\n", tick, cpu_job->id);
                completed_jobs++;
                cpu_job->state = JOB_DONE;
                cpu_job = NULL;
            }
        }

        // Preempt if quantum expired
        if (cpu_job != NULL &&
            cpu_job->remaining_time > 0 &&   // don’t preempt if it just finished
            cpu_job->rr_ticks_used >= cfg->quantum &&
            rq.front != rq.back) {

            fprintf(trace_fp, "%d PREEMPT %s\n", tick, cpu_job->id);

            cpu_job->state = JOB_READY;
            cpu_job->ready_enqueue_time = tick + 1; // RR rule
            cpu_job->rr_ticks_used = 0;

            if (rq.back - rq.front >= MAX_JOBS) {
                fprintf(stderr, "Ready queue overflow\n");
                if (cfg->trace_path != NULL) fclose(trace_fp);
                if (cfg->stats_path != NULL) fclose(stats_fp);
                return -1;
            }
            rq.data[rq.back++] = cpu_job; //enqueue
            cpu_job = NULL;

        }
    
        tick++;
    }

    // Finish trace write and close
    fprintf(trace_fp, "END\n");

    // Write stats 
    dump_stats(wl, stats_fp);

    // Close files if not writing to stdout
    if (cfg->trace_path != NULL) fclose(trace_fp);
    if (cfg->stats_path != NULL) fclose(stats_fp);

    return 0;   
}

int run_scheduler_single_cpu(const sim_config_t *cfg) {
    (void)cfg;
    // fprintf(stderr,
    //         "TODO: implement the single-CPU scheduler in src/scheduler.c\n"
    //         "Suggested order:\n"
    //         "- parse jobs in the format JOB_ID ARRIVAL PRIORITY CPU_TIME\n"
    //         "- implement FCFS for --cpus 1\n"
    //         "- add SJF, SRTF, and RR\n"
    //         "- verify trace and stats output\n");

    // Initialize workload then parse jobs
    workload_t wl = {0};
    wl.jobs = calloc(MAX_JOBS, sizeof(job_t));
    if (wl.jobs == NULL) {
        perror("calloc");
        return -1;
    }

    if(parse_jobs(cfg->input_path, &wl) != 0) {
        fprintf(stderr, "Error parsing jobs\n");
        return -1;
    }

    // // Remove this later but used to show jobs after parsing
    // for (int i = 0; i < wl.njobs; i++) {
    //     printf("Job %s: arrival=%d priority=%d total_time=%d\n",
    //            wl.jobs[i].id, wl.jobs[i].arrival_time, wl.jobs[i].priority, wl.jobs[i].total_time);
    // }

    int result = 0;
    // Print starting message at tick 0
    printf("Starting simulation with policy %s\n", policy_name(cfg->policy));

    // FCFS scheduling
    if (strcmp(policy_name(cfg->policy),"FCFS") == 0){
        result = fcfs(cfg, &wl);
    }

    // RR scheduling
    else if (strcmp(policy_name(cfg->policy),"RR") == 0){
        result = RR(cfg, &wl);
    }

    // SJF scheduling
    else if (strcmp(policy_name(cfg->policy),"SJF") == 0){
        // Implemement SJF scheduling - Brandon
    }

    // SRTF scheduling
    else{
        // Implemement SRTF scheduling - Brandon
    }

    // Verify trace and stats output - Franky & Brandon

    free(wl.jobs);
    return result;
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
    
    // Implement multi-CPU threaded scheduler - IDK

    return -1;
}