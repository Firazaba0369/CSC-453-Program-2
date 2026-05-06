#include <stdio.h>
#include <sys/stat.h>
#include <string.h>
#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>

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
        (double)total_response_time / wl->njobs, (double)total_turnaround_time / wl->njobs, 
        (double)total_waiting_time / wl->njobs);

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

int sjf(const sim_config_t *cfg, workload_t *wl){
    // Implementation for SJF scheduling
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

    job_t *cpu_job = NULL;

    // Run until all jobs have completed
    while (completed_jobs < total_jobs) {

        // Admit new arrival
        for(int i = 0; i < wl->njobs; i++){
            job_t *job = &wl->jobs[i];
            if(job->state == JOB_NEW && job->arrival_time == tick){
                job->state = JOB_READY;
                job->ready_enqueue_time = tick;
                fprintf(trace_fp, "%d ARRIVE %s\n", tick, job->id);
            }
        }

        // if CPU is idle, dispatch the shortest job in ready state
        if(cpu_job == NULL){
            job_t *shortest_job = NULL;

            for(int i = 0; i < wl->njobs; i++){
                job_t *job = &wl->jobs[i];
                
                if (job->state != JOB_READY) continue;

                if (shortest_job == NULL) {
                    shortest_job = job;
                    continue;
                }

                if (job->remaining_time < shortest_job->remaining_time) {
                    shortest_job = job;
                } else if (job->remaining_time == shortest_job->remaining_time) {
                    if(job->arrival_time < shortest_job->arrival_time) {
                        shortest_job = job;
                    } else if (job->arrival_time == shortest_job->arrival_time) {
                        if (job->priority > shortest_job->priority) {
                            shortest_job = job;
                        } else if (job->priority == shortest_job->priority) {
                            if (strcmp(job->id, shortest_job->id) < 0) {
                                shortest_job = job;
                            } 
                        }
                    }
                }
            }
            if(shortest_job != NULL){
                cpu_job = shortest_job;
                cpu_job->total_wait_time += tick - cpu_job->ready_enqueue_time;
                cpu_job->state = JOB_RUNNING;
                if (!cpu_job->started) {
                    cpu_job->first_run_time = tick;
                    cpu_job->started = 1;
                }
                fprintf(trace_fp, "%d DISPATCH %s\n", tick, cpu_job->id); 
            }
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

int srtf(const sim_config_t *cfg, workload_t *wl){
    // Implementation for SRTF scheduling
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

    job_t *cpu_job = NULL;

    // Run until all jobs have completed
    while (completed_jobs < total_jobs) {

        // Admit new arrival
        for(int i = 0; i < wl->njobs; i++){
            job_t *job = &wl->jobs[i];
            if(job->state == JOB_NEW && job->arrival_time == tick){
                job->state = JOB_READY;
                job->ready_enqueue_time = tick;
                fprintf(trace_fp, "%d ARRIVE %s\n", tick, job->id);
            }
        }

        // select best job every tick
        job_t *shortest_job = NULL;
        for(int i = 0; i < wl->njobs; i++){
            job_t *job = &wl->jobs[i];
            
            if(job->state != JOB_READY && job != cpu_job) continue;

            if (shortest_job == NULL) {
                shortest_job = job;
                continue;
            }

            // tie breaking
            if (job->remaining_time < shortest_job->remaining_time) {
                shortest_job = job;
            } else if (job->remaining_time == shortest_job->remaining_time) {
                if (job->arrival_time < shortest_job->arrival_time) {
                    shortest_job = job;
                } else if (job->arrival_time == shortest_job->arrival_time) {
                    if (job->priority > shortest_job->priority) {
                        shortest_job = job;
                    } else if (job->priority == shortest_job->priority) {
                        if (strcmp(job->id, shortest_job->id) < 0) {
                            shortest_job = job;
                        } 
                    }
                }
            }
        }
        if(shortest_job != cpu_job){
            if (cpu_job != NULL) {
                cpu_job->state = JOB_READY;
                cpu_job->ready_enqueue_time = tick;
                fprintf(trace_fp, "%d PREEMPT %s\n", tick, cpu_job->id);
            }

            cpu_job = shortest_job;
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


static void rq_enqueue(queue_t *q, job_t *job) {
    q->data[q->back++] = job;
}

static job_t *rq_dequeue(queue_t *q) {
    if (q->front == q->back) return NULL;
    job_t *j = q->data[q->front++];
    if (q->front == q->back) {
        q->front = q->back = 0;
    }
    return j;
}

static int rq_empty(queue_t *q) {
    return q->front == q->back;
}

// Return 1 if a is strictly better than b under SJF/SRTF rules
static int better_job(const job_t *a, const job_t *b) {
    if (a->remaining_time != b->remaining_time)
        return a->remaining_time < b->remaining_time;
    if (a->arrival_time != b->arrival_time)
        return a->arrival_time < b->arrival_time;
    if (a->priority != b->priority)
        return a->priority > b->priority;
    return strcmp(a->id, b->id) < 0;
}

// Pop best job from ready queue for SJF/SRTF
static job_t *rq_pop_best(queue_t *q) {
    if (rq_empty(q)) return NULL;

    int best_idx = q->front;
    for (int i = q->front + 1; i < q->back; i++) {
        if (better_job(q->data[i], q->data[best_idx])) {
            best_idx = i;
        }
    }

    job_t *best = q->data[best_idx];

    for (int i = best_idx; i < q->back - 1; i++) {
        q->data[i] = q->data[i + 1];
    }
    q->back--;
    if (q->front == q->back) {
        q->front = q->back = 0;
    }

    return best;
}



void *cpu_worker(void *arg) {
    threadArgs_t *targ = (threadArgs_t *)arg;
    shared_t *s = targ->shared;
    int id = targ->thread_id;
    int my_tick = 0;

    pthread_mutex_lock(&s->mutex);

    while (1) {
        // Wait until scheduler starts a new tick or shuts down
        while (my_tick == s->tick && !s->shutdown) {
            pthread_cond_wait(&s->worker_cv, &s->mutex);
        }

        if (s->shutdown) {
            pthread_mutex_unlock(&s->mutex);
            return NULL;
        }

        // Take assigned work
        job_t *job = s->cpu_jobs[id];
        my_tick = s->tick;

        pthread_mutex_unlock(&s->mutex);

        if (job != NULL) {
            job->remaining_time--;
            job->rr_ticks_used++;
        }

        pthread_mutex_lock(&s->mutex);

        // Update shared state and signal scheduler if last worker done
        s->workers_done++;
        if (s->workers_done == s->cfg->cpus) {
            pthread_cond_signal(&s->scheduler_cv);
        }

        // Loop back - will sleep on worker_cv until next tick
    }
}

void *schedule_worker(void *arg) {
    shared_t *s = (shared_t *)arg;
    const sim_config_t *cfg = s->cfg;
    workload_t *wl = s->wl;
    int total_jobs = wl->njobs;

    pthread_mutex_lock(&s->mutex);

    while (s->completed_jobs < total_jobs) {
        int tick = s->tick;

        /* 1. Admit arrivals */
        for (int i = 0; i < wl->njobs; i++) {
            job_t *job = &wl->jobs[i];
            if (job->state == JOB_NEW && job->arrival_time == tick) {
                job->state = JOB_READY;
                job->ready_enqueue_time = tick;
                rq_enqueue(&s->ready_queue, job);
                fprintf(s->trace_fp, "%d ARRIVE %s\n", tick, job->id);
            }
        }

        /* 2. SRTF preemption check (in CPU order) */
        if (cfg->policy == POLICY_SRTF) {
            for (int c = 0; c < cfg->cpus; c++) {
                job_t *running = s->cpu_jobs[c];
                if (running == NULL) continue;

                job_t *best = NULL;
                for (int i = s->ready_queue.front; i < s->ready_queue.back; i++) {
                    job_t *cand = s->ready_queue.data[i];
                    if (best == NULL || better_job(cand, best)) best = cand;
                }

                if (best != NULL && better_job(best, running)) {
                    running->state = JOB_READY;
                    running->ready_enqueue_time = tick;
                    rq_enqueue(&s->ready_queue, running);
                    fprintf(s->trace_fp, "%d PREEMPT CPU%d %s\n", tick, c, running->id);
                    s->cpu_jobs[c] = NULL;
                }
            }
        }

        /* 3. Dispatch to idle CPUs (in CPU order) */
        for (int c = 0; c < cfg->cpus; c++) {
            if (s->cpu_jobs[c] != NULL) continue;
            if (rq_empty(&s->ready_queue)) break;

            job_t *job = (cfg->policy == POLICY_FCFS || cfg->policy == POLICY_RR)
                         ? rq_dequeue(&s->ready_queue)
                         : rq_pop_best(&s->ready_queue);

            s->cpu_jobs[c] = job;
            job->state = JOB_RUNNING;
            job->total_wait_time += tick - job->ready_enqueue_time;
            job->assigned_cpu = c;
            if (!job->started) {
                job->first_run_time = tick;
                job->started = 1;
            }
            if (cfg->policy == POLICY_RR) job->rr_ticks_used = 0;

            fprintf(s->trace_fp, "%d DISPATCH CPU%d %s\n", tick, c, job->id);
        }

        /* 4. Signal workers — incrementing tick is the signal */
        s->workers_done = 0;
        s->tick++;
        pthread_cond_broadcast(&s->worker_cv); // wake all workers

        /* 5. Wait for all workers to finish */
        while (s->workers_done < cfg->cpus) {
            pthread_cond_wait(&s->scheduler_cv, &s->mutex);
        }
        // All workers done — safe to read job fields now

        /* 6. Process completions and RR quantum expiry (in CPU order) */
        for (int c = 0; c < cfg->cpus; c++) {
            job_t *job = s->cpu_jobs[c];
            if (job == NULL) continue;

            if (job->remaining_time == 0) {
                job->completion_time = tick;
                job->state = JOB_DONE;
                fprintf(s->trace_fp, "%d COMPLETE CPU%d %s\n", tick, c, job->id);
                s->cpu_jobs[c] = NULL;
                s->completed_jobs++;
                continue;
            }

            if (cfg->policy == POLICY_RR &&
                job->rr_ticks_used >= cfg->quantum &&
                !rq_empty(&s->ready_queue)) {
                fprintf(s->trace_fp, "%d PREEMPT CPU%d %s\n", tick, c, job->id);
                job->state = JOB_READY;
                job->ready_enqueue_time = tick + 1;
                job->rr_ticks_used = 0;
                rq_enqueue(&s->ready_queue, job);
                s->cpu_jobs[c] = NULL;
            }
        }
        // tick already incremented in step 4
    }

    /* Shutdown */
    s->shutdown = 1;
    pthread_cond_broadcast(&s->worker_cv);
    pthread_mutex_unlock(&s->mutex);

    fprintf(s->trace_fp, "END\n");
    dump_stats(wl, s->stats_fp);

    return NULL;
}

int run_scheduler_single_cpu(const sim_config_t *cfg) {
    (void)cfg;

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

    int result = 0;

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
        result = sjf(cfg, &wl);
    }

    // SRTF scheduling
    else{
        // Implemement SRTF scheduling - Brandon
        result = srtf(cfg, &wl);
    }

    free(wl.jobs);
    return result;
}

int run_scheduler_multi_cpu(const sim_config_t *cfg) {
    // Initialize workload and parse jobs
    workload_t wl = {0};
    wl.jobs = calloc(MAX_JOBS, sizeof(job_t));
    if (wl.jobs == NULL) {
        perror("calloc");
        return -1;
    }

    if (parse_jobs(cfg->input_path, &wl) != 0) {
        fprintf(stderr, "Error parsing jobs\n");
        free(wl.jobs);
        return -1;
    }

    // Initialize shared state
    shared_t s = {0};
    s.cfg = cfg;
    s.wl = &wl;
    s.cpu_jobs = calloc(cfg->cpus, sizeof(job_t *));
    if (s.cpu_jobs == NULL) {
        perror("calloc");
        free(wl.jobs);
        return -1;
    }
    s.completed_jobs = 0;
    s.shutdown = 0;
    s.tick = 0;
    s.workers_done = 0;
    s.tick_active = 0;

    // Open trace file
    s.trace_fp = stdout;
    if (cfg->trace_path != NULL) {
        s.trace_fp = fopen(cfg->trace_path, "w");
        if (s.trace_fp == NULL) {
            perror("Couldn't open trace file");
            free(wl.jobs);
            free(s.cpu_jobs);
            return -1;
        }
    }

    // Open stats file
    s.stats_fp = stdout;
    if (cfg->stats_path != NULL) {
        s.stats_fp = fopen(cfg->stats_path, "w");
        if (s.stats_fp == NULL) {
            perror("Couldn't open stats file");
            if (cfg->trace_path != NULL) fclose(s.trace_fp);
            free(wl.jobs);
            free(s.cpu_jobs);
            return -1;
        }
    }

    // Initialize synchronization primitives
    pthread_mutex_init(&s.mutex, NULL);
    pthread_cond_init(&s.worker_cv, NULL);
    pthread_cond_init(&s.scheduler_cv, NULL);

    // Create CPU worker threads
    pthread_t cpu_threads[cfg->cpus];
    threadArgs_t args[cfg->cpus];
    for (int i = 0; i < cfg->cpus; i++) {
        args[i].shared = &s;
        args[i].thread_id = i;
        if (pthread_create(&cpu_threads[i], NULL, cpu_worker, &args[i]) != 0) {
            perror("pthread_create cpu_worker");
            // shutdown any already-created threads
            pthread_mutex_lock(&s.mutex);
            s.shutdown = 1;
            pthread_cond_broadcast(&s.worker_cv);
            pthread_mutex_unlock(&s.mutex);
            for (int j = 0; j < i; j++) pthread_join(cpu_threads[j], NULL);
            if (cfg->trace_path != NULL) fclose(s.trace_fp);
            if (cfg->stats_path != NULL) fclose(s.stats_fp);
            free(s.cpu_jobs);
            free(wl.jobs);
            return -1;
        }
    }

    // Create scheduler thread
    pthread_t scheduler_thread;
    if (pthread_create(&scheduler_thread, NULL, schedule_worker, &s) != 0) {
        perror("pthread_create schedule_worker");
        pthread_mutex_lock(&s.mutex);
        s.shutdown = 1;
        pthread_cond_broadcast(&s.worker_cv);
        pthread_mutex_unlock(&s.mutex);
        for (int i = 0; i < cfg->cpus; i++) pthread_join(cpu_threads[i], NULL);
        if (cfg->trace_path != NULL) fclose(s.trace_fp);
        if (cfg->stats_path != NULL) fclose(s.stats_fp);
        free(s.cpu_jobs);
        free(wl.jobs);
        return -1;
    }

    // Wait for scheduler to finish
    pthread_join(scheduler_thread, NULL);

    // Tell workers to shut down and wake them
    pthread_mutex_lock(&s.mutex);
    s.shutdown = 1;
    pthread_cond_broadcast(&s.worker_cv);
    pthread_mutex_unlock(&s.mutex);

    // Wait for all CPU workers to finish
    for (int i = 0; i < cfg->cpus; i++) {
        pthread_join(cpu_threads[i], NULL);
    }

    // Cleanup
    pthread_mutex_destroy(&s.mutex);
    pthread_cond_destroy(&s.worker_cv);
    pthread_cond_destroy(&s.scheduler_cv);

    if (cfg->trace_path != NULL) fclose(s.trace_fp);
    if (cfg->stats_path != NULL) fclose(s.stats_fp);

    free(s.cpu_jobs);
    free(wl.jobs);

    return 0;

}