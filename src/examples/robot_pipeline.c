/*
 * MPMC Robot Pipeline Example
 * 
 * Demonstrates a sensor fusion pipeline using MPMC partitions:
 *   Sensor → Filter → Estimator → Controller
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>
#include <stdbool.h>
#include <stdatomic.h>
#include "mpmc.h"

/* Pipeline partitions */
#define PART_SENSOR    0
#define PART_FILTERED  1
#define PART_CONTROL   2
#define NUM_PARTITIONS 3

/* Message types */
typedef struct {
    uint64_t timestamp_ns;
    double accel[3];
    double gyro[3];
    uint64_t seq;
} sensor_msg_t;

typedef struct {
    uint64_t timestamp_ns;
    double position[3];
    double velocity[3];
    uint64_t seq;
} state_msg_t;

typedef struct {
    uint64_t timestamp_ns;
    double thrust;
    double torque[3];
    uint64_t seq;
} control_msg_t;

static mpmc_t queue;
static _Atomic bool running = true;
static _Atomic uint64_t sensor_count = 0;
static _Atomic uint64_t control_count = 0;

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

/* Stage 1: Sensor (100 Hz) */
void* sensor_thread(void* arg) {
    (void)arg;
    uint64_t seq = 0;
    
    while (running) {
        sensor_msg_t* slot = (sensor_msg_t*)mpmc_reserve(&queue, PART_SENSOR);
        if (slot) {
            slot->timestamp_ns = now_ns();
            slot->accel[0] = 0.1  * (rand() % 100 - 50);
            slot->accel[1] = 0.1  * (rand() % 100 - 50);
            slot->accel[2] = 9.81 + 0.01    * (rand() % 100 - 50);
            slot->gyro[0]  = 0.01 * (rand() % 100 - 50);
            slot->gyro[1]  = 0.01 * (rand() % 100 - 50);
            slot->gyro[2]  = 0.01 * (rand() % 100 - 50);
            slot->seq = seq++;
            mpmc_submit(&queue, PART_SENSOR);
            atomic_fetch_add(&sensor_count, 1);
        }
        
        struct timespec sleep = {0, 10000000};  /* 10ms = 100Hz */
        nanosleep(&sleep, NULL);
    }
    return NULL;
}

/* Stage 2: Filter (reads sensor, writes filtered state) */
void* filter_thread(void* arg) {
    (void)arg;
    double pos[3] = {0}, vel[3] = {0};
    
    while (running) {
        mpmc_item_t item = mpmc_claim_partition(&queue, PART_SENSOR);
        if (item.data) {
            sensor_msg_t* sensor = (sensor_msg_t*)item.data;
            
            /* Simple integration */
            double dt = 0.01;
            for (int i = 0; i < 3; i++) {
                vel[i] += sensor->accel[i] * dt;
                pos[i] += vel[i] * dt;
            }
            
            /* Publish filtered state */
            state_msg_t* out = (state_msg_t*)mpmc_reserve(&queue, PART_FILTERED);
            if (out) {
                out->timestamp_ns = sensor->timestamp_ns;
                memcpy(out->position, pos, sizeof(pos));
                memcpy(out->velocity, vel, sizeof(vel));
                out->seq = sensor->seq;
                mpmc_submit(&queue, PART_FILTERED);
            }
            
            mpmc_release(&queue, &item);
        } else {
            sched_yield();
        }
    }
    return NULL;
}

/* Stage 3: Controller (reads state, writes control) */
void* controller_thread(void* arg) {
    (void)arg;
    double target_z = 1.0;
    
    while (running) {
        mpmc_item_t item = mpmc_claim_partition(&queue, PART_FILTERED);
        if (item.data) {
            state_msg_t* state = (state_msg_t*)item.data;
            
            /* Simple PD controller for altitude */
            double error_z = target_z - state->position[2];
            double error_vz = -state->velocity[2];
            
            control_msg_t* out = (control_msg_t*)mpmc_reserve(&queue, PART_CONTROL);
            if (out) {
                out->timestamp_ns = state->timestamp_ns;
                out->thrust    = 9.81 + 2.0 * error_z + 1.0 * error_vz;
                out->torque[0] = 0;
                out->torque[1] = 0;
                out->torque[2] = 0;
                out->seq = state->seq;
                mpmc_submit(&queue, PART_CONTROL);
                atomic_fetch_add(&control_count, 1);
            }
            
            mpmc_release(&queue, &item);
        } else {
            sched_yield();
        }
    }
    return NULL;
}

/* Stage 4: Actuator (reads control, applies) */
void* actuator_thread(void* arg) {
    (void)arg;
    
    while (running) {
        mpmc_item_t item = mpmc_claim_partition(&queue, PART_CONTROL);
        if (item.data) {
            /* In real system: send to hardware */
            mpmc_release(&queue, &item);
        } else {
            sched_yield();
        }
    }
    return NULL;
}

int main(void) {
    printf("=== MPMC Robot Pipeline Example ===\n\n");
    
    /* Slot size must fit largest message type (sensor_msg_t = 72 bytes) */
    if (mpmc_init(&queue, NUM_PARTITIONS, 128, sizeof(sensor_msg_t)) != 0) {
        fprintf(stderr, "Failed to initialize queue\n");
        return 1;
    }
    
    pthread_t threads[4];
    pthread_create(&threads[0], NULL, sensor_thread,     NULL);
    pthread_create(&threads[1], NULL, filter_thread,     NULL);
    pthread_create(&threads[2], NULL, controller_thread, NULL);
    pthread_create(&threads[3], NULL, actuator_thread,   NULL);
    
    printf("Pipeline running... (5 seconds)\n");
    sleep(5);
    
    running = false;
    for (int i = 0; i < 4; i++) {
        pthread_join(threads[i], NULL);
    }
    
    printf("\nResults:\n");
    printf("  Sensor messages:  %llu\n",   (unsigned long long)sensor_count);
    printf("  Control commands: %llu\n",   (unsigned long long)control_count);
    printf("  Throughput: %.0f msg/sec\n", (double)control_count / 5.0);
    
    mpmc_destroy(&queue);
    return 0;
}
