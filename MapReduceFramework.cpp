
#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include <pthread.h>
#include "Barrier.h"
#include <atomic>
#include <vector>
#include <algorithm>
#include <semaphore.h>
#include <map>
#include <cstdio>
#include <iostream>



#define SYSTEM_ERROR "system error: cannot create thread\n"


#define INCREASE_COUNTER 0x7ffffffful //0x7FFFFFF
#define INCREASE_PROCESSED 1UL<<31
#define RESET_COUNTER 0xffffffff80000000ul
#define RESET_PROCESS 0xc00000007ffffffful

#define RESET_ATOMIC 0x0000000000000003ul
#define INC_PROCESSED 0x80000000
#define PROCESSED 0x3FFFFFFF80000000


struct Job_context;

struct Thread_context{
    int tid;
    Job_context *job_context;
};


// bool compare_keys(const IntermediatePair &a, IntermediatePair &b) {
//     return *(a.first) < *(b.first);
// }

struct Compare {
    bool operator()(const IntermediatePair &first, const  IntermediatePair &sec) const{
        return *(first.first) < *(sec.first);
    }
};


struct Job_context{
    pthread_t *threads;
    JobState state {UNDEFINED_STAGE,0.0};
    // IntermediateVec inter_vec;


    InputVec input_vec;
    OutputVec* output_vec;

    int levels;
    Barrier *barrier;
    Thread_context* thread_contexts;
    const MapReduceClient *client;
    std::map< IntermediatePair, std::vector<IntermediatePair>, Compare> inter_map;
    std::vector<IntermediateVec* > vec_of_inter_vecs;

    std::atomic<uint64_t> atomic_counter;
    bool flag;

    sem_t map_semaphore;
    sem_t reduce_semaphore;
    sem_t shuffle_sem;
    sem_t wait_sem;
    sem_t state_sem;
    sem_t emit3_sem;

    ~Job_context();
};

Job_context::~Job_context() {
    sem_destroy(&map_semaphore);
    sem_destroy(&reduce_semaphore);
    sem_destroy(&shuffle_sem);
    sem_destroy(&wait_sem);
    sem_destroy(&state_sem);
    sem_destroy(&emit3_sem);
    delete [] threads;
    delete [] thread_contexts;
    delete barrier;

}


////helper functions////

void mapPhase(void* arg) {


    Thread_context* thread_context = static_cast<Thread_context*>(arg);
    Job_context* job_context = thread_context->job_context;

    while (true) {
       uint64_t counter = job_context->atomic_counter.fetch_add(1);
        uint64_t index = counter & INCREASE_COUNTER;

        if (index >= job_context->input_vec.size()) {
            break;
        }

        sem_wait(&job_context->map_semaphore);

        const auto& inputPair = job_context->input_vec[index];
        job_context->client->map(inputPair.first, inputPair.second, job_context);
        job_context->atomic_counter += INCREASE_PROCESSED;

        sem_post(&job_context->map_semaphore);
    }

    // Update the job state to the next stage if needed
    if (job_context->state.stage == MAP_STAGE) {
        sem_wait(&job_context->state_sem);
        job_context->state.stage = SHUFFLE_STAGE;
        job_context->state.percentage = 0;
        sem_post(&job_context->state_sem);
    }
}


void shuffle_phase(void* arg) {
    Thread_context *thread_context = (Thread_context *) arg;
    uint64_t val = (thread_context->job_context->atomic_counter).load() & RESET_PROCESS;
    thread_context->job_context->atomic_counter = val;

    thread_context->job_context->state.stage = SHUFFLE_STAGE;
    thread_context->job_context->atomic_counter = ((uint64_t) 1) << 63;
    for(auto &pair: thread_context->job_context->inter_map) {
        // printf("%s\n",(pair.first.first));
        thread_context->job_context->vec_of_inter_vecs.push_back(&pair.second);
    }

}

void reduce_phase(void * arg) {
    Thread_context* thread_context = static_cast<Thread_context*>(arg);
    Job_context* job_context = thread_context->job_context;


    while (true) {

        uint64_t counter = job_context->atomic_counter.fetch_add(1);
        uint64_t index = counter & INCREASE_COUNTER;

        if (index >= job_context->vec_of_inter_vecs.size()) {
            break;
        }
        // auto a = *job_context->vec_of_inter_vecs.at(index);
        sem_wait(&job_context->reduce_semaphore);

        job_context->client->reduce(job_context->vec_of_inter_vecs.at(index), job_context);
        job_context->atomic_counter+= INCREASE_PROCESSED;

        sem_post(&job_context->reduce_semaphore);
    }
    thread_context->job_context->barrier->barrier();


}





void* map_reduce(void* arg){
    Thread_context* thread_context = static_cast<Thread_context*>(arg);

    mapPhase(arg);
    thread_context->job_context->barrier->barrier();

    // sem_wait(&thread_context->job_context->shuffle_sem);
    // std::sort(thread_context->job_context->inter_vec.begin(),
    //     thread_context->job_context->inter_vec.end(),compare_keys);

    if(thread_context->tid==0) {
    shuffle_phase(arg);
    //
    thread_context->job_context->state.stage=REDUCE_STAGE;
    //
    //     //reseting the atomic counter
    uint64_t val = (thread_context->job_context->atomic_counter).load() & RESET_ATOMIC;
    thread_context->job_context->atomic_counter = val;
    thread_context->job_context->atomic_counter= ((uint64_t) 3) << 62;
    thread_context->job_context->state.percentage = 0.0f;
    }
    // sem_post(&thread_context->job_context->shuffle_sem);
    //

    thread_context->job_context->barrier->barrier();

    reduce_phase(arg);

    return nullptr;
}

////API functions////
//todo craete job
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){

    auto *job = new Job_context;
    job->threads = new pthread_t[multiThreadLevel];
    job->barrier= new Barrier(multiThreadLevel);
    job->atomic_counter = inputVec.size();

    //job->state.stage= UNDEFINED_STAGE;
    job->thread_contexts=new Thread_context[multiThreadLevel];
    job->levels = multiThreadLevel;
    job->input_vec = inputVec ;
    job->output_vec= &outputVec;
    job->client= &client;
    job->atomic_counter=(uint64_t) 1 << 62;

    job->state.stage= MAP_STAGE;
    job->flag=false;


    sem_init(&job->map_semaphore, 0, 1);
    sem_init(&job->reduce_semaphore, 0, 1);
    sem_init(&job->wait_sem, 0, 1);
    sem_init(&job->state_sem, 0, 1);
    sem_init(&job->shuffle_sem, 0, 1);
    sem_init(&job->emit3_sem, 0, 1);
    for(int i=0; i<multiThreadLevel ; i++){
        job->thread_contexts[i]={i,job};
        if(pthread_create(&job->threads[i], nullptr,map_reduce,job->thread_contexts+i)!=0){
            printf(SYSTEM_ERROR);
        }
    }
    return job;
}



void emit2 (K2* key, V2* value, void* context) {
    auto job_context = (Job_context*) context;
    auto pair = IntermediatePair(key, value);
    auto it = job_context->inter_map.find(pair);
    if(it == job_context->inter_map.end()) {
        std::vector<IntermediatePair> vec;
        job_context->inter_map[pair] = vec;
    }
    job_context->inter_map[pair].push_back(pair);

}


void emit3 (K3* key, V3* value, void* context) {
    auto job_context = (Job_context*) context;
    job_context->output_vec->emplace_back(key, value);

}

void waitForJob(JobHandle job) {
    auto* job_context= (Job_context*) job;
    if(job_context->flag) {
        return;
    }
    job_context->flag=true;

    sem_wait(&job_context->wait_sem);
    for (int i = 0; i < job_context->levels; ++i) {
        if (pthread_join(job_context->threads[i], nullptr) != 0) {
            printf(SYSTEM_ERROR);
            exit(EXIT_FAILURE);
        }
    }
    sem_post(&job_context->wait_sem);
}



// this function gets a JobHandle and updates the state of the job into the  given JobState struct (from the pdf)
 void getJobState(JobHandle job, JobState* state) {
     auto *job_context = (Job_context *) job;
     sem_wait(&job_context->state_sem);

     unsigned long counter = (job_context->atomic_counter.load());
     unsigned long processed = (counter & PROCESSED) >> 31; // taking the 31 in thr middle
     stage_t stage = static_cast<stage_t>(counter >> 62);
     job_context->state.stage = stage;
     unsigned long size = 0;

     switch (stage) {

         case UNDEFINED_STAGE:
             state->percentage = job_context->state.percentage = 0;
             state->stage = UNDEFINED_STAGE;
             sem_post(&job_context->state_sem);
             return;

         case MAP_STAGE:
             size = job_context->input_vec.size();
             state->stage= MAP_STAGE;
             break;

         case SHUFFLE_STAGE:
             size = job_context->inter_map.size();
             state->stage= SHUFFLE_STAGE;
             break;

         case REDUCE_STAGE:
             size = job_context->vec_of_inter_vecs.size();
             state->stage= REDUCE_STAGE;
             break;
     }

     if (size == 0 ){
         job_context->state.percentage = state->percentage = 0;
     } else {
         job_context->state.percentage = state->percentage = ((float) processed / (float) size) * 100;
     }

     sem_post(&job_context->state_sem);
 }


void closeJobHandle(JobHandle job) {
    auto job_context= (Job_context*) job;
    if(!job_context->flag) {
        waitForJob(job);
    }
    delete job_context;
}