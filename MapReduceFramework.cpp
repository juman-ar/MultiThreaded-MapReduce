
#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include <pthread.h>
#include "Barrier.h"
#include <atomic>
#include <iostream>
#include <vector>
#include <algorithm>
#include <semaphore.h>
#include <set>

#define SYSTEM_ERROR "system error: cannot create thread\n"


#define INDEX 0x7FFFFFF
#define INC_PROCESSED 0x80000000
#define PROCESSED 0x3FFFFFFF80000000


struct Job_context;

struct Thread_context{
    int tid;
    Job_context *job_context;
};

struct Compare {
    bool operator()(const IntermediatePair &first, const  IntermediatePair &sec) const{
        return *(first.first) < *(sec.first);
    }
};



struct Job_context{
    pthread_t *threads;
    JobState state {UNDEFINED_STAGE,0.0};
    //IntermediateVec intermediate_vec;
    std::set<IntermediatePair, Compare> inter_set;

    InputVec input_vec;
    OutputVec output_vec;
    int levels;
    Barrier *barrier;
   Thread_context* thread_contexts;
    const MapReduceClient *client;
    std::vector<IntermediateVec> vec_of_inter_vecs;

    std::atomic<uint64_t> atomic_counter;
    bool flag;


    // pthread_mutex_t map_mutex = PTHREAD_MUTEX_INITIALIZER;
    // pthread_mutex_t reduce_mutex= PTHREAD_MUTEX_INITIALIZER;

    sem_t map_semaphore;
    sem_t reduce_semaphore;
    sem_t shuffle_sem;
    sem_t wait_sem;
    sem_t state_sem;


    ~Job_context();
};

Job_context::~Job_context() {
    // pthread_mutex_destroy(&outputMutex);
    // pthread_mutex_destroy(&intermediateMutex);
    // pthread_mutex_destroy(&holdMutex);
    sem_destroy(&map_semaphore);
    sem_destroy(&reduce_semaphore);
    sem_destroy(&shuffle_sem);
    sem_destroy(&wait_sem);
    sem_destroy(&state_sem);
    delete [] threads;
    delete [] thread_contexts;
    //delete atomic_counter;
    delete barrier;

}


////helper functions////

void mapPhase(void* arg) {
//    Thread_context* thread_context = static_cast<Thread_context*>(arg);
//
////    if (thread_context->job_context->atomic_counter == nullptr) {
////        // Handle error or initialize atomic_counter
////        thread_context->job_context->atomic_counter = new std::atomic<uint64_t>(0);
////    }
//
//    //*(((thread_context->job_context->atomic_counter))++) & INDEX;
//    for (const auto& inputPair : thread_context->job_context->input_vec) {
//        // uint64_t pair_index = ((*(counter))++) & INDEX;
//
//        sem_wait(&thread_context->job_context->map_semaphore);
//
//        // momken bdl jobcontext tkon intermediate vec
//        thread_context->job_context->client->map(inputPair.first, inputPair.second, thread_context->job_context);
//        thread_context->job_context->atomic_counter += INC_PROCESSED;
//        (((thread_context->job_context->atomic_counter))++) & INDEX;
//
//        sem_post(&thread_context->job_context->map_semaphore);
//    }
    Thread_context* thread_context = static_cast<Thread_context*>(arg);
    Job_context* job_context = thread_context->job_context;

    while (true) {
        // Atomically fetch and increment the counter
        uint64_t counter = job_context->atomic_counter.fetch_add(1);
        uint64_t index = counter & INDEX;

        // Check if we've processed all input pairs
        if (index >= job_context->input_vec.size()) {
            break;
        }



        // Process the input pair
        const auto& inputPair = job_context->input_vec[index];

        // Ensure proper synchronization
        sem_wait(&job_context->map_semaphore);

        job_context->client->map(inputPair.first, inputPair.second, job_context);

        // Increment processed counter atomically
        job_context->atomic_counter.fetch_add(INC_PROCESSED);

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
    Thread_context* thread_context = (Thread_context*) arg;
    thread_context->job_context->state.stage=SHUFFLE_STAGE;
    thread_context->job_context->atomic_counter=((uint64_t) 1) << 63;

    IntermediateVec new_vec;
    auto& inter_set = thread_context->job_context->inter_set;

    if (!inter_set.empty()) {
        auto lastElement = *inter_set.rbegin(); // Copy the last element
        thread_context->job_context->vec_of_inter_vecs.push_back(new_vec);
        for (auto i = inter_set.rbegin(); i != inter_set.rend(); ++i) {
            if (!(*(lastElement.first) < *(i->first) ||  *(i->first) < *(lastElement.first))) {

                thread_context->job_context->vec_of_inter_vecs.back().push_back(*i);
                thread_context->job_context->atomic_counter += INC_PROCESSED;
            } else {
                lastElement = *i;

                IntermediateVec v;
                v.push_back(*i);
                thread_context->job_context->vec_of_inter_vecs.push_back(v);
                thread_context->job_context->atomic_counter += INC_PROCESSED; // TODO
            }
        }
    }
}

//void reduce_phase(void * arg) {
//    Thread_context* thread_context = (Thread_context*) arg;
//
//
//
//    sem_wait(&thread_context->job_context->reduce_semaphore);
//
//    int element = thread_context->job_context->atomic_counter++ & (INDEX);
//    while (element < thread_context->job_context->vec_of_inter_vecs.size()) {
//        thread_context->job_context->client->reduce(&thread_context->job_context->vec_of_inter_vecs.at(element)
//                ,thread_context);
//        thread_context->job_context->atomic_counter += (INC_PROCESSED);
//        element = thread_context->job_context->atomic_counter++ & INDEX;
//
//    }
//
//    sem_post(&thread_context->job_context->reduce_semaphore);
//}
void reduce_phase(void * arg) {
    Thread_context* thread_context = static_cast<Thread_context*>(arg);
    Job_context* job_context = thread_context->job_context;

    while (true) {
        // Atomically fetch and increment the counter
        uint64_t counter = job_context->atomic_counter.fetch_add(1);
        uint64_t index = counter & INDEX;

        // Check if we've processed all intermediate vectors
        if (index >= job_context->vec_of_inter_vecs.size()) {
            break;
        }

        // Process the intermediate vector
        sem_wait(&job_context->reduce_semaphore);

        job_context->client->reduce(&job_context->vec_of_inter_vecs.at(index), job_context);

        // Increment processed counter atomically
        job_context->atomic_counter.fetch_add(INC_PROCESSED);

        sem_post(&job_context->reduce_semaphore);
    }

    // Update the job state to the next stage if needed

}

// if(pthread_mutex_lock(&thread_context->job_context->reduce_mutex)!=0){
//     printf("ERROR");
//     printf("\n");
//     exit(EXIT_FAILURE);
// }
//
//
// if(pthread_mutex_unlock(&thread_context->job_context->reduce_mutex)!=0){
//     printf("ERROR");//
//     printf("\n");
//     exit(EXIT_FAILURE);
// }



void* map_reduce(void* arg){
    Thread_context* thread_context = (Thread_context*) arg;
    // if (thread_context == nullptr || thread_context->job_context == nullptr || thread_context->job_context->barrier == nullptr) {
    //     // Handle error appropriately, possibly logging and exiting the function
    //     return nullptr;
    //}


    mapPhase(arg);
    thread_context->job_context->barrier->barrier();

    if(thread_context->tid==0) {
        sem_wait(&thread_context->job_context->shuffle_sem);

        shuffle_phase(arg);
        thread_context->job_context->state.stage=REDUCE_STAGE;
        thread_context->job_context->atomic_counter= ((uint64_t) 3) << 62;
        sem_post(&thread_context->job_context->shuffle_sem);
    }

    thread_context->job_context->barrier->barrier();
    reduce_phase(arg);
    return nullptr;
}

////API functions////

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){

    auto *job = new Job_context;
    job->threads = new pthread_t[multiThreadLevel];
    job->barrier= new Barrier(multiThreadLevel);
    //job->state.stage= UNDEFINED_STAGE;
    job->atomic_counter = inputVec.size();
    // job->atomic_counter->store(0);
    job->thread_contexts=new Thread_context[multiThreadLevel];
    job->levels = multiThreadLevel;
    job->input_vec = inputVec ;
    job->output_vec= outputVec;
    job->client= &client;
    job->atomic_counter=(uint64_t) 1 << 62;

    job->state.stage= MAP_STAGE;
    job->flag=false;


    sem_init(&job->map_semaphore, 0, 1);
    sem_init(&job->reduce_semaphore, 0, 1);
    sem_init(&job->wait_sem, 0, 1);
    sem_init(&job->state_sem, 0, 1);
    sem_init(&job->shuffle_sem, 0, 1);

    for(int i=0; i<multiThreadLevel ; i++){
        job->thread_contexts[i]={i,job};
        if(pthread_create(&job->threads[i], nullptr,map_reduce,job->thread_contexts+i)!=0){
            printf(SYSTEM_ERROR);
        }
    }
    return job;
}



void emit2 (K2* key, V2* value, void* context) {
    auto *job_context = (Job_context*) context;
//    if (key == nullptr || value == nullptr) {
//        // Handle the error appropriately
//        return;

    job_context->inter_set.emplace(std::make_pair(key, value));;

}


void emit3 (K3* key, V3* value, void* context) {
    //todo check if we need a mutex
    auto *job_context = (Job_context*) context;
    job_context->output_vec.push_back(OutputPair(key, value));

}

void waitForJob(JobHandle job) {
    auto *job_context= (Job_context*) job;
    sem_wait(&job_context->wait_sem);
    for (int i = 0; i < job_context->levels; ++i) {
        if (pthread_join(job_context->threads[i], nullptr) != 0) {
            fprintf(stderr,SYSTEM_ERROR);
            exit(EXIT_FAILURE);
        }
    }
    sem_post(&job_context->wait_sem);
}



//this function gets a JobHandle and updates the state of the job into the  given JobState struct (from the pdf)
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
            size = job_context->vec_of_inter_vecs.size();
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
    auto *job_context= (Job_context*) job;
    if(!job_context->flag) {
        waitForJob(job);
        job_context->flag=true;
    }
    delete job_context;
}
