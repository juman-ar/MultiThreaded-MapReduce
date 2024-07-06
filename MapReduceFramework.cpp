//
// Created by rawaa on 01/07/2024.
//
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

//#include <cstdio>
//#include <cstdlib>
#define SYSTEM_ERROR "system error: cannot create thread\n"

#define INDEX 0x7FFFFFF
#define INC_PROCESSED 0x80000000
#define PROCESSED 0x3FFFFFFF80000000
#define PERCENTAGE 100


typedef struct Job_context;
struct Thread_context{
    int tid;
    Job_context *job_context;
   // IntermediateVec inter_vec;
};

struct Compare {
    bool operator()(const IntermediatePair &first, const  IntermediatePair &sec) const{
        return *(first.first) < *(sec.first);
    }
};

// bool equal(IntermediatePair &right, IntermediatePair &left) {
//     return !(*right.first < *left.first) && !(*left.first < *right.first);
// }


struct Job_context{
    pthread_t *threads;
    JobState state {UNDEFINED_STAGE,0.0};
    //IntermediateVec intermediate_vec;
    std::set<IntermediatePair, Compare> inter_set;

    InputVec input_vec;
    OutputVec output_vec;
    int levels;
    Barrier *barrier;
    Thread_context* thread_contexts; //made it a pointer to an array
    const MapReduceClient *client;
    std::vector<IntermediateVec> vec_of_inter_vecs;
    std::vector<IntermediateVec> shuffled_vecs;

    std::atomic<uint64_t> atomic_counter;
    bool flag; //todo may have to make it atomic

    // std::atomic<int>mapcompleted;
    // std::atomic<int>reducecompleted;
    // std::atomic<int>total_pairs;
    // pthread_mutex_t map_mutex = PTHREAD_MUTEX_INITIALIZER;
    // pthread_mutex_t reduce_mutex= PTHREAD_MUTEX_INITIALIZER;

    sem_t map_semaphore;
    sem_t reduce_semaphore;
    sem_t wait_sem;

    ~Job_context();
};

Job_context::~Job_context() {
    // pthread_mutex_destroy(&outputMutex);
    // pthread_mutex_destroy(&intermediateMutex);
    // pthread_mutex_destroy(&holdMutex);
    sem_destroy(&map_semaphore);
    sem_destroy(&reduce_semaphore);
    sem_destroy(&wait_sem);
    delete [] threads;
    delete [] thread_contexts;
    delete barrier;

}
////helper functions////



void mapPhase(void* arg) {
    Thread_context* thread_context = static_cast<Thread_context*>(arg);

    for (const auto& inputPair : thread_context->job_context->input_vec) {
       // uint64_t pair_index = ((*(counter))++) & INDEX;

        // if(pthread_mutex_lock(&thread_context->job_context->map_mutex)!=0){
        //     printf("ERROR");
        //     printf("\n");
        //     exit(EXIT_FAILURE);
        // }

        sem_wait(&thread_context->job_context->map_semaphore);

        // momken bdl jobcontext tkon intermediate vec
        thread_context->job_context->client->map(inputPair.first, inputPair.second, thread_context->job_context);

        sem_post(&thread_context->job_context->map_semaphore);

        // if(pthread_mutex_unlock(&thread_context->job_context->map_mutex)!=0){
        //     printf("ERROR");
        //     printf("\n");
        //     exit(EXIT_FAILURE);
        // }

    }
}




// void* sort_vec(Thread_context* threadContext){
//     auto *jobContext = (Job_context*)threadContext->job_context;
//     IntermediateVec &curr = jobContext->vec_of_inter_vecs.at(threadContext->tid);
//     std::sort(curr.begin(),curr.end(),sort_helper);
// }


//void suffle_vectors(void* arg){
//
//    while (!allIntermediateVectorsEmpty()) {
//            std::vector<std::pair<K2, V2>> newVector;
//            for (auto &intermediateVector : intermediateVectors) {
//                if (!intermediateVector.empty()) {
//                    newVector.push_back(intermediateVector.back());  // Access the last element
//                    intermediateVector.pop_back();  // Remove the last element
//                }
//            }
//            queueOfVectors.push_back(newVector);
//            vectorCounter++;
//        }
//    }
//
//}

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
                thread_context->job_context->atomic_counter++; // TODO
            } else {
                lastElement = *i;
                IntermediateVec v;
                v.push_back(*i);
                thread_context->job_context->vec_of_inter_vecs.push_back(v);
                thread_context->job_context->atomic_counter++; // TODO
            }
        }
    }
}

void reduce_phase(void * arg) {
    Thread_context* thread_context = (Thread_context*) arg;
    thread_context->job_context->state.stage=REDUCE_STAGE;
    thread_context->job_context->atomic_counter= ((uint64_t) 3) << 62;

    sem_wait(&thread_context->job_context->reduce_semaphore);

    int element = thread_context->job_context->atomic_counter++;
    while (element < thread_context->job_context->vec_of_inter_vecs.size()) {
        thread_context->job_context->client->reduce(&thread_context->job_context->vec_of_inter_vecs.at(element)
            ,thread_context);
        //todo add progress
        element = thread_context->job_context->atomic_counter++;

    }

    sem_post(&thread_context->job_context->reduce_semaphore);
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
    mapPhase(arg);
    thread_context->job_context->barrier->barrier();

    if(thread_context->tid==0) {
        //todo check if we need to do all the stage chenges or if we can just do them in the suffle function itself
        shuffle_phase(arg);
    }
    thread_context->job_context->barrier->barrier();
    reduce_phase(arg);
    return nullptr;
}

////API functions////

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    Job_context *job = new Job_context;
    job->threads = new pthread_t(multiThreadLevel);
    job->state.stage= MAP_STAGE;
    job->atomic_counter = ((uint64_t) 1) << 62;
    job->levels = multiThreadLevel;
    job->input_vec = inputVec ;
    job->output_vec= outputVec;
    job->client= &client;
    job->flag=false;

    job->barrier= new Barrier(multiThreadLevel);
    sem_init(&job->map_semaphore, 0, 1);
    sem_init(&job->reduce_semaphore, 0, 1);
    sem_init(&job->wait_sem, 0, 1);
    for(int i ; i<multiThreadLevel ; i++){
        pthread_create(&job->threads[i], nullptr,map_reduce,job->threads+i);
    }
    // for(int i ; i<multiThreadLevel; i++) {
    //     pthread_join(job->threads[i], nullptr);
    // }
    return job;
}



void emit2 (K2* key, V2* value, void* context) {
    auto *thread_context = (Thread_context*) context;
    thread_context->job_context->inter_set.insert(IntermediatePair(key, value));

}


void emit3 (K3* key, V3* value, void* context) {
    //todo check if we need a mutex
    auto *thread_context = (Thread_context*) context;
    thread_context->job_context->output_vec.push_back(OutputPair(key, value));

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

}


void closeJobHandle(JobHandle job) {
    auto *job_context= (Job_context*) job;
    if(!job_context->flag) {
        waitForJob(job);
        job_context->flag=true;
    }
   delete job_context;
}

