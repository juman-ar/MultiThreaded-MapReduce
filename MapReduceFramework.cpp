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
//#include <cstdio>
//#include <cstdlib>



typedef struct Job_context;
struct Thread_context{
    int tid;
    Job_context *job_context;
    IntermediateVec inter_vec; //TODO zedt hay
};


struct Job_context{
    pthread_t *threads;
    JobState state {UNDEFINED_STAGE,0.0};
    IntermediateVec intermediate_vec;
    InputVec input_vec;
    OutputVec output_vec;
    int levels;
    Barrier *barrier;
    Thread_context* thread_contexts; //made it a pointer to an array
    const MapReduceClient *client;
    std::vector<IntermediateVec> vec_of_inter_vecs;
    std::vector<IntermediateVec> shuffled_vecs;

    std::atomic<int>mapcompleted;
    std::atomic<int>reducecompleted;
    std::atomic<int>total_pairs;
    // pthread_mutex_t map_mutex = PTHREAD_MUTEX_INITIALIZER;
    // pthread_mutex_t reduce_mutex= PTHREAD_MUTEX_INITIALIZER;
    sem_t map_semaphore;
    sem_t reduce_semaphore;
};


////helper functions////



//TODO checkthis function
void* mapPhase(void* arg) {
    Thread_context* thread_context = static_cast<Thread_context*>(arg);
    //auto *jobContext = (Job_context*)threadContext->;
    for (const auto& inputPair : thread_context->job_context->input_vec) {
        // if(pthread_mutex_lock(&thread_context->job_context->map_mutex)!=0){
        //     printf("ERROR");//TODO
        //     printf("\n");
        //     exit(EXIT_FAILURE);
        // }

        sem_wait(&thread_context->job_context->map_semaphore);

        // momken bdl jobcontext tkon intermediate vec
        thread_context->job_context->client->map(inputPair.first, inputPair.second, thread_context->job_context);
        thread_context->job_context->mapcompleted++;

        sem_post(&thread_context->job_context->map_semaphore);

        // if(pthread_mutex_unlock(&thread_context->job_context->map_mutex)!=0){
        //     printf("ERROR");//TODO
        //     printf("\n");
        //     exit(EXIT_FAILURE);
        // }

    }
    return nullptr;
}


bool sort_helper(IntermediatePair &first, IntermediatePair &sec){
    return *(first.first) < *(sec.first);
}

void* sort_vec(Thread_context* threadContext){
    auto *jobContext = (Job_context*)threadContext->job_context;
    IntermediateVec &curr = jobContext->vec_of_inter_vecs.at(threadContext->tid);
    std::sort(curr.begin(),curr.end(),sort_helper);
}


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
    std::vector<std::pair<K2, V2>> new_vec;
    auto curr = thread_context->job_context->intermediate_vec.back();

    while (!thread_context->job_context->vec_of_inter_vecs.empty()) {

       // if(!(*(curr.first)<*()))
    }
}
void reduce_phase(void * arg){
    Thread_context* thread_context = (Thread_context*) arg;

    sem_wait(&thread_context->job_context->reduce_semaphore);

    // Reduce logic goes here

    sem_post(&thread_context->job_context->reduce_semaphore);


    // if(pthread_mutex_lock(&thread_context->job_context->reduce_mutex)!=0){
    //     printf("ERROR");//TODO
    //     printf("\n");
    //     exit(EXIT_FAILURE);
    // }
    //
    //
    // if(pthread_mutex_unlock(&thread_context->job_context->reduce_mutex)!=0){
    //     printf("ERROR");//TODO
    //     printf("\n");
    //     exit(EXIT_FAILURE);
    // }
}


void* map_reduce_job(void* arg){
    Job_context* jobContext = static_cast<Job_context*>(arg);

}


////API functions////
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    Job_context *job = new Job_context;
    job->threads = new pthread_t(multiThreadLevel);
    job->state.stage= MAP_STAGE;
    job->levels = multiThreadLevel;
    job->input_vec = inputVec ;
    job->output_vec= outputVec;
    job->client= &client;
    job->barrier= new Barrier(multiThreadLevel);
    sem_init(&job->map_semaphore, 0, 1);
    sem_init(&job->reduce_semaphore, 0, 1);

    for(int i ; i<multiThreadLevel ; i++){
        pthread_create(&job->threads[i], nullptr,mapPhase,job->threads+i);
    }
    for(int i ; i<multiThreadLevel; i++) {
        pthread_join(job->threads[i], nullptr);
    }
    return job;
}



void emit2 (K2* key, V2* value, void* context) {
    auto *thread_context = (Thread_context*) context;

}


void emit3 (K3* key, V3* value, void* context) {
    auto *thread_context = (Thread_context*) context;

}

void waitForJob(JobHandle job){}
void getJobState(JobHandle job, JobState* state);
void closeJobHandle(JobHandle job);