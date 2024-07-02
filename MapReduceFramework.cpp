//
// Created by rawaa on 01/07/2024.
//
#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include <pthread.h>
#include "Barrier.h"
#include <atomic>
#include <iostream>
#include "vector"
#include "algorithm"
//#include <cstdio>
//#include <cstdlib>



JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel);
typedef struct Job_context;
struct Thread_context{
    int tid;
    Job_context *job_context;


};
struct Job_context{
    pthread_t *threads;
    JobState state {UNDEFINED_STAGE,0.0};
    IntermediateVec intermediate_vec;
    InputVec input_vec;
    OutputVec output_vec;
    int levels;
    Barrier *barrier;
    Thread_context thread_context;
    const MapReduceClient *client;
    std::vector<IntermediateVec> vec_of_inter_vecs;
    std::vector<IntermediateVec> shuffled_vecs;

    std::atomic<int>mapcompleted;
    std::atomic<int>reducecompleted;
    std::atomic<int>total_pairs;
    pthread_mutex_t map_mutex = PTHREAD_MUTEX_INITIALIZER;
    pthread_mutex_t reduce_mutex= PTHREAD_MUTEX_INITIALIZER;

};
//TODO checkthis function
void* mapPhase(void* arg) {
    Thread_context* thread_context = static_cast<Thread_context*>(arg);
    //auto *jobContext = (Job_context*)threadContext->;
    for (const auto& inputPair : thread_context->job_context->input_vec) {
        if(pthread_mutex_lock(&thread_context->job_context->map_mutex)!=0){
            printf("ERROR");//TODO
            printf("\n");
            exit(EXIT_FAILURE);
        }
        // momken bdl jobcontext tkon intermediate vec
        thread_context->job_context->client->map(inputPair.first, inputPair.second, thread_context->job_context);
        thread_context->job_context->mapcompleted++;
        if(pthread_mutex_unlock(&thread_context->job_context->map_mutex)!=0){
            printf("ERROR");//TODO
            printf("\n");
            exit(EXIT_FAILURE);
        }

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
    for(int i ; i<multiThreadLevel ; i++){
        pthread_create(&job->threads[i], nullptr,mapPhase,job->threads+i);
    }
    for(int i ; i<multiThreadLevel; i++) {
        pthread_join(job->threads[i], nullptr);
    }
    return job;
}



void* map_reduce_job(void* arg){
    Job_context* jobContext = static_cast<Job_context*>(arg);

}

void waitForJob(JobHandle job){}
