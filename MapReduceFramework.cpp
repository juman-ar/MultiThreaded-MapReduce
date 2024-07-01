//
// Created by rawaa on 01/07/2024.
//
#include "MapReduceFramework.h"
#include "MapReduceClient.h"
#include "pthread.h"
#include "Barrier.h"
#include <atomic>
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
    std::atomic<int>mapcompleted;
    std::atomic<int>reducecompleted;
    std::atomic<int>total_pairs;

};
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel){
    Job_context *job = new Job_context;
    job->threads = new pthread_t(multiThreadLevel);
    job->levels = multiThreadLevel;
    job->input_vec = inputVec ;
    job->output_vec= outputVec;
    job->client= &client;
    job->barrier= new Barrier(multiThreadLevel);
    for(int i ; i<multiThreadLevel ; i++){
        pthread_create(job->threads[i],mapPhase,job);
    }
    for(int i ; i<multiThreadLevel; i++){
        pthread_join(job->threads[i], nullptr);
    }




}
void waitForJob(JobHandle job){}
