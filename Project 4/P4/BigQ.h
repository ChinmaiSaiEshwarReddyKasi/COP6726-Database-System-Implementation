#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include <vector>
#include <queue>
#include <algorithm>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

using namespace std;

class Run;

class RunComparer {
private:
    OrderMaker* sortorder;
public:
    bool operator() (Run* leftRun, Run* rightRun);
};

class BigQ {

private:
    int pageCount;
    pthread_t worker;
    char* writeFileName;
    priority_queue<Run*, vector<Run*>, RunComparer> runHeap;
    OrderMaker* sortorder;
    void WriteToFile ();
    Pipe* inPipe;
    Pipe* outPipe;
    void AddRunToHeap (int runPageSize, int runPageOffset);
    friend bool RecordComparer (Record* leftRecord, Record* rightRecord);

public:

    int runlength;
    vector<Record*> recordVec;
    File runsFile;

    void SortPhase();
    void MergePhase ();

    static void *StartMainThread (void* start) {
        BigQ* bigQ = (BigQ*)start;
        bigQ->SortPhase ();
        bigQ->MergePhase ();
    }

    BigQ (Pipe& in, Pipe& out, OrderMaker& sortorder, int length);
    ~BigQ () {};

};

class RecordComparer {
private:
    OrderMaker* sortorder;
public:
    bool operator() (Record* leftRecord, Record* rightRecord);
    RecordComparer (OrderMaker* sort_order);
};

class Run {

private:

    Page RunPage;

public:

    int runPageOffset;
    int runPageSize;

    Record* head;
    OrderMaker* sortorder;
    File* runsFile;

    int getHead();

    Run (int length, int offset, File* runfileName, OrderMaker* sort_order);
    Run (File* runfileName, OrderMaker* sort_order);
    ~Run();

};


#endif