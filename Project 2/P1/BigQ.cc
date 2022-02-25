#include "BigQ.h"

void* workerMain(void* arg) {
    WorkerArg* argWorker = (WorkerArg*) arg;
	priority_queue<Record*, vector<Record*>, RecordComparer> queueRecord (argWorker->sequence);
    priority_queue<Run*, vector<Run*>, RunComparer> queueRunner(argWorker->sequence);
    Record thisRecord;
    vector<Record* > RecordBuffer;
    

    //Set disk based file for sorting
    File f;
	f.Open(0, fName);
    char* fName = "tmp.bin";
    

    //Buffer page used for disk based file
   
	int countOfPage = 0;
    int indexOfPage = 0;
	Page bP;
    
    //Retrieve all records from input pipe
    while (argWorker->in->Remove(&thisRecord) == 1) {
        Record* recordTemp = new Record;
        recordTemp->Copy(&thisRecord);
        //Add to another page if current page is full
        if (bP.Append(&thisRecord) == 0) {
            countOfPage++;
            bP.EmptyItOut();

            //Add to another run if current run is full
            if (countOfPage == argWorker->runlen) {
                recordQueueToRun(queueRecord, queueRunner, f, bP, indexOfPage);
                queueRecord = priority_queue<Record*, vector<Record*>, RecordComparer> (argWorker->sequence);
                countOfPage = 0;
            }

            bP.Append(&thisRecord);
        }

        queueRecord.push(recordTemp);
    }
    // Handle the last run
    if (!queueRecord.empty()) {
        recordQueueToRun(queueRecord, queueRunner, f, bP, indexOfPage);
        queueRecord = priority_queue<Record*, vector<Record*>, RecordComparer> (argWorker->sequence);
    }
    // Merge for all runs
    while (!queueRunner.empty()) {
        Run* run = queueRunner.top();
        queueRunner.pop();
        argWorker->out->Insert(run->highestRecord);
        if (run->UpdatehighestRecord() == 1) {
            queueRunner.push(run);
        }
    }
    f.Close();
    argWorker->out->ShutDown();
    return NULL;
}

//Used for puting records into a run, which is disk file based
void* recordQueueToRun(priority_queue<Record*, vector<Record*>, RecordComparer>& queueRecord, 
    priority_queue<Run*, vector<Run*>, RunComparer>& queueRunner, File& f, Page& bP, int& indexOfPage) {

    bP.EmptyItOut();
    int indexOfStart = indexOfPage;
    while (!queueRecord.empty()) {
        Record* recordTemp = new Record;
        recordTemp->Copy(queueRecord.top());
        queueRecord.pop();
        if (bP.Append(recordTemp) == 0) {
            f.AddPage(&bP, indexOfPage++);
            bP.EmptyItOut();
            bP.Append(recordTemp);
        }
    }
    f.AddPage(&bP, indexOfPage++);
    bP.EmptyItOut();
    Run* run = new Run(&f, indexOfStart, indexOfPage - indexOfStart);
    queueRunner.push(run);
    return NULL;
}



BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    pthread_t worker;
    //Construct arguement used for worker thread
    WorkerArg* argWorker = new WorkerArg;
    argWorker->in = &in;
    argWorker->out = &out;
    argWorker->sequence = &sortorder;
    argWorker->runlen = runlen;
    pthread_create(&worker, NULL, workerMain, (void*) argWorker);
    pthread_join(worker, NULL);
	out.ShutDown ();
}

BigQ::~BigQ () {

}

Run::Run(File* f, int start, int length) {
	runLength = length;
    fBase = f;
    startPage = start;
    
    curPage = start;
    fBase->GetPage(&bP, startPage);
    highestRecord = new Record;
    UpdatehighestRecord();
}

int Run::UpdatehighestRecord() {
    //if bufferPage is full
    if (bP.GetFirst(highestRecord) == 0) {
        //if reach the last page
        curPage++;
        if (curPage == startPage + runLength) {
            return 0;
        }
        bP.EmptyItOut();
        fBase->GetPage(&bP, curPage);
        bP.GetFirst(highestRecord);
    }
    return 1;
}

RecordComparer::RecordComparer(OrderMaker* OM) {
    sequence = OM;
}

bool RecordComparer::operator () (Record* left, Record* right) {
    ComparisonEngine CE;
    if (CE.Compare(left, right, sequence) >= 0)
        return true;
    return false;
}

RunComparer::RunComparer(OrderMaker* OM) {
    sequence = OM;
}

bool RunComparer::operator () (Run* left, Run* right) {
    ComparisonEngine CE;
    if (CE.Compare(left->highestRecord, right->highestRecord, sequence) >= 0)
        return true;
    return false;
}


