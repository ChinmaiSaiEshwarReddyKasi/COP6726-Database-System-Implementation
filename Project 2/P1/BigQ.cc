#include "BigQ.h"

void* tpmmsMainProcess(void* arg) {
    tpmmsStruct* argWorker = (tpmmsStruct*) arg;
	priority_queue<Record*, vector<Record*>, CompareRecords> recordsQ (argWorker->sequence);
    priority_queue<TPMMS*, vector<TPMMS*>, CompareBuffers> buffersQ (argWorker->sequence);
    Record thisRecord;
    vector<Record* > RecordBuffer;
    

    //Set disk based file for sorting
    File f;
    char* fName = "tmp.bin";
	f.Open(0, fName);
    

    //Buffer page used for disk based file
   
	int countOfPage = 0;
    int indexOfPage = 0;
	Page bP;
    
    //Retrieve all records from input pipe
    while (argWorker->inputData->Remove(&thisRecord) == 1) {
        Record* recordTemp = new Record;
        recordTemp->Copy(&thisRecord);
        //Add to another page if current page is full
        if (bP.Append(&thisRecord) == 0) {
            countOfPage++;
            bP.EmptyItOut();

            //Add to another run if current run is full
            if (countOfPage == argWorker->runlength) {
                tpmmsPipeline(recordsQ, buffersQ, f, bP, indexOfPage);
                recordsQ = priority_queue<Record*, vector<Record*>, CompareRecords> (argWorker->sequence);
                countOfPage = 0;
            }

            bP.Append(&thisRecord);
        }

        recordsQ.push(recordTemp);
    }
    // Handle the last run
    if (!recordsQ.empty()) {
        tpmmsPipeline(recordsQ, buffersQ, f, bP, indexOfPage);
        recordsQ = priority_queue<Record*, vector<Record*>, CompareRecords> (argWorker->sequence);
    }
    // Merge for all runs
    while (!buffersQ.empty()) {
        TPMMS* topData = buffersQ.top();
        buffersQ.pop();
        argWorker->outputData->Insert(topData->firstRecord);
        if (topData->firstRecordUpdate() == 1) {
            buffersQ.push(topData);
        }
    }
    f.Close();
    argWorker->outputData->ShutDown();
    return NULL;
}

//Used for puting records into a run, which is disk file based
void* tpmmsPipeline(priority_queue<Record*, vector<Record*>, CompareRecords>& recordsQ, priority_queue<TPMMS*, vector<TPMMS*>, CompareBuffers>& buffersQ, 
File& f, Page& bP, int& indexOfPage) {

    int indexOfStart = indexOfPage;
    bP.EmptyItOut();
    while (!recordsQ.empty()) {
        Record* recordTemp = new Record;
        recordTemp->Copy(recordsQ.top());
        recordsQ.pop();
        if (bP.Append(recordTemp) == 0) {
            f.AddPage(&bP, indexOfPage++);
            bP.EmptyItOut();
            bP.Append(recordTemp);
        }
    }
    f.AddPage(&bP, indexOfPage++);
    bP.EmptyItOut();
    TPMMS* tpmmsData = new TPMMS(&f, indexOfStart, indexOfPage - indexOfStart);
    buffersQ.push(tpmmsData);
    return NULL;
}



BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    pthread_t worker;
    //Construct arguement used for worker thread
    tpmmsStruct* argWorker = new tpmmsStruct;
    argWorker->inputData = &in;
    argWorker->outputData = &out;
    argWorker->sequence = &sortorder;
    argWorker->runlength = runlen;
    pthread_create(&worker, NULL, tpmmsMainProcess, (void*) argWorker);
    pthread_join(worker, NULL);
	out.ShutDown ();
}

BigQ::~BigQ () {

}

TPMMS::TPMMS(File* f, int start, int l) {
	runLength = l;
    fBase = f;
    firstPage = start;
    
    presentPage = start;
    fBase->GetPage(&bP, firstPage);
    firstRecord = new Record;
    firstRecordUpdate();
}

int TPMMS::firstRecordUpdate() {
    //if bufferPage is full
    if (bP.GetFirst(firstRecord) == 0) {
        //if reach the last page
        presentPage++;
        if (presentPage == firstPage + runLength) {
            return 0;
        }
        bP.EmptyItOut();
        fBase->GetPage(&bP, presentPage);
        bP.GetFirst(firstRecord);
    }
    return 1;
}

CompareRecords::CompareRecords(OrderMaker* OM) {
    sequence = OM;
}

bool CompareRecords::operator () (Record* left, Record* right) {
    ComparisonEngine CE;
    if (CE.Compare(left, right, sequence) >= 0)
        return true;
    return false;
}

CompareBuffers::CompareBuffers(OrderMaker* OM) {
    sequence = OM;
}

bool CompareBuffers::operator () (TPMMS* left, TPMMS* right) {
    ComparisonEngine CE;
    if (CE.Compare(left->firstRecord, right->firstRecord, sequence) >= 0)
        return true;
    return false;
}


