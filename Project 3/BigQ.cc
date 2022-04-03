#include "BigQ.h"
#include "DBFile.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
    pthread_t thread;
    tpmmsStruct* tpmmsArgs = new tpmmsStruct;
    tpmmsArgs->inputData = &in;
    tpmmsArgs->outputData = &out;
    tpmmsArgs->sequence = &sortorder;
    tpmmsArgs->runlength = runlen;
    pthread_create(&thread, NULL, tpmmsMainProcess, (void*) tpmmsArgs);
}

BigQ::~BigQ () {

}

TPMMS::TPMMS(File* f, int start, int l) {
    fBase = f;
	runLength = l;
    firstPage = start;    
    presentPage = start;
    firstRecord = new Record;
    fBase->GetPage(&bP, firstPage);
    firstRecordUpdate();
}

void* tpmmsMainProcess(void* arg) {
    File f;
    Record r;
    Page bP;
    int countOfPage = 0;
    int indexOfPage = 0;
    tpmmsStruct* tpmmsArgs = (tpmmsStruct*) arg;
	priority_queue<Record*, vector<Record*>, CompareRecords> recordsQ (tpmmsArgs->sequence);
    vector<Record* > RecordBuffer;
    priority_queue<TPMMS*, vector<TPMMS*>, CompareBuffers> buffersQ (tpmmsArgs->sequence);
    char* fName = new char[100];
    sprintf(fName, "%d.bin", pthread_self());
	f.Open(0, fName);
    
    // TPMMS Phase 1 - Getting all records from the pipe
    while (tpmmsArgs->inputData->Remove(&r) == 1) {
        Record* recordTemp = new Record;
        recordTemp->Copy(&r);
        // Check if the present page is full or not
        if (bP.Append(&r) == 0) {
            countOfPage++;
            bP.EmptyItOut();

            // Check if the present pipeline is full or not, and create a new one if full
            if (countOfPage == tpmmsArgs->runlength) {
                tpmmsPipeline(recordsQ, buffersQ, f, bP, indexOfPage);
                recordsQ = priority_queue<Record*, vector<Record*>, CompareRecords> (tpmmsArgs->sequence);
                countOfPage = 0;
            }

            bP.Append(&r);
        }

        recordsQ.push(recordTemp);
    }

    if (!recordsQ.empty()) {
        tpmmsPipeline(recordsQ, buffersQ, f, bP, indexOfPage);
        recordsQ = priority_queue<Record*, vector<Record*>, CompareRecords> (tpmmsArgs->sequence);
    }
    // TPMMS Phase 2 - Gather all the buffers and merge them
	Schema s ("catalog", "supplier");
	TPMMS* tpmms;
    // Record rec;
    while (!buffersQ.empty()) {
        tpmms = buffersQ.top();
		Record* temprec = new Record();
        buffersQ.pop();
		temprec->Copy(tpmms->firstRecord);
		tpmmsArgs->outputData->Insert(temprec);
        if (tpmms->firstRecordUpdate() == 1) {
            buffersQ.push(tpmms);
        }
    }
    f.Close();
    tpmmsArgs->outputData->ShutDown();
	remove(fName);
    return NULL;
}


CompareBuffers::CompareBuffers(OrderMaker* orderMaker) {
    sequence = orderMaker;
}

bool CompareBuffers::operator () (TPMMS* l, TPMMS* r) {
    ComparisonEngine CE;
    if (CE.Compare(l->firstRecord, r->firstRecord, sequence) >= 0)
        return true;
    return false;
}

// Function to put the records in pipeline
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

int TPMMS::firstRecordUpdate() {
    //checking if the page is already full
    if (bP.GetFirst(firstRecord) == 0) {
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

CompareRecords::CompareRecords(OrderMaker* orderMaker) {
    sequence = orderMaker;
}

bool CompareRecords::operator () (Record* l, Record* r) {
    ComparisonEngine CE;
    if (CE.Compare(l, r, sequence) >= 0)
        return true;
    return false;
}
