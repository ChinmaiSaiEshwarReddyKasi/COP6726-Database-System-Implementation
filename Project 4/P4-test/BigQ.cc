#include "BigQ.h"

RecordComparer :: RecordComparer (OrderMaker* sort_order) : sortorder(sort_order){
}

bool RecordComparer::operator() (Record* leftRecord, Record* rightRecord) {

    ComparisonEngine ceng;
    return (ceng.Compare (leftRecord, rightRecord, sortorder) < 0) ;

}

bool RunComparer :: operator() (Run* leftRun, Run* rightRun) {

    ComparisonEngine ceng;
    return (ceng.Compare (leftRun->head, rightRun->head, leftRun->sortorder) >= 0);

}

Run :: ~Run () {
    delete head;
}

Run :: Run (int length, int offset, File* runFileName, OrderMaker* sort_order) : runPageSize(length), runPageOffset(offset), runsFile(runFileName), sortorder(sort_order) {

    head = new Record ();
    runsFile->GetPage (&RunPage, offset);
    getHead ();

}

Run :: Run (File* runFileName, OrderMaker* sort_order) : head(NULL), runsFile(runFileName), sortorder(sort_order){
}

int Run :: getHead () {
    Record* tempRecord = new Record();
    if(runPageSize <= 0) {return 0;}

    if (RunPage.GetFirst(tempRecord) == 0) {
        runsFile->GetPage(&RunPage, ++runPageOffset);
        RunPage.GetFirst(tempRecord);
    }

    runPageSize--;
    head->Consume(tempRecord);
    return 1;
}

void BigQ :: WriteToFile () {

    Page* tempPage = new Page();
    int numRecords = recordVec.size(), headOffset = pageCount, numPages = 1;

    for (auto tempRecord : recordVec) {
        if ((tempPage->Append (tempRecord)) == 0) {
            numPages++;
            runsFile.AddPage (tempPage, pageCount++);
            tempPage->EmptyItOut ();
            tempPage->Append (tempRecord);
        }
        //delete tempRecord;
    }

    runsFile.AddPage(tempPage, pageCount++);
    tempPage->EmptyItOut();
    delete tempPage;
    recordVec.clear();
    AddRunToHeap (numRecords, headOffset);
}

void BigQ :: AddRunToHeap (int runPageSize, int runPageOffset) {
    Run* tempRun = new Run(runPageSize, runPageOffset, &runsFile, sortorder);
    runHeap.push(tempRun);
    return;
}

void BigQ :: SortPhase() {

    writeFileName = new char[100];
    Record* tempRecord = new Record();
    Page* tempPage = new Page();

    srand (time(NULL));
    sprintf (writeFileName, "%d.txt", (uintptr_t)worker);

    runsFile.Open (0, writeFileName);

    int numPages = 0, numRecords = 0;
    while (inPipe->Remove(tempRecord)) {

        Record* vecRecord = new Record ();
        vecRecord->Copy (tempRecord);

        numRecords++;

        if (tempPage->Append (tempRecord) == 0) {
            if (++numPages == runlength) {
                sort (recordVec.begin (), recordVec.end (), RecordComparer (sortorder));
                WriteToFile();
                numPages = 0;
            }
            tempPage->EmptyItOut ();
            tempPage->Append (tempRecord);
        }
        recordVec.push_back(vecRecord);
    }

    if(!recordVec.empty()) {
        sort (recordVec.begin (), recordVec.end (), RecordComparer(sortorder));
        WriteToFile();
        tempPage->EmptyItOut ();
    }

    delete tempRecord;
    delete tempPage;

}

void BigQ :: MergePhase () {

    Run* tempRun = new Run (&runsFile, sortorder);

    while (!runHeap.empty ()) {
        Record* tempRecord = new Record ();
        tempRun = runHeap.top();
        tempRecord->Copy(tempRun->head);
        outPipe->Insert(tempRecord);
        runHeap.pop ();

        if (tempRun->getHead() > 0) {
            runHeap.push(tempRun);
        }
        delete tempRecord;
    }
    runsFile.Close();
    outPipe->ShutDown();
    delete tempRun;
    remove(writeFileName);
}

BigQ :: BigQ (Pipe& in, Pipe& out, OrderMaker& sort_order, int length) : sortorder(&sort_order), inPipe(&in), outPipe(&out), runlength(length), pageCount(1){
    pthread_create(&worker, NULL, StartMainThread, (void*)this);

}