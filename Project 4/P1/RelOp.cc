#include "RelOp.h"
#include <iostream>
#include "BigQ.h"


typedef struct SFArgs{
    Pipe &outPipe;
    DBFile &inFile;
    Record &literal;
    CNF &selOp;
};

void* SFMain(void* arg){
    SFArgs* sfArgs = (SFArgs*) arg;
    Record rec;
    while(sfArgs->inFile.GetNext(rec, sfArgs->selOp, sfArgs->literal)){
        sfArgs->outPipe.Insert(&rec);
    }
    sfArgs->outPipe.ShutDown();
    return NULL;
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    SFArgs* sfArgs = new SFArgs{outPipe, inFile, literal, selOp};
    pthread_create(&thread, NULL, SFMain, (void*) sfArgs);
}

void SelectFile::Use_n_Pages (int runlen) {

}

void SelectFile::WaitUntilDone () {
	pthread_join (thread, NULL);
}



typedef struct SPArgs{
    CNF &selOp;
    Pipe &outPipe;
    Record &literal;
    Pipe &inPipe;
};
void* SPMain(void*arg){
    ComparisonEngine ce;
    SPArgs* spArgs = (SPArgs*) arg;
    Record r;
    while(spArgs->inPipe.Remove(&r)){
        if(ce.Compare(&r, &spArgs->literal, &spArgs->selOp)){
            spArgs->outPipe.Insert(&r);
        }
    }
    spArgs->outPipe.ShutDown();
    return NULL;
}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    SPArgs* spArgs = new SPArgs{selOp, outPipe, literal, inPipe};
    pthread_create(&thread, NULL, SPMain, (void*) spArgs);
}

void SelectPipe::Use_n_Pages (int runlen) {

}

void SelectPipe::WaitUntilDone () {
	pthread_join (thread, NULL);
}


typedef struct SumArgs{
    Pipe &inPipe;
    Function &computeMe;
    Pipe &outPipe;
};

void* SumMain(void*arg){
    int ISum = 0;
    int IRes = 0;
    double DSum = 0.0;
    double DRes = 0.0;
    SumArgs* sumArgs = (SumArgs*) arg;
    Record r;
    Type type;
    while(sumArgs->inPipe.Remove(&r)){
        type = sumArgs->computeMe.Apply(r, IRes, DRes);
        if(type==Int){
            ISum += IRes;
        }
        else{
            DSum += DRes;
        }
    }

    Attribute att = {"SUM", type};
    Schema out_sch ("out_sch", 1, &att);
    Record result;
    char name[100];
    if(type==Int){
        sprintf(name, "%d|", ISum);
    }
    else{
        sprintf(name, "%lf|", DSum);
    }
    result.ComposeRecord(&out_sch, name);
    sumArgs->outPipe.Insert(&result);
    sumArgs->outPipe.ShutDown();
    return NULL;
}
void Sum::Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe){
    SumArgs* sumArgs = new SumArgs{inPipe, computeMe, outPipe};
    pthread_create(&thread, NULL, SumMain, (void*) sumArgs);
}
void Sum::WaitUntilDone (){
    pthread_join(thread, NULL);
}
void Sum::Use_n_Pages (int n){

}


typedef struct GBArgs{
    Pipe &inPipe;
    OrderMaker &groupAtts;
    int pages;
    Function &computeMe;
    Pipe &outPipe;
};
void* GBMain(void*arg){
    GBArgs* gbArgs = (GBArgs*) arg;
    Pipe sorted(100);
    int ISum = 0;
    int IRes = 0;
    double DSum = 0.0;
    double DRes = 0.0;
    ComparisonEngine ce;
    BigQ bigQ(gbArgs->inPipe, sorted, gbArgs->groupAtts, gbArgs->pages);
    Record last;
    Record now;
    Type type;
    Attribute att = {"SUM", type};
    Schema out_sch ("out_sch", 1, &att);
    bool checkFirst = true;
    while(sorted.Remove(&now)){
        if(!checkFirst && ce.Compare(&now, &last, &gbArgs->groupAtts)!=0){
            cout<<"==="<<endl;
            Record recordResult;
            char name[100];
            if(type==Int){
                sprintf(name, "%d|", ISum);
            }
            else {
                sprintf(name, "%lf|", DSum);
            }
            recordResult.ComposeRecord(&out_sch, name);
            gbArgs->outPipe.Insert(&recordResult);

            ISum = 0;
            DSum = 0.0;
        }
        // add to the lastious group
        checkFirst = false;
        type = gbArgs->computeMe.Apply(now, IRes, DRes);
        if(type==Int){
            ISum += IRes;
        }
        else {
            DSum += DRes;
        }
        last.Copy(&now);
    }
    // for the last group
    Record recordResult;
    char name[100];
    if(type==Int){
        sprintf(name, "%d|", ISum);
    }
    else {
        sprintf(name, "%lf|", DSum);
    }
    recordResult.ComposeRecord(&out_sch, name);
    gbArgs->outPipe.Insert(&recordResult);
    gbArgs->outPipe.ShutDown();
    return NULL;
}
void GroupBy::Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe){
    GBArgs* gbArgs = new GBArgs{inPipe, groupAtts, pages, computeMe, outPipe};
    pthread_create(&thread, NULL, GBMain, (void*) gbArgs);
}
void GroupBy::WaitUntilDone (){
    pthread_join(thread, NULL);
}
void GroupBy::Use_n_Pages (int n){
    pages = n;
}


void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    PArgs* pa = new PArgs;
    pa->inPipe = &inPipe;
    pa->outPipe = &outPipe;
    pa->keepMe = keepMe;
    pa->numAttsInput = numAttsInput;
    pa->numAttsOutput = numAttsOutput;
    pthread_create(&thread, NULL, ProjectOperation, (void*) pa);
}

void* ProjectOperation (void* arg) {
    PArgs* pa = (PArgs*) arg;
    Record Prec;
    //Projecting records
    while (pa->inPipe->Remove(&Prec) == 1) {
        Record* tempPrec = new Record;
        tempPrec->Consume(&Prec);
        tempPrec->Project(pa->keepMe, pa->numAttsOutput, pa->numAttsInput);
        pa->outPipe->Insert(tempPrec);     
    }
    pa->outPipe->ShutDown();
    return NULL;
}

void Project::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void Project::Use_n_Pages(int n) {

}

void DuplicateRemoval::Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) { 
    DRArgs* drArgs = new DRArgs;
    drArgs->inPipe = &inPipe;
    drArgs->outPipe = &outPipe;
    OrderMaker* seq = new OrderMaker(&mySchema);
    drArgs->order = seq;
    if (this->runLen <= 0)
        drArgs->runLen = 8;
    else
        drArgs->runLen = this->runLen;
    pthread_create(&thread, NULL, RemoveDuplicates, (void*) drArgs);
}

// Method to delete duplicates. We use BigQ to sort records and then check those records for duplicates.
void* RemoveDuplicates(void* arg) {
    DRArgs* drArgs = (DRArgs*) arg;
    Record r1, r2;
    Schema schema ("catalog", "partsupp");
    ComparisonEngine ce;
    Pipe* sortedPipe = new Pipe(1000);
    BigQ* bq = new BigQ(*(drArgs->inPipe), *sortedPipe, *(drArgs->order), drArgs->runLen);
    sortedPipe->Remove(&r2);
    while (sortedPipe->Remove(&r1) == 1) {
        if (ce.Compare(&r2, &r1, drArgs->order) != 0) {
            Record* tr = new Record;
            tr->Consume(&r2);
            drArgs->outPipe->Insert(tr);
            r2.Consume(&r1);
        }
    }
    drArgs->outPipe->Insert(&r2);
    drArgs->outPipe->ShutDown();
    return NULL;
}

void DuplicateRemoval::Use_n_Pages (int n) {
    this->runLen = n;
}

void DuplicateRemoval::WaitUntilDone () { 
    pthread_join(thread, NULL);
}

void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    WOArgs* woArgs = new WOArgs;
    woArgs->inPipe = &inPipe;
    woArgs->outFile = outFile;
    woArgs->schema = &mySchema;
    pthread_create(&thread, NULL, WriteOutMain, (void*) woArgs);
}

void* WriteOutMain(void* arg) {
    WOArgs* woArgs = (WOArgs*) arg;
    Record cRec;
    while (woArgs->inPipe->Remove(&cRec) == 1) {
        int nAtts = woArgs->schema->GetNumAtts();
        Attribute *att = woArgs->schema->GetAtts();
        for (int i = 0; i < nAtts; i++) {
            fprintf(woArgs->outFile, "%s:", att[i].name);
            int p = ((int *) cRec.bits)[i + 1];
            fprintf(woArgs->outFile, "[");
            if (att[i].myType == Int) {
                int *woI = (int*) &(cRec.bits[p]);
                fprintf(woArgs->outFile, "%d", *woI);
            }
            else if (att[i].myType == Double) {
                double *woD = (double*) &(cRec.bits[p]);
                fprintf(woArgs->outFile, "%f", *woD);
            }
            else if (att[i].myType == String) {
                char* woS = (char*) &(cRec.bits[p]);
                fprintf(woArgs->outFile, "%s", woS);
            }
            fprintf(woArgs->outFile, "]");
            if (i != nAtts - 1)
                fprintf(woArgs->outFile, ", ");
        }
        fprintf(woArgs->outFile, "\n");
    }
    return NULL;
}

void WriteOut::WaitUntilDone () {
    pthread_join(thread, NULL);
}

void WriteOut::Use_n_Pages (int n) { 

}

void Join::Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    JArgs* ja = new JArgs;
    ja->inPipeL = &inPipeL;
    ja->inPipeR = &inPipeR;
    ja->outPipe = &outPipe;
    ja->selOp = &selOp;
    ja->literal = &literal;
    if (this->runLen <= 0)
        ja->runLen = 8;
    else
        ja->runLen = this->runLen;
    pthread_create(&thread, NULL, JoinMain, (void*) ja);
}

//Method to merge multiple records into one.
void JoinMergeRecords(Record* leftRecord, Record* rightRecord, Pipe* pipe) {
    Record mRecs;
    int RAtts = ((((int*) rightRecord->bits)[1]) / sizeof(int)) - 1;
    int LAtts = ((((int*) leftRecord->bits)[1]) / sizeof(int)) - 1;
    int* atts = new int[LAtts + RAtts];
    for (int i = 0; i < LAtts; i++)
        atts[i] = i;
    for (int i = LAtts; i < LAtts + RAtts; i++)
        atts[i] = i - LAtts;
    mRecs.MergeRecords(leftRecord, rightRecord, LAtts, RAtts, atts, LAtts + RAtts, LAtts);
    pipe->Insert(&mRecs);
}

// Here we sort and merge by comparing the left and right records in the Pipe. Two Pipes are generated using BigQ
void JoinMainMerge(JArgs* joinArg, OrderMaker* leftOrder, OrderMaker* rightOrder) {

    bool done = false;
    Pipe* sortedRP = new Pipe(1000);
    Record rightrec;
    Pipe* sortedLP = new Pipe(1000);
    BigQ* LQ = new BigQ(*(joinArg->inPipeL), *sortedLP, *leftOrder, joinArg->runLen);
    Record leftrec;
    ComparisonEngine ce;
    BigQ* RQ = new BigQ(*(joinArg->inPipeR), *sortedRP, *rightOrder, joinArg->runLen);

    if (sortedLP->Remove(&leftrec) == 0)
        done = true;
    if (sortedRP->Remove(&rightrec) == 0)
        done = true;
    
    //Merge the similar records
    while (!done) {
        int resultCompare = ce.Compare(&leftrec, leftOrder, &rightrec, rightOrder);
        //If left and right records are equal, we find the similar records in both the pipes and merge them and put in output pipe
        if (resultCompare == 0) {
            vector<Record*> Rleft;
            vector<Record*> Rright;
            while (true) {
                Record* previousLR = new Record;
                previousLR->Consume(&leftrec);
                Rleft.push_back(previousLR);
                if (sortedLP->Remove(&leftrec) == 0) {
                    done = true;
                    break;
                }
                if (ce.Compare(&leftrec, previousLR, leftOrder) != 0) {
                    break;
                }
            }
            while (true) {
                Record* previousRR = new Record;
                previousRR->Consume(&rightrec);
                Rright.push_back(previousRR);
                if (sortedRP->Remove(&rightrec) == 0) {
                    done = true;
                    break;
                }
                if (ce.Compare(&rightrec, previousRR, rightOrder) != 0) {
                    break;
                }
            }
            for (int i = 0; i < Rleft.size(); i++) {
                for (int j = 0; j < Rright.size(); j++) {
                    JoinMergeRecords(Rleft[i], Rright[j], joinArg->outPipe);
                }
            }
            Rleft.clear();
            Rright.clear();
        }
        //Move to right pipe, if left pipe has larger records
        else if (resultCompare > 0) {
            if (sortedRP->Remove(&rightrec) == 0)
                done = true;
        }
        //Move to left pipe, if right pipe has larger records
        else {
            if (sortedLP->Remove(&leftrec) == 0)
                done = true;
        }
    }
    cout << "Finish read fron sorted pipe" << endl;
    while (sortedLP->Remove(&leftrec) == 1);
    while (sortedRP->Remove(&rightrec) == 1);
}

void* JoinMain(void* arg) {
    JArgs* ja = (JArgs*) arg;
    OrderMaker lo, ro;
    ja->selOp->GetSortOrders(lo, ro);
    if (lo.numAtts > 0 && ro.numAtts > 0) {
        cout << "Enter sort merge " << endl;
        JoinMainMerge(ja, &lo, & ro);
    }
    else {
        cout << "BlockNestJoin" << endl;
        NestedJoin(ja);
    }
    ja->outPipe->ShutDown();
    return NULL;
}

//This is nested join where we join every record to other.
void NestedJoin(JArgs* joinArg) {
    DBFile DbFile;
    Record r;
    Record r1, r2;
    ComparisonEngine ce;

    char* fName = new char[100];
    sprintf(fName, "BlockNestedTemp%d.bin", pthread_self());
    DbFile.Create(fName, heap, NULL);
    DbFile.Open(fName);
    while (joinArg->inPipeL->Remove(&r) == 1)
        DbFile.Add(r);
    
    while (joinArg->inPipeR->Remove(&r1) == 1) {
        DbFile.MoveFirst();
        while (DbFile.GetNext(r) == 1) {
            if (ce.Compare(&r1, &r2, joinArg->literal, joinArg->selOp)) {
                JoinMergeRecords(&r1, &r2, joinArg->outPipe);
            }
        }
    }
}

void Join::Use_n_Pages (int n) { 
    this->runLen = n;
}

void Join::WaitUntilDone () { 
    pthread_join(thread, NULL);
}
