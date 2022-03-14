#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedDBFile.h"
#include "Defs.h"
#include <chrono>
#include <thread>
#include <pthread.h>
#include <set>
#include <string.h>

SortedDBFile::SortedDBFile () {
    indexOfPage = 0;
    checkifWriting = 0;
    
}

int SortedDBFile::Open (char *file_path) {
    fileOnDisk.Open(1, const_cast<char *>(file_path));
    checkifWriting = 0;
    way_Out = file_path;
    indexOfPage = 0;
    MoveFirst();
    return 1;
}

void SortedDBFile::Load (Schema &mySch, char *path_Load) {
    ComparisonEngine CE;
    Record interim;  
    FILE *f = fopen (path_Load, "r");
    while (interim.SuckNextRecord (&mySch, f) == 1) {
        this->Add(interim);
    }
    fclose(f);
}

void SortedDBFile::writeMode(){
    bCal = 0;
    if(checkifWriting==0) {
        // cout << "begin writeMode " << endl;
        checkifWriting = 1;
        tpmmsStruct *workerArg = new tpmmsStruct;
        workerArg->inputData = in;
        workerArg->outputData = out;
        workerArg->sequence = om;
        workerArg->runlength = runLength;
        thread = new pthread_t();
        pthread_create(thread, NULL, tpmmsMainProcess, (void *) workerArg);
        // cout << "end writeMode " << endl;
    }
}


void SortedDBFile::MoveFirst () {
    readMode();
    indexOfPage = 0;
    pageBuffer.EmptyItOut();
    if (fileOnDisk.GetLength() > 0) {
        fileOnDisk.GetPage(&pageBuffer, indexOfPage);
    }
}

int SortedDBFile::Create (char *file_path, fType file_type, void *begin) {
    // cout<<"begin c Create"<<endl;
    fileOnDisk.Open(0, const_cast<char *>(file_path));
    checkifWriting = 0;
    way_Out = file_path;
    indexOfPage = 0;
    MoveFirst();
    // cout<<"end Create" << endl;
    return 1;
}

int SortedDBFile::Close () {
    readMode();
    pageBuffer.EmptyItOut();
    fileOnDisk.Close();
    if(out!= nullptr)delete out;
    if(in!= nullptr)delete in;
    
    return 1;
}

void SortedDBFile::readMode(){
    if(checkifWriting==1){
        bCal = 0;
        checkifWriting = 0;
        in->ShutDown();
        if(thread!= nullptr) {
            pthread_join (*thread, NULL);
            delete thread;
        }
        char* mergefileName = "temporaryMergeFile.bin";
        char* tempfileName = "temporaryFile.bin";

        DBFile fileCombined;
        DBFile tempFile;
        fileCombined.Create(mergefileName, heap, nullptr);
        tempFile.Open(tempfileName);

        this->MoveFirst();
        Record record1;
        Record record2;
        ComparisonEngine CE1;
        int st1 = tempFile.GetNext(record1);
        int st2 = this->GetNext(record2);
        while(st1 && st2){
            if (CE1.Compare(&record1, &record2, om) < 0){
                fileCombined.Add(record1);
                st1 = tempFile.GetNext(record1);
            }
            else{
                fileCombined.Add(record2);
                st2 = this->GetNext(record2);
            }
        }
        while(st1){
            fileCombined.Add(record1);
            st1 = tempFile.GetNext(record1);
        }
        while(st2){
            fileCombined.Add(record2);
            st2 = this->GetNext(record2);
        }
        tempFile.Close();
        fileOnDisk.Close();
        fileCombined.Close();
        remove(way_Out);
        remove(tempfileName);
        rename(mergefileName, way_Out);
    }
}

void SortedDBFile::Add (Record &append) {
    writeMode();
    in->Insert(&append);
}

int SortedDBFile::GetNext (Record &bringBack) {
    //If file is writing, then write page into disk based file, redirect page ot first page
    readMode();
    if (pageBuffer.GetFirst(&bringBack) == 0) {
        indexOfPage = indexOfPage + 1;
        if (indexOfPage >= fileOnDisk.GetLength() - 1) {
            return 0;
        }
        pageBuffer.EmptyItOut();
        fileOnDisk.GetPage(&pageBuffer, indexOfPage);
        pageBuffer.GetFirst(&bringBack);
    }
    return 1;
}

int SortedDBFile::GetNext (Record &bringBack, CNF &cnf, Record &literal) {  

    ComparisonEngine CE;
    if(!bCal) {
        bCal = 1;
        set<int> setOfAttributes;
        int lBound = 0;
        int hBound = fileOnDisk.GetLength() - 2;
        cout << "Higher bound for original data: " << hBound;
        cout << "Lower bound for original data: " << lBound;
        
        for (int i = 0; i < om->numAtts; i++) {
            setOfAttributes.insert(om->whichAtts[i]);
        }
        
        for (int i = 0; i < cnf.numAnds; i++) {
            for (int j = 0; j < cnf.orLens[i]; j++) {
                if (setOfAttributes.find(cnf.orList[i][j].whichAtt1) == setOfAttributes.end()) continue;
                if (cnf.orList[i][j].op == GreaterThan) {
                    Record record;
                    int l = 0;
                    int r = fileOnDisk.GetLength() - 2;
                    while (l < r) {
                        int mid = l + (r - l) / 2;
                        fileOnDisk.GetPage(&pageBuffer, mid);
                        pageBuffer.GetFirst(&record);
                        int answer = CompareRecords(&record, &literal, &cnf.orList[i][j]);
                        if (answer != 0) {
                            r = mid;
                        } else {
                            l = mid + 1;
                        }
                    }
                    //update the global lower and upper bound
                    lBound = max(lBound, l);
                }
                    // calculate the lower bound for a UpperThan
                else if (cnf.orList[i][j].op == LessThan) {
                    Record record;
                    int l = 0;
                    int r = fileOnDisk.GetLength() - 2;
                    while (l < r) {
                        int mid = l + (r - l + 1) / 2;
                        fileOnDisk.GetPage(&pageBuffer, mid);
                        pageBuffer.GetFirst(&record);
                        int answer = CompareRecords(&record, &literal, &cnf.orList[i][j]);
                        if (answer != 0) {
                            l = mid;
                        } else {
                            r = mid - 1;
                        }
                    }
                    //update the global lower and upper bound
                    hBound = min(hBound, r);
                }
            }
        }
        lBound = lBound - 1; // left shift for a page
        lBound = max(0, lBound);

        cout << "Higher bound after Binary Search: " << hBound;
        cout << "Lower bound after Binary Search: " << lBound;
        boundSmall = lBound;
        boundLarge = hBound;
        indexOfPage = lBound;
    }
    //If not reach the end of file

    while (GetNext(bringBack) == 1) {
        if (indexOfPage > boundLarge) return 0;
        if (CE.Compare(&bringBack, &literal, &cnf) == 1)
            return 1;
    }
    return 0;
}

int SortedDBFile :: CompareRecords (Record *l, Record *lit, Comparison *comp) {

    char *c1;
    char *c2;
    double c1D;
    double c2D;
    int c1I;
    int c2I;
    int interimResult;
    char *bitLiterals = lit->GetBits();
    char *bitsOnLeft = l->GetBits();
    
    if (comp->operand2 == Left) {
        c2 = bitsOnLeft + ((int *) bitsOnLeft)[comp->whichAtt2 + 1];
    } else {
        c2 = bitLiterals + ((int *) bitLiterals)[comp->whichAtt2 + 1];
    }

    if (comp->operand1 == Left) {
        c1 = bitsOnLeft + ((int *) bitsOnLeft)[comp->whichAtt1 + 1];
    } else {
        c1 = bitLiterals + ((int *) bitLiterals)[comp->whichAtt1 + 1];
    }
    
    switch (comp->attType) {
        case Int:

            c1I = *((int *) c1);
            c2I = *((int *) c2);
            switch (comp->op) {

                case GreaterThan:
                    return (c1I > c2I);
                    break;
                
                case LessThan:
                    return (c1I < c2I);
                    break;

                default:
                    return (c1I == c2I);
                    break;
            }
            break;
        case Double:
            c2D = *((double *) c2);
            c1D = *((double *) c1);
            switch (comp->op) {

                case GreaterThan:
                    return (c1D > c2D);
                    break;

                case LessThan:
                    return (c1D < c2D);
                    break;

                default:
                    return (c1D == c2D);
                    break;
            }
            break;
        default:
            interimResult = strcmp (c1, c2);
            switch (comp->op) {

                case GreaterThan:
                    return interimResult > 0;
                    break;

                case LessThan:
                    return interimResult < 0;
                    break;

                default:
                    return interimResult == 0;
                    break;
            }
            break;
    }

}