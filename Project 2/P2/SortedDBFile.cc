//
// Created by LJJ on 2/24/21.
//

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

// stub file .. replace it with your own SortedDBFile.cc

SortedDBFile::SortedDBFile () {
    indexOfPage = 0;
    checkifWriting = 0;
    
}

int SortedDBFile::Create (char *file_path, fType file_type, void *begin) {
    cout<<"begin c Create"<<endl;
    fileOnDisk.Open(0, const_cast<char *>(file_path));
    way_Out = file_path;
    checkifWriting = 0;
    indexOfPage = 0;
    MoveFirst();
    cout<<"end Create" << endl;
    return 1;
}


void SortedDBFile::Load (Schema &mySch, char *path_Load) {
    FILE *fileonTable = fopen (path_Load, "r");
    ComparisonEngine CE;
    Record interim;
    

    while (interim.SuckNextRecord (&mySch, fileonTable) == 1) {
        this->Add(interim);
    }
//    in->ShutDown();
    fclose(fileonTable);
}

int SortedDBFile::Open (char *file_path) {
    fileOnDisk.Open(1, const_cast<char *>(file_path));
    way_Out = file_path;
    indexOfPage = 0;
    
    //Reading mode on default
    checkifWriting = 0;
    MoveFirst();
    return 1;
}

void SortedDBFile::MoveFirst () {
    readMode();
    indexOfPage = 0;
    pageBuffer.EmptyItOut();
    //If DBfile is not empty
    if (fileOnDisk.GetLength() > 0) {
        fileOnDisk.GetPage(&pageBuffer, indexOfPage);
    }
}

int SortedDBFile::Close () {
    readMode();
    pageBuffer.EmptyItOut();
    fileOnDisk.Close();
    if(out!= nullptr)delete out;
    if(in!= nullptr)delete in;
    
    return 1;
}

void SortedDBFile::Add (Record &append) {
//    cout<< "begin Add " << endl;

    writeMode();
    in->Insert(&append);
//    cout<< "end Add " << endl;
}

int SortedDBFile::GetNext (Record &bringBack) {
    //If file is writing, then write page into disk based file, redirect page ot first page
    readMode();
    //If reach the end of page
//    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    if (pageBuffer.GetFirst(&bringBack) == 0) {
//        cout<<fileOnDisk.GetLength()<<endl;
        indexOfPage++;
        //If reach the end of file
        if (indexOfPage >= fileOnDisk.GetLength() - 1) {
            return 0;
        }
        //Else get next page
        pageBuffer.EmptyItOut();
        fileOnDisk.GetPage(&pageBuffer, indexOfPage);
        pageBuffer.GetFirst(&bringBack);
    }
    return 1;
}

int SortedDBFile::GetNext (Record &bringBack, CNF &cnf, Record &literal) {
    if(!bCal) {
        bCal = 1;
        set<int> setOfAttributes;
        for (int i = 0; i < orderMaker->numAtts; i++) {
            setOfAttributes.insert(orderMaker->whichAtts[i]);
        }
        int loGlobal = 0, hiGlobal = fileOnDisk.GetLength() - 2;
        cout << "original higher bound: " << hiGlobal << endl;
        cout << "original lower bound: " << loGlobal << endl;
        
        for (int i = 0; i < cnf.numAnds; i++) {
            for (int j = 0; j < cnf.orLens[i]; j++) {
                if (setOfAttributes.find(cnf.orList[i][j].whichAtt1) == setOfAttributes.end()) continue;
                //calculate the lower bound and upper bound by Binary Search
                // calculate the upper bound for a LessThan
                if (cnf.orList[i][j].op == LessThan) {
                    int left = 0, right = fileOnDisk.GetLength() - 2;
                    Record record;
                    while (left < right) {
                        int mid = left + (right - left + 1) / 2;
                        fileOnDisk.GetPage(&pageBuffer, mid);
                        pageBuffer.GetFirst(&record);
                        int answer = Run(&record, &literal, &cnf.orList[i][j]);
                        if (answer != 0) {
                            left = mid;
                        } else {
                            right = mid - 1;
                        }
                    }
                    //update the global lower and upper bound
                    hiGlobal = min(hiGlobal, right);
                }
                    // calculate the lower bound for a UpperThan
                else if (cnf.orList[i][j].op == GreaterThan) {
                    int left = 0, right = fileOnDisk.GetLength() - 2;
                    Record record;
                    while (left < right) {
                        int mid = left + (right - left) / 2;
                        fileOnDisk.GetPage(&pageBuffer, mid);
                        pageBuffer.GetFirst(&record);
                        int answer = Run(&record, &literal, &cnf.orList[i][j]);
                        if (answer != 0) {
                            right = mid;
                        } else {
                            left = mid + 1;
                        }
                    }
                    //update the global lower and upper bound
                    loGlobal = max(loGlobal, left);
                }
            }
        }
        loGlobal = loGlobal - 1; // left shift for a page
        loGlobal = max(0, loGlobal);

        cout << "updated lower bound by Binary Search: " << loGlobal << endl;
        cout << "updated higher bound by Binary Search: " << hiGlobal << endl;
        boundSmall = loGlobal;
        boundLarge = hiGlobal;
        indexOfPage = loGlobal;
    }

    ComparisonEngine CE;
    //If not reach the end of file

    while (GetNext(bringBack) == 1) {
        if (indexOfPage > boundLarge) return 0;
        if (CE.Compare(&bringBack, &literal, &cnf) == 1)
            return 1;
    }
    return 0;
}

void SortedDBFile::writeMode(){
    bCal = 0;
    if(checkifWriting==0) {
        cout << "begin writeMode " << endl;
        checkifWriting = 1;
    //    ThreadArg arg2 = {way_Out, out};
    //    thread2 = new pthread_t();
    //    pthread_create (thread2, NULL, consumer, (void*)&arg2);
    //    bigQ = new BigQ(*in, *out, *orderMaker, runLength);
        //Construct arguement used for worker thread
        tpmmsStruct *workerArg = new tpmmsStruct;
        workerArg->inputData = in;
        workerArg->outputData = out;
        workerArg->sequence = orderMaker;
        workerArg->runlength = runLength;
        thread = new pthread_t();
        pthread_create(thread, NULL, tpmmsMainProcess, (void *) workerArg);
        cout << "end writeMode " << endl;
    }
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
        char* mergefile = "tempMergedFile.bin";
        char* f_dif = "tempDifFile.bin";
        // merge
        DBFile fileCombined;
        fileCombined.Create(mergefile, heap, nullptr);

        DBFile difFile;
        difFile.Open(f_dif);

        this->MoveFirst();
        Record record1;
        Record record2;
        ComparisonEngine CE1;
        int st1 = difFile.GetNext(record1);
        int st2 = this->GetNext(record2);
        while(st1 && st2){
            if (CE1.Compare(&record1, &record2, orderMaker) < 0){
                fileCombined.Add(record1);
                st1 = difFile.GetNext(record1);
            }
            else{
                fileCombined.Add(record2);
                st2 = this->GetNext(record2);
            }
        }
        while(st1){
            fileCombined.Add(record1);
            st1 = difFile.GetNext(record1);
        }
        while(st2){
            fileCombined.Add(record2);
            st2 = this->GetNext(record2);
        }
        difFile.Close();
        fileCombined.Close();
        fileOnDisk.Close();
        remove(f_dif);
        remove(way_Out);
        rename(mergefile, way_Out);
    }
}

//void* SortedDBFile::consumer(void *arg2) {
//    cout<< "begin Consumer Thread" << endl;
//    ThreadArg* arg = (ThreadArg *)arg2;
//    char bigQPath[100];
//    sprintf(bigQPath, "%s.bigq", arg->way_Out);
////    cout<< "=====bigQPath" << bigQPath << endl;
//    DBFile dbFileHeap;
//    cout<< "1" << endl;
//    dbFileHeap.Create(bigQPath, heap, nullptr);
//    cout<< "2" << endl;
//    Record record;
//    while(arg->out->Remove(&record)){
//        cout<< "5" << endl;
//        dbFileHeap.Add(record);
//    }
//    cout<< "3" << endl;
//    dbFileHeap.Close();
//    cout<< "4" << endl;
//    cout<< "end Consumer Thread" << endl;
//}

int SortedDBFile :: Run (Record *left, Record *literal, Comparison *c) {

    char *val1, *val2;
    char *bitLiterals = literal->GetBits();
    char *bitsOnLeft = left->GetBits();
    

    // first get a pointer to the first value to compare
    if (c->operand1 == Left) {
        val1 = bitsOnLeft + ((int *) bitsOnLeft)[c->whichAtt1 + 1];
    } else {
        val1 = bitLiterals + ((int *) bitLiterals)[c->whichAtt1 + 1];
    }

    // next get a pointer to the second value to compare
    if (c->operand2 == Left) {
        val2 = bitsOnLeft + ((int *) bitsOnLeft)[c->whichAtt2 + 1];
    } else {
        val2 = bitLiterals + ((int *) bitLiterals)[c->whichAtt2 + 1];
    }

    double val1Double, val2Double;
    int val1Integer, val2Integer, interimResult;
    

    // now check the type and the comparison operation
    switch (c->attType) {

        // first case: we are dealing with integers
        case Int:

            val1Integer = *((int *) val1);
            val2Integer = *((int *) val2);

            // and check the operation type in order to actually do the comparison
            switch (c->op) {

                case LessThan:
                    return (val1Integer < val2Integer);
                    break;

                case GreaterThan:
                    return (val1Integer > val2Integer);
                    break;

                default:
                    return (val1Integer == val2Integer);
                    break;
            }
            break;

            // second case: dealing with doubles
        case Double:
            val2Double = *((double *) val2);
            val1Double = *((double *) val1);
           

            // and check the operation type in order to actually do the comparison
            switch (c->op) {

                case LessThan:
                    return (val1Double < val2Double);
                    break;

                case GreaterThan:
                    return (val1Double > val2Double);
                    break;

                default:
                    return (val1Double == val2Double);
                    break;
            }
            break;

            // final case: dealing with strings
        default:

            // so check the operation type in order to actually do the comparison
            interimResult = strcmp (val1, val2);
            switch (c->op) {

                case LessThan:
                    return interimResult < 0;
                    break;

                case GreaterThan:
                    return interimResult > 0;
                    break;

                default:
                    return interimResult == 0;
                    break;
            }
            break;
    }

}