#include <string>
#include <cstring>
#include <fstream>
#include <iostream>

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

DBFile :: DBFile () {}
DBFile :: ~DBFile () {
    delete myInternalVar;
}

int DBFile :: Create (const char *fpath, fType ftype, void *startup) {

    char metaDatapath[100];
    sprintf (metaDatapath, "%s.md", fpath);
    ofstream metaDataStream;
    metaDataStream.open (metaDatapath);

    switch(ftype){
        case heap:{
            metaDataStream << "heap" << endl;
            myInternalVar = new Heap;
            break;}

        case sorted:{
            metaDataStream << "sorted" << endl;
            SortInfo* sortInfo = (SortInfo *) startup;
            metaDataStream << sortInfo->runLength << endl;
            metaDataStream << sortInfo->myOrder->numAtts << endl;
            sortInfo->myOrder->PrintInOfstream (metaDataStream);
            myInternalVar = new Sorted (sortInfo->myOrder, sortInfo->runLength);
            break;}

        default:{
            metaDataStream.close ();
            cout << "Invalid File Type: " << fpath << endl;
            return 0;}
    }

    metaDataStream.close ();
    myInternalVar->Create (fpath);
    return 1;
}

int DBFile :: Open (char *fpath) {

    char *metaDatapath = new char[100];

    sprintf (metaDatapath, "%s.md", fpath);
    ifstream md;
    md.open (metaDatapath);
    string str;
    int attNum = 0;

    if (md.is_open ()) {

        md >> str;


        if ( str == "heap") {
            myInternalVar = new Heap;
        } else if (str == "sorted"){

            OrderMaker *orderMaker = new OrderMaker;
            int runLen;

            md >> runLen;
            md >> orderMaker->numAtts;

            int i = 0;
            while(i<orderMaker->numAtts){
                md >> attNum;
                md >> str;

                orderMaker->whichAtts[i] = attNum;

                if (str == "Int" ) {
                    orderMaker->whichTypes[i] = Int;
                } else if (str == "Double" ) {
                    orderMaker->whichTypes[i] = Double;
                } else if ( str == "String" ) {
                    orderMaker->whichTypes[i] = String;
                } else {
                    cout << "Invalid Metadata for the sorted File: " << fpath << endl;
                    delete orderMaker;
                    md.close ();
                    return 0;
                }
                i++;
            }
            myInternalVar = new Sorted (orderMaker, runLen);
        }  else {
            md.close ();
            cout << "Invalid file type in metadata of file (" << fpath << ")" << endl;
            return 0;
        }

    } else {
        md.close ();
        cout << "Unable to open the file (" << fpath << ")!!" << endl;
        return 0;
    }
    myInternalVar->Open (fpath);
    md.close ();
    return 1;
}


void DBFile :: Load (Schema &myschema, const char *loadpath) {
    myInternalVar->Load (myschema, loadpath);
}

void DBFile :: Add (Record &addme) {
    myInternalVar->Add (addme);
}

void DBFile :: MoveFirst () {
    myInternalVar->MoveFirst ();
}

int DBFile :: GetNext (Record &fetchme) {
    return myInternalVar->GetNext (fetchme);
}

int DBFile :: Close () {
    return myInternalVar->Close ();
}

int DBFile :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return myInternalVar->GetNext (fetchme, cnf, literal);
}

Heap :: Heap () {
    currentPage = new Page ();
    file = new File ();
}

int Heap :: Create (const char *fpath) {
    this->fpath = (char *)fpath;
    strcpy (this->fpath, (char *)fpath);
    currentPageIndex = 0;
    currentPage->EmptyItOut ();
    file->Open (0, this->fpath);
    writingMode = true;
    return 1;

}

void Heap :: Load (Schema &f_schema, const char *loadpath) {

    FILE *inpFile = fopen((char *)loadpath, "r");

    //To handle the failure case of file opening.
    if(inpFile == 0){
        exit(EXIT_FAILURE);
    }

    int recordsCount =0, pagesCount =0;
    Record record;
    Page page;

    //To fetch each record from the file and append to page.
    while(record.SuckNextRecord(&f_schema, inpFile)==1){
        recordsCount++;
        if(page.Append(&record)==0){    //Page is full
            file->AddPage(&page, pagesCount++);  //Add the page to file
            page.EmptyItOut();  //Empty page.
            page.Append(&record);   //Add the record to page.
        }
    }

    file->AddPage(&page, pagesCount++);  //Add the page to the file.
    cout<< "Fetched "<< recordsCount << " records, into "<< pagesCount <<" pages. "<< endl;

}

int Heap :: Open (char *fpath) {

    this->fpath = (char *)fpath;
    file->Open (1, (char *)fpath);
    currentPage->EmptyItOut ();
    currentPageIndex = (off_t) 0;
    return 1;
}

void Heap :: MoveFirst () {
    if (writingMode && currentPage->GetNumRecs () > 0) {
        FlushCurrentPage();
        writingMode = !writingMode;
    }
    currentPage->EmptyItOut ();
    currentPageIndex = (off_t) 0;
    file->GetPage (currentPage, currentPageIndex);
}

int Heap :: Close () {
    if (writingMode && currentPage->GetNumRecs () > 0) {
        FlushCurrentPage();
        writingMode = false;
    }
    file->Close ();
    return 1;
}

void Heap :: Add (Record &rec) {
    if (! (currentPage->Append (&rec))) {
        FlushCurrentPage();
        currentPage->Append (&rec);
    }
}

int Heap :: GetNext (Record &fetchme) {

    while (!currentPage->GetFirst(&fetchme)) {
        if (currentPageIndex == file->GetLength() - 2) {     //If the page is the last page of file.
            return 0;
        }
        else {
            file->GetPage(currentPage, ++ currentPageIndex);     //Get next page.
        }
    }
    return 1;

}

int Heap :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {

    ComparisonEngine comparisonEngine;
    while(GetNext(fetchme) == 1)
    {
        if (comparisonEngine.Compare(&fetchme,&literal,&cnf))
            return 1;
    }
    return 0;

}

void Heap :: FlushCurrentPage() {
    file->AddPage (currentPage, currentPageIndex++);
    currentPage->EmptyItOut ();
}

Heap :: ~Heap () {
    delete file;
    delete currentPage;
}

Sorted :: Sorted (OrderMaker *order, int runLen) {
    this->order = order;
    this->runLength = runLen;
    this->query = NULL;
    this->currentPage = new Page ();
    this->bigq = NULL;
    this->file = new File ();
    this->buffsize = 100;
}

Sorted :: ~Sorted () {
    delete currentPage, file, query;
}

int Sorted :: Create (const char *fpath) {
    this->fpath = (char *)fpath;
    currentPage->EmptyItOut ();
    file->Open (0, this->fpath);
    writingMode = false;
    currentPageIndex = 0;
    return 1;

}

int Sorted :: Open (char *fpath) {
    currentPageIndex = (off_t)0;
    this->fpath = (char *)fpath;
    currentPage->EmptyItOut ();
    writingMode = false;
    file->Open (1, (char *)fpath);
    if (file->GetLength () > 0) {
        file->GetPage (currentPage, currentPageIndex);
    }
    return 1;
}

int Sorted :: Close () {
    if (writingMode == true)
        TwoWayMerge ();

    return file->Close ();
}

void Sorted :: Add (Record &addme) {

    if (writingMode == false)
        InitBigQ ();

    inPipe->Insert (&addme);
}

void Sorted :: Load (Schema &myschema, const char *loadpath) {

    FILE *inpFile = fopen((char *)loadpath, "r");

    //To handle the failure case of file opening.
    if(inpFile == 0){
        exit(EXIT_FAILURE);
    }

    int recordsCount =0, pagesCount =0;
    Record record;
    Page page;

    //To fetch each record from the file and append to page.
    while(record.SuckNextRecord(&myschema, inpFile)==1){
        recordsCount++;
        if(page.Append(&record)==0){    //Page is full
            file->AddPage(&page, pagesCount++);  //Add the page to file
            page.EmptyItOut();  //Empty page.
            page.Append(&record);   //Add the record to page.
        }
    }

    file->AddPage(&page, pagesCount++);  //Add the page to the file.
}

void Sorted :: MoveFirst () {

    if (writingMode == true) {
        TwoWayMerge ();
    } else {
        currentPageIndex = 0;
        currentPage->EmptyItOut ();
        file->GetPage (currentPage, currentPageIndex);

        if (query!=NULL)
            delete query;
    }
}

int Sorted :: GetNext (Record &fetchme, CNF &cnf, Record &literal) {

    if ( writingMode == true ) {
        TwoWayMerge ();
    }

    if (!query) {

        query = new OrderMaker;
        return QueryOrderMaker (*query, *order, cnf) > 0 ?  BinarySearch (fetchme, cnf, literal) : GetNextInSequence (fetchme, cnf, literal);

    } else {
        return query->numAtts == 0? GetNextInSequence (fetchme, cnf, literal): GetNextOfQuery (fetchme, cnf, literal);
    }

}

int Sorted :: GetNext (Record &fetchme) {

    if (writingMode == true)
        TwoWayMerge ();

    if (!currentPage->GetFirst(&fetchme)) {
        if (++currentPageIndex < file->GetLength() - 1) {     //If the page is the last page of file.
            file->GetPage(currentPage, currentPageIndex);
            currentPage->GetFirst(&fetchme);
        }
        else {
            return 0;
        }
    }
    return 1;
}



int Sorted :: GetNextOfQuery (Record &fetchme, CNF &cnf, Record &literal) {

    ComparisonEngine comparisonEngine;
    while (GetNext (fetchme) && !comparisonEngine.Compare (&literal, query, &fetchme, order)) {
        if (comparisonEngine.Compare (&fetchme, &literal, &cnf))
            return 1;
    }
    return 0;
}

int Sorted :: GetNextInSequence (Record &fetchme, CNF &cnf, Record &literal) {

    ComparisonEngine comparisonEngine;

    while (GetNext (fetchme) == 1) {

        if (comparisonEngine.Compare (&fetchme, &literal, &cnf) == 1)
            return 1;
    }
    return 0;
}

int Sorted :: BinarySearch(Record &fetchme, CNF &cnf, Record &literal) {

    off_t mid = 0;
    off_t first = currentPageIndex;
    off_t last = file->GetLength () - 1;

    Page *page = new Page;

    ComparisonEngine comp;

    while (true) {

        if(last > first) {
            mid = first + (last - first) / 2;
            file->GetPage(page, mid);

            if (page->GetFirst(&fetchme)) {

                if (comp.Compare(&literal, query, &fetchme, order) > 0) {
                    first = ++mid;
                } else {
                    last = --mid;
                }
            } else
                break;
        }
        else
            break;
    }

    if (comp.Compare (&fetchme, &literal, &cnf)) {
        currentPage = page;
        currentPageIndex = mid;
        delete currentPage;
        return 1;

    } else {
        delete page;
        return 0;
    }

}


void Sorted :: InitBigQ () {
    inPipe = new Pipe (buffsize);
    outPipe = new Pipe (buffsize);
    bigq = new BigQ(*inPipe, *outPipe, *order, runLength);
    writingMode = true;
}

void Sorted :: TwoWayMerge () {

    inPipe->ShutDown ();
    Record *pipeRecord = new Record;
    Record *fileRecord = new Record;
    writingMode = false;

    if (file->GetLength () > 0) {
        MoveFirst ();
    }
    int flagPipe = outPipe->Remove (pipeRecord);
    int flagFile = GetNext (*fileRecord);

    Heap *newFile = new Heap;
    newFile->Create ("bin/temp.bin");



    ComparisonEngine comparisonEngine;

    while (flagFile && flagPipe) {

        if (comparisonEngine.Compare (pipeRecord, fileRecord, order) <= 0) {
            newFile->Add (*pipeRecord);
            flagPipe = outPipe->Remove (pipeRecord);
        } else {
            newFile->Add (*fileRecord);
            flagFile = GetNext (*fileRecord);
        }

    }

    for(;flagFile!=NULL;flagFile = GetNext(*fileRecord))
        newFile->Add (*fileRecord);

    for(;flagPipe!=NULL;flagPipe = outPipe->Remove (pipeRecord))
        newFile->Add (*pipeRecord);

    newFile->Close ();
    delete newFile;
    outPipe->ShutDown ();


    remove (fpath);
    rename ("bin/temp.bin", fpath);
    file->Close ();
    file->Open (1, fpath);
    MoveFirst ();
}

int Sorted :: QueryOrderMaker (OrderMaker &query, OrderMaker &order, CNF &cnf) {

    bool flag = false;
    query.numAtts = 0;
    int i=0,j=0;
    while(i<order.numAtts){
        i++; j=0;
        while(j< cnf.numAnds){
            j++;
            if (cnf.orLens[j] != 1 || cnf.orList[j][0].op != Equals || (cnf.orList[i][0].operand1 == Left && cnf.orList[i][0].operand2 == Left) ||
                (cnf.orList[i][0].operand1==Left && cnf.orList[i][0].operand2 == Right) ||
                (cnf.orList[i][0].operand1==Right && cnf.orList[i][0].operand2 == Left) ||
                (cnf.orList[i][0].operand2 == Right && cnf.orList[i][0].operand1 == Right)) {
                continue;
            }


            if (cnf.orList[j][0].operand1 == Left &&
                cnf.orList[j][0].whichAtt1 == order.whichAtts[i]) {

                query.whichAtts[query.numAtts] = cnf.orList[i][0].whichAtt2;
                query.whichTypes[query.numAtts++] = cnf.orList[i][0].attType;


                flag = true;

                break;

            }

            if (cnf.orList[j][0].operand2 == Left &&
                cnf.orList[j][0].whichAtt2 == order.whichAtts[i]) {

                query.whichAtts[query.numAtts] = cnf.orList[i][0].whichAtt1;
                query.whichTypes[query.numAtts++] = cnf.orList[i][0].attType;


                flag = true;

                break;

            }

        }

        if (flag == false) {
            break;
        }

    }

    return query.numAtts;

}

GenericDBFile :: ~GenericDBFile () {}
