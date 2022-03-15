#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "HeapDBFile.h"
#include "Defs.h"
#include "SortedDBFile.h"
#include <iostream>
#include <fstream>
// stub file .. replace it with your own DBFile.cc

DBFile::DBFile () {

}

int DBFile::Create (char *file_path, fType type_file, void *begin) {
    char file_metadata[100];
    sprintf (file_metadata, "%s.meta", file_path);
    cout<<"meta data in "<<file_metadata<<endl;
    ofstream metadata_file;
    metadata_file.open(file_metadata);
    OrderMaker* om = nullptr;
    int runLength = 0;
    // write in file type
    if(type_file == heap){
        metadata_file << "heap" << endl;
        genericDB = new HeapDBFile();
    }
    else if(type_file==sorted){
        metadata_file << "sorted"<< endl;
        genericDB = new SortedDBFile();
    }
    /*else if(type_file==tree){
        metadata_file << "tree"<< endl;
        genericDB = new DBFileTree();
    }*/
    if(begin!= nullptr) {
        SortedInfo* sortedInfo = ((SortedInfo*)begin);
        om = sortedInfo->myOrder;
        runLength = sortedInfo->runLength;
        metadata_file << runLength << endl;
        metadata_file << om->numAtts << endl;
        for (int i = 0; i < om->numAtts; i++) {
            metadata_file << om->whichAtts[i] << endl;
            if (om->whichTypes[i] == Int)
                metadata_file << "Int" << endl;
            else if (om->whichTypes[i] == Double)
                metadata_file << "Double" << endl;
            else if(om->whichTypes[i] == String)
                metadata_file << "String" << endl;
        }
        // store orderMaker and runLength into subclass
        if(type_file == heap){

        }
        else if(type_file==sorted){
            ((SortedDBFile*)genericDB)->om = om;
            ((SortedDBFile*)genericDB)->runLength = runLength;
        }
        else if(type_file==tree){

        }
    }
    metadata_file.close();
    int res = genericDB->Create(file_path, type_file, begin);
    return res;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
    genericDB->Load(f_schema, loadpath);
}

int DBFile::Open (char *file_path) {
    OrderMaker* om = new OrderMaker();
    char file_metadata[100];
    sprintf (file_metadata, "%s.meta", file_path);
    ifstream metadata_file(file_metadata);

    string s;
    getline(metadata_file, s);
    if(s.compare("heap")==0){
        genericDB = new HeapDBFile();
    }
    else if(s.compare("sorted")==0){
        genericDB = new SortedDBFile();
        string temporary;
        getline(metadata_file, temporary);
        int runLength = stoi(temporary);
        temporary.clear();
        getline(metadata_file, temporary);
        om->numAtts = stoi(temporary);
        for(int i=0; i<om->numAtts; i++){
            temporary.clear();
            getline(metadata_file, temporary);
            om->whichAtts[i] = stoi(temporary);
            temporary.clear();
            getline(metadata_file, temporary);
            if(temporary.compare("Int")==0){
                om->whichTypes[i] = Int;
            }
            else if(temporary.compare("Double")==0){
                om->whichTypes[i] = Double;
            }
            else if(temporary.compare("String")==0){
                om->whichTypes[i] = String;
            }
        }
        ((SortedDBFile*)genericDB)->om = om;
        ((SortedDBFile*)genericDB)->runLength = runLength;
        om->Print();
    }
    // else if(s.compare("tree")==0){
    //     genericDB = new DBFileTree();
    // }
    metadata_file.close();
    int res = genericDB->Open(file_path);
    return res;
}

void DBFile::MoveFirst () {
    genericDB->MoveFirst();
}

int DBFile::Close () {
    int res = genericDB->Close();
    delete genericDB;
    return res;
}

void DBFile::Add (Record &Record) {
    genericDB->Add(Record);
}

int DBFile::GetNext (Record &bringBack) {
    return genericDB->GetNext(bringBack);
}

int DBFile::GetNext (Record &bringBack, CNF &cnf, Record &literal) {
    return genericDB->GetNext(bringBack, cnf, literal);
}
