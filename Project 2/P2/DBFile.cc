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

int DBFile::Create (char *f_path, fType f_type, void *startup) {
    ofstream metaFile;
    char fileName[100];
    int runLength = 0;
    OrderMaker* om = nullptr;
    sprintf (fileName, "%s.meta", f_path);
    cout<<"meta data in "<<fileName<<endl;
    metaFile.open(fileName);

    if(f_type == sorted){
        metaFile << "sorted"<< endl;
        genericDB = new SortedDBFile();
    }
    else if(f_type==heap){		
        metaFile << "heap" << endl;
        genericDB = new HeapDBFile();
    }
    /*else if(f_type==tree){
        metaFile << "tree"<< endl;
        genericDB = new DBFileTree();
    }*/

    if(startup!= nullptr) {
        SortedInfo* s = ((SortedInfo*)startup);
        om = s->myOrder;
        runLength = s->runLength;
        metaFile << runLength << endl;
        metaFile << om->numAtts << endl;
        for (int i = 0; i < om->numAtts; i++) {
            metaFile << om->whichAtts[i] << endl;
            if (om->whichTypes[i] == String)
                metaFile << "String" << endl;
            else if (om->whichTypes[i] == Int)
                metaFile << "Int" << endl;
            else if(om->whichTypes[i] == Double)
                metaFile << "Double" << endl;
        }
        // store om and runLength into subclass
        if(f_type == heap){}
        else if(f_type==sorted){
            ((SortedDBFile*)genericDB)->om = om;
            ((SortedDBFile*)genericDB)->runLength = runLength;
        }
        else if(f_type==tree){}
    }
    metaFile.close();
    int res = genericDB->Create(f_path, f_type, startup);
    return res;
}

void DBFile::Load (Schema &f_schema, char *loadpath) {
    genericDB->Load(f_schema, loadpath);
}

int DBFile::Open (char *f_path) {
    string s;
    char fileName[100];
    ifstream metaFile(fileName);
    sprintf (fileName, "%s.meta", f_path);
    OrderMaker* om = new OrderMaker();

    getline(metaFile, s);
    if(s.compare("heap")==0){
        genericDB = new HeapDBFile();
    }
    else if(s.compare("sorted")==0){
        genericDB = new SortedDBFile();
        string t;
        getline(metaFile, t);
        int runLength = stoi(t);
        t.clear();
        getline(metaFile, t);
        om->numAtts = stoi(t);
        for(int i=0; i<om->numAtts; i++){
            t.clear();
            getline(metaFile, t);
            om->whichAtts[i] = stoi(t);
            t.clear();
            getline(metaFile, t);
            if(t.compare("Int")==0){
                om->whichTypes[i] = Int;
            }
            else if(t.compare("Double")==0){
                om->whichTypes[i] = Double;
            }
            else if(t.compare("String")==0){
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
    metaFile.close();
    int res = genericDB->Open(f_path);
    return res;
}

void DBFile::MoveFirst () {
    genericDB->MoveFirst();
}

int DBFile::Close () {
    int ans = genericDB->Close();
    delete genericDB;
    return ans;
}

void DBFile::Add (Record &rec) {
    genericDB->Add(rec);
}

int DBFile::GetNext (Record &fetchme) {
    return genericDB->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return genericDB->GetNext(fetchme, cnf, literal);
}
