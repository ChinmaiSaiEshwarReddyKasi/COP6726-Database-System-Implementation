#include "Schema.h"
#include "Function.h"
#include "Comparison.h"
#include "NodeDataQuery.h"
#include <iostream>
#include <vector>

using namespace std;

NodeDataQuery::NodeDataQuery() : leftNodeId(0),rightNodeId(0),outputNodeId(0),previous(NULL), nodeLeft(NULL), nodeRight(NULL), attStore(NULL), numAttStore(NULL),andList(NULL), cnf(NULL), schema(NULL), om(NULL), fOperator(NULL), func(NULL), selectFile(NULL), selectPipe(NULL), join(NULL), groupBy(NULL), project(NULL), sum(NULL), duplicateRemoval(NULL), writeOut(NULL), dbFile(NULL){
}

void NodeDataQuery::NodeTypeSetter(NodeDataType setter){
    nodeType = setter;
}

NodeDataQuery::~NodeDataQuery(){
}

NodeDataType NodeDataQuery::GetType(){
    return nodeType;
}

void NodeDataQuery::WaitUntilDone(){
    if(nodeType == SELECTF){
        selectFile->WaitUntilDone();
    }
    if(nodeType == SELECTP) {
        selectPipe->WaitUntilDone();
    }
}

std::string NodeDataQuery::nodeTypeGetter(){
    if(nodeType == SELECTF){
        return "SELECT FILE";
    }else if(nodeType == JOIN){
        return "JOIN";
    }else if(nodeType == SELECTP){
        return "SELECT PIPE";
    }else if(nodeType == SUM){
        return "SUM";
    }else if(nodeType == PROJECT){
        return "PROJECT";
    }else if(nodeType == DISTINCT){
        return "DISTINCT";
    }else if(nodeType == GROUP_BY){
        return "GROUP BY";
    }else{
        return "WRITE";
    }
}


void NodeDataQuery::orderPrint(){
    if(nodeLeft != NULL){nodeLeft->orderPrint();}
    if(nodeRight != NULL){nodeRight->orderPrint();}
    nodePrint();
}

void NodeDataQuery::idPrint(NodeDataType type){
    if(type == SELECTP || type == SELECTF || type == PROJECT){
        clog << "Input Pipe " << leftNodeId << endl;
    }else if(type == SUM || type == DISTINCT || type == GROUP_BY){
        clog << "Left Input Pipe " << leftNodeId << endl;
    }
    clog << "Output Pipe " << outputNodeId << endl;
    clog << "Output Schema: " << endl;
    schema->Print();
}


void NodeDataQuery::nodePrint(){
    clog << " *********** " << endl;
    clog << nodeTypeGetter() << " operation" << endl;

    if(nodeType == SELECTF){

        idPrint(SELECTF);
        cnfPrint();

    }else if(nodeType == SELECTP){

        idPrint(SELECTP);
        clog << "SELECTION CNF :" << endl;
        cnfPrint();

    }else if(nodeType == SUM){

        idPrint(SUM);
        clog << endl << "FUNCTION: " << endl;
        func->Print(finalFunction, *schema);

    }else if(nodeType == PROJECT){

        idPrint(PROJECT);
        clog << endl << "************" << endl;

    }else if(nodeType == DISTINCT){

        idPrint(DISTINCT);
        clog << endl << "FUNCTION: " << endl;
        func->Print(finalFunction, *schema);

    }else if(nodeType == JOIN){

        clog << "Left Input Pipe " << leftNodeId << endl;
        clog << "Right Input Pipe " << rightNodeId << endl;
        int check = 0;
        clog << "Output Pipe " << outputNodeId << endl;
        clog << "Output Schema: " << endl;
        check = 1;
        schema->Print();
        clog << endl << "CNF: " << endl;
        cnfPrint();

    }else if(nodeType == GROUP_BY){

        idPrint(GROUP_BY);
        clog << endl << "GROUPING ON " << endl;
        om->Print();
        clog << endl << "FUNCTION " << endl;
        func->Print(finalFunction, *schema);

    }else{
        clog << "Left Input Pipe " << leftNodeId << endl;
        clog << "Output File " << p << endl;
    }

}

void NodeDataQuery::createNodeTree(){
    func = new Function();
    func->GrowFromParseTree(fOperator, *schema);
}


void NodeDataQuery::cnfPrint(){
    int count = 0;
    if (andList){
        struct ComparisonOp *compOp;
        struct OrList *ol;
        struct AndList *al = andList;
        while (al){
            ol = al->left;
            if (al->left){clog << "(";}
            count++;
            while (ol){
                compOp = ol->left;
                count--;
                if (compOp){
                    if (compOp->left){clog << compOp->left->value;}
                    count++;
                    if(compOp->code == 5){
                        clog << " < ";
                    }else if(compOp->code == 6){
                        clog << " > ";
                    }else if(compOp->code == 7){
                        clog << " = ";
                    }
                    if (compOp->right){clog << compOp->right->value;}
                }
                count--;
                if (ol->rightOr){clog << " OR ";}
                ol = ol->rightOr;
                count++;
            }
            if (al->left){clog << ")";}
            if (al->rightAnd) {clog << " AND ";}
            count--;
            al = al->rightAnd;
        }
    }
    count=0;
    clog << endl;
}

void NodeDataQuery::setSchema(){
    schema = new Schema(nodeLeft->schema, nodeRight->schema);
}

void NodeDataQuery::createOrder(int numAtts, vector<int> whichAtts, vector<int> whichTypes){
    om = new OrderMaker();
    int i=0;
    om->numAtts = numAtts;
    while(i < whichAtts.size()){
        om->whichAtts[i] = whichAtts[i];
        om->whichTypes[i] = (Type)whichTypes[i];
        i++;
    }
}
