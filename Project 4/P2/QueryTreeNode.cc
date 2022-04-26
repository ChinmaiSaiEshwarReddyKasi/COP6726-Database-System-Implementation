#include "Schema.h"
#include "Function.h"
#include "Comparison.h"
#include "QueryTreeNode.h"
#include <iostream>
#include <vector>

using namespace std;

QueryTreeNode::QueryTreeNode() : lChildPipeID(0),rChildPipeID(0),outPipeID(0),parent(NULL), left(NULL), right(NULL), attsToKeep(NULL), numAttsToKeep(NULL),cnf(NULL), opCNF(NULL), schema(NULL), order(NULL), funcOp(NULL), func(NULL), sf(NULL), sp(NULL), j(NULL), gb(NULL), p(NULL), s(NULL), d(NULL), h(NULL), db(NULL){
}

void QueryTreeNode::SetType(QueryNodeType setter){
    type = setter;
}

QueryTreeNode::~QueryTreeNode(){
}

QueryNodeType QueryTreeNode::GetType(){
    return type;
}

void QueryTreeNode::WaitUntilDone(){
    if(type == SELECTF){
        sf->WaitUntilDone();
    }
    if(type == SELECTP) {
        sp->WaitUntilDone();
    }
}

std::string QueryTreeNode::GetTypeName(){
    if(type == SELECTF){
        return "SELECT FILE";
    }else if(type == JOIN){
        return "JOIN";
    }else if(type == SELECTP){
        return "SELECT PIPE";
    }else if(type == SUM){
        return "SUM";
    }else if(type == PROJECT){
        return "PROJECT";
    }else if(type == DISTINCT){
        return "DISTINCT";
    }else if(type == GROUP_BY){
        return "GROUP BY";
    }else{
        return "WRITE";
    }
}


void QueryTreeNode::PrintInOrder(){
    if(left != NULL){left->PrintInOrder();}
    if(right != NULL){right->PrintInOrder();}
    PrintNode();
}

void QueryTreeNode::printIDs(QueryNodeType type){
    if(type == SELECTP || type == SELECTF || type == PROJECT){
        clog << "Input Pipe " << lChildPipeID << endl;
    }else if(type == SUM || type == DISTINCT || type == GROUP_BY){
        clog << "Left Input Pipe " << lChildPipeID << endl;
    }
    clog << "Output Pipe " << outPipeID << endl;
    clog << "Output Schema: " << endl;
    schema->Print();
}


void QueryTreeNode::PrintNode(){
    clog << " *********** " << endl;
    clog << GetTypeName() << " operation" << endl;

    if(type == SELECTF){

        printIDs(SELECTF);
        PrintCNF();

    }else if(type == SELECTP){

        printIDs(SELECTP);
        clog << "SELECTION CNF :" << endl;
        PrintCNF();

    }else if(type == SUM){

        printIDs(SUM);
        clog << endl << "FUNCTION: " << endl;
        func->Print(finalFunction, *schema);

    }else if(type == PROJECT){

        printIDs(PROJECT);
        clog << endl << "************" << endl;

    }else if(type == DISTINCT){

        printIDs(DISTINCT);
        clog << endl << "FUNCTION: " << endl;
        func->Print(finalFunction, *schema);

    }else if(type == JOIN){

        clog << "Left Input Pipe " << lChildPipeID << endl;
        clog << "Right Input Pipe " << rChildPipeID << endl;
        int check = 0;
        clog << "Output Pipe " << outPipeID << endl;
        clog << "Output Schema: " << endl;
        check = 1;
        schema->Print();
        clog << endl << "CNF: " << endl;
        PrintCNF();

    }else if(type == GROUP_BY){

        printIDs(GROUP_BY);
        clog << endl << "GROUPING ON " << endl;
        order->Print();
        clog << endl << "FUNCTION " << endl;
        func->Print(finalFunction, *schema);

    }else{
        clog << "Left Input Pipe " << lChildPipeID << endl;
        clog << "Output File " << path << endl;
    }

}

void QueryTreeNode::GenerateFunction(){
    func = new Function();
    func->GrowFromParseTree(funcOp, *schema);
}


void QueryTreeNode::PrintCNF(){
    int check = 0;
    if (cnf){
        struct ComparisonOp *curOp;
        struct OrList *curOr;
        struct AndList *curAnd = cnf;
        while (curAnd){
            curOr = curAnd->left;
            if (curAnd->left){clog << "(";}
            check++;
            while (curOr){
                curOp = curOr->left;
                check--;
                if (curOp){
                    if (curOp->left){clog << curOp->left->value;}
                    check++;
                    if(curOp->code == 5){
                        clog << " < ";
                    }else if(curOp->code == 6){
                        clog << " > ";
                    }else if(curOp->code == 7){
                        clog << " = ";
                    }
                    if (curOp->right){clog << curOp->right->value;}
                }
                check--;
                if (curOr->rightOr){clog << " OR ";}
                curOr = curOr->rightOr;
                check++;
            }
            if (curAnd->left){clog << ")";}
            if (curAnd->rightAnd) {clog << " AND ";}
            check--;
            curAnd = curAnd->rightAnd;
        }
    }
    check=0;
    clog << endl;
}

void QueryTreeNode::GenerateSchema(){
    schema = new Schema(left->schema, right->schema);
}

void QueryTreeNode::GenerateOM(int numAtts, vector<int> whichAtts, vector<int> whichTypes){
    order = new OrderMaker();
    int i=0;
    order->numAtts = numAtts;
    while(i < whichAtts.size()){
        order->whichAtts[i] = whichAtts[i];
        order->whichTypes[i] = (Type)whichTypes[i];
        i++;
    }
}
