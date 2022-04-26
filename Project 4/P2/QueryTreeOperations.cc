#include "QueryTreeOperations.h"
#include <map>
#include <vector>
#include <iostream>
#include <assert.h>
#include <string.h>

#include "QueryTreeNode.h"
#include "ParseTree.h"
#include "Statistics.h"

QueryTreeOperations ::QueryTreeOperations() {}

extern "C" {
int yyparse (void);
}

extern struct FuncOperator* finalFunction;
extern struct TableList* tables;
extern struct AndList* boolean;
extern struct NameList* groupingAtts;
extern struct NameList* attsToSelect;
extern int distinctAtts;
extern int distinctFunc;

void QueryTreeOperations :: GetTables(vector<string>& relations)
{
    TableList* list = tables;
    while (list){
        (list->aliasAs) ? relations.push_back(list->aliasAs) : relations.push_back(list->tableName);
        list = list->next;
    }
}

void QueryTreeOperations ::GetJoinsAndSelects(vector <AndList> &joins, vector <AndList> &selects, vector <AndList> &joinDepSel,
                               Statistics s)
{

    int check = 0;
    struct OrList* currentOrL;
    while (boolean != 0){
        currentOrL = boolean->left;
        check++;
        if (currentOrL && currentOrL->left->code == EQUALS && currentOrL->left->right->code == NAME && currentOrL->left->left->code == NAME){
            check--;
            AndList newAnd = *boolean;
            newAnd.rightAnd = 0;
            check++;
            joins.push_back(newAnd);
        }else{
            check++;
            currentOrL = boolean->left;
            if (currentOrL->left == 0){
                check = 0;
                AndList newAnd = *boolean;
                newAnd.rightAnd = 0;
                check++;
                selects.push_back(newAnd);
            }else{
                vector<string> involvedTables;
                check--;
                while (currentOrL != 0){
                    Operand* oprnd = currentOrL->left->left;
                    string rel;
                    if (oprnd->code != NAME){
                        oprnd = currentOrL->left->right;
                    }
                    s.ParseRelation(oprnd, rel);
                    check++;
                    if (involvedTables.size() == 0 || rel.compare(involvedTables[0]) != 0){
                        involvedTables.push_back(rel);
                    }
                    check--;
                    currentOrL = currentOrL->rightOr;
                }
                AndList newAnd = *boolean;
                check++;
                newAnd.rightAnd = 0;
                (involvedTables.size() > 1) ? joinDepSel.push_back(newAnd) : selects.push_back(newAnd);
            }
        }
        check--;
        boolean = boolean->rightAnd;
    }

}