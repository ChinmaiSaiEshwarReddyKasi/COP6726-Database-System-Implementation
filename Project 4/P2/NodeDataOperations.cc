#include "NodeDataOperations.h"
#include <map>
#include <vector>
#include <iostream>
#include <assert.h>
#include <string.h>

#include "NodeDataQuery.h"
#include "ParseTree.h"
#include "Statistics.h"

NodeDataOperations ::NodeDataOperations() {}

extern "C" {
int yyparse (void);
}

extern struct NameList* groupingAtts;
extern int distinctAtts;
extern struct FuncOperator* finalFunction;
extern int distinctFunc;
extern struct AndList* boolean;
extern struct TableList* tables;
extern struct NameList* attsToSelect;

void NodeDataOperations :: getTableData(vector<string>& r)
{
    TableList* tl = tables;
    while (tl){
        (tl->aliasAs) ? r.push_back(tl->aliasAs) : r.push_back(tl->tableName);
        tl = tl->next;
    }
}

void NodeDataOperations ::getOperationsData(vector <AndList> &joins, vector <AndList> &selects, vector <AndList> &joinDepSel,
                               Statistics s)
{

    int count = 0;
    struct OrList* ol;
    while (boolean != 0){
        ol = boolean->left;
        count++;
        if (ol && ol->left->code == EQUALS && ol->left->right->code == NAME && ol->left->left->code == NAME){
            count--;
            AndList temp = *boolean;
            temp.rightAnd = 0;
            count++;
            joins.push_back(temp);
        }else{
            count++;
            ol = boolean->left;
            if (ol->left == 0){
                count = 0;
                AndList temp = *boolean;
                temp.rightAnd = 0;
                count++;
                selects.push_back(temp);
            }else{
                vector<string> tempTables;
                count--;
                while (ol != 0){
                    Operand* oprnd = ol->left->left;
                    string rel;
                    if (oprnd->code != NAME){
                        oprnd = ol->left->right;
                    }
                    s.processRelationData(oprnd, rel);
                    count++;
                    if (tempTables.size() == 0 || rel.compare(tempTables[0]) != 0){
                        tempTables.push_back(rel);
                    }
                    count--;
                    ol = ol->rightOr;
                }
                AndList temp = *boolean;
                count++;
                temp.rightAnd = 0;
                (tempTables.size() > 1) ? joinDepSel.push_back(temp) : selects.push_back(temp);
            }
        }
        count--;
        boolean = boolean->rightAnd;
    }

}