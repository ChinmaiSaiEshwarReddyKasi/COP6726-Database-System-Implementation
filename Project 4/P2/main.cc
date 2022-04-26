#include <map>
#include <vector>
#include <iostream>
#include <assert.h>
#include <string.h>

#include "QueryTreeOperations.h"
#include "QueryTreeNode.h"
#include "ParseTree.h"
#include "Statistics.h"

extern struct NameList* attsToSelect;
extern struct AndList* boolean;
extern struct TableList* tables;
extern int distinctAtts;
extern struct NameList* groupingAtts;
extern struct FuncOperator* finalFunction;
extern int distinctFunc;
extern "C" {
int yyparse (void);
}


vector<AndList> optimizeJoinOrder(vector<AndList> joins, Statistics* s){
    AndList join;
    int i = 0,smallestIdx = 0,count = 0,check=0;
    double smallest = -1.0,guess = 0.0;
    vector<string> joinedRelationsVec;
	vector<AndList> newOrder;
	newOrder.reserve(joins.size());
	string rel1,rel2;
	check++;
	while (joins.size() >= 2){
		while (i < joins.size()){
			join = joins[i];
			check--;
			s->ParseRelation(join.left->left->right, rel2);
            s->ParseRelation(join.left->left->left, rel1);
            check++;
            char* rels[] = { (char*)rel1.c_str(), (char*)rel2.c_str() };
			if (smallest == -1.0){
                smallestIdx = i;
				smallest = s->Estimate(&join, rels, 2);
			}else{
				guess = s->Estimate(&join, rels, 2);
				if (guess < smallest){
					smallest = guess;
					smallestIdx = i;
				}
			}
			i++;
		}
        smallest = -1.0,i = 0;
        newOrder.push_back(joins[smallestIdx]);
		joinedRelationsVec.push_back(rel1);
		joinedRelationsVec.push_back(rel2);
		joins.erase(joins.begin() + smallestIdx);
        count++;
	}
	newOrder.insert(newOrder.begin() + count, joins[0]);
	return newOrder;
}


int main(){
	Statistics* s = new Statistics();
	s->Read("Statistics.txt");
    int pipeID = 1,check=0;
	cout << "enter: ";
	map<string, double> joinCosts;
    string projectStart;
	vector<AndList> joins, selects, joinDepSels;
	yyparse();
    vector<string> relations;
    QueryTreeOperations* queryTreeOperations = new QueryTreeOperations();

    queryTreeOperations->GetTables(relations);
	check++;
    queryTreeOperations->GetJoinsAndSelects(joins, selects, joinDepSels, *s);
	cout << endl << "Number of selects: " << selects.size() << endl;
	check--;
	cout << "Number of joins: " << joins.size() << endl;
    QueryTreeNode* topLevelNode = NULL;
    QueryTreeNode* traversalNode;
	QueryTreeNode* insertNode = NULL;
    map<string, QueryTreeNode*> leafs;
	TableList* iterTable = tables;
	while (iterTable != 0){
		if (iterTable->aliasAs == 0){
            leafs.insert(std::pair<string, QueryTreeNode*>(iterTable->tableName, new QueryTreeNode()));
		}else{
            leafs.insert(std::pair<string, QueryTreeNode*>(iterTable->aliasAs, new QueryTreeNode()));
            s->CopyRel(iterTable->tableName, iterTable->aliasAs);
		}
		insertNode = leafs[iterTable->aliasAs];
        insertNode->schema = new Schema("catalog", iterTable->tableName);
        check++;
		if (iterTable->aliasAs != 0){insertNode->schema->updateName(string(iterTable->aliasAs));}
        check--;
		topLevelNode = insertNode;
		insertNode->outPipeID = pipeID++;
        insertNode->SetType(SELECTF);
		string base(iterTable->tableName);
		string path("bin/" + base + ".bin");
		insertNode->path = strdup(path.c_str());
		iterTable = iterTable->next;
	}
    string table,attribute;
	AndList selectIter;
	for (unsigned i = 0; i < selects.size(); i++){
		selectIter = selects[i];
		(selectIter.left->left->left->code == NAME) ? s->ParseRelation(selectIter.left->left->left, table) : s->ParseRelation(selectIter.left->left->right, table);
        projectStart = table;
		traversalNode = leafs[table];
		check--;
		while (traversalNode->parent != NULL){
			traversalNode = traversalNode->parent;
		}
		insertNode = new QueryTreeNode();
		traversalNode->parent = insertNode;
		check++;
        insertNode->type = SELECTP;
        insertNode->schema = traversalNode->schema;
        insertNode->cnf = &selects[i];
        insertNode->left = traversalNode;
        insertNode->outPipeID = pipeID++;
		insertNode->lChildPipeID = traversalNode->outPipeID;
		char* statApply = strdup(table.c_str());
		check++;
		s->Apply(&selectIter, &statApply, 1);
		topLevelNode = insertNode;
	}
	check--;
	if (joins.size() > 1){
		joins = optimizeJoinOrder(joins, s);
	}
    AndList curJoin;
    QueryTreeNode* rTreeNode;
	QueryTreeNode* lTreeNode;
	string reln1, reln2;
	for (unsigned i = 0; i < joins.size(); i++){
		curJoin = joins[i];
		reln1 = "",reln2 = "";
        s->ParseRelation(curJoin.left->left->right, reln2);
		s->ParseRelation(curJoin.left->left->left, reln1);
        rTreeNode = leafs[reln2],lTreeNode = leafs[reln1];
        table = reln1;
		while (rTreeNode->parent != NULL){
            rTreeNode = rTreeNode->parent;
		}
        while (lTreeNode->parent != NULL){
            lTreeNode = lTreeNode->parent;
        }

		insertNode = new QueryTreeNode();
        insertNode->cnf = &joins[i];
		insertNode->rChildPipeID = rTreeNode->outPipeID;
        insertNode->lChildPipeID = lTreeNode->outPipeID;
		insertNode->outPipeID = pipeID++;
        insertNode->type = JOIN;check--;
        insertNode->right = rTreeNode;
        rTreeNode->parent = insertNode;
		insertNode->left = lTreeNode;check++;
		lTreeNode->parent = insertNode;
        insertNode->GenerateSchema();
		topLevelNode = insertNode;
	}
	check--;
	for (unsigned i = 0; i < joinDepSels.size(); i++){
		traversalNode = topLevelNode;
		insertNode = new QueryTreeNode();
        insertNode->left = traversalNode;check--;
		traversalNode->parent = insertNode;
        insertNode->type = SELECTP;
		insertNode->schema = traversalNode->schema;check++;
        insertNode->lChildPipeID = traversalNode->outPipeID;
		insertNode->cnf = &joinDepSels[i];
		insertNode->outPipeID = pipeID++;
		topLevelNode = insertNode;
	}
	if (finalFunction != 0){
		if (distinctFunc != 0){
			insertNode = new QueryTreeNode();
			insertNode->left = topLevelNode;check++;
            insertNode->schema = topLevelNode->schema;
            insertNode->lChildPipeID = topLevelNode->outPipeID;
			insertNode->outPipeID = pipeID++;check--;
            topLevelNode->parent = insertNode;
            insertNode->type = DISTINCT;
			topLevelNode = insertNode;
		}

		if (groupingAtts  == 0){
			insertNode = new QueryTreeNode();
            topLevelNode->parent = insertNode;
			insertNode->left = topLevelNode;
			insertNode->lChildPipeID = topLevelNode->outPipeID;
            insertNode->schema = topLevelNode->schema;
			insertNode->funcOp = finalFunction;
            insertNode->outPipeID = pipeID++;
            insertNode->type = SUM;
			insertNode->GenerateFunction();
		}else{
			insertNode = new QueryTreeNode();
			insertNode->left = topLevelNode;check++;
			topLevelNode->parent = insertNode;
            insertNode->schema = topLevelNode->schema;
			insertNode->outPipeID = pipeID++;check++;
            insertNode->type = GROUP_BY;
            insertNode->lChildPipeID = topLevelNode->outPipeID;
			insertNode->order = new OrderMaker();check--;
			NameList* groupTraverse = groupingAtts;
            vector<int> attsToGroup,whichType;
            int numAttsGrp = 0;
			while (groupTraverse){
				attsToGroup.push_back(insertNode->schema->Find(groupTraverse->name));
				whichType.push_back(insertNode->schema->FindType(groupTraverse->name));
                numAttsGrp++;check++;
				cout << "GROUPING ON " << groupTraverse->name << endl;
				groupTraverse = groupTraverse->next;
			}
			check++;
			insertNode->GenerateOM(numAttsGrp, attsToGroup, whichType);
			insertNode->funcOp = finalFunction;check++;
			insertNode->GenerateFunction();
		}
		topLevelNode = insertNode;
	}
	if (distinctAtts != 0){
		insertNode = new QueryTreeNode();
        topLevelNode->parent = insertNode;
        insertNode->schema = topLevelNode->schema;
		insertNode->left = topLevelNode;
        insertNode->type = DISTINCT;
		insertNode->lChildPipeID = topLevelNode->outPipeID;
		insertNode->outPipeID = pipeID++;
		topLevelNode = insertNode;
	}
	if (attsToSelect != 0){
		traversalNode = topLevelNode;
		insertNode = new QueryTreeNode();
        traversalNode->parent = insertNode;check++;
        insertNode->left = traversalNode;
		insertNode->type = PROJECT;check++;
        insertNode->outPipeID = pipeID++;
        insertNode->lChildPipeID = traversalNode->outPipeID;
        NameList* attsTraverse = attsToSelect;
		Schema* oldSchema = traversalNode->schema;
		string attribute;check++;
        vector<int> indexOfAttsToKeep;
        while (attsTraverse != 0){
			attribute = attsTraverse->name;check++;
			indexOfAttsToKeep.push_back(oldSchema->Find(const_cast<char*>(attribute.c_str())));
			attsTraverse = attsTraverse->next;
		}
        check--;
		Schema* newSchema = new Schema(oldSchema, indexOfAttsToKeep);
		insertNode->schema = newSchema;
		insertNode->schema->Print();
	}
	cout << "PRINTING TREE IN ORDER: " << endl << endl;
	check++;
	if (insertNode != NULL){
        insertNode->PrintInOrder();
	}
}
