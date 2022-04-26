#include <map>
#include <vector>
#include <iostream>
#include <assert.h>
#include <string.h>

#include "NodeDataOperations.h"
#include "NodeDataQuery.h"
#include "ParseTree.h"
#include "Statistics.h"

extern struct NameList* groupingAtts;
extern int distinctFunc;
extern struct FuncOperator* finalFunction;
extern struct TableList* tables;
extern struct AndList* boolean;
extern int distinctAtts;
extern struct NameList* attsToSelect;
extern "C" {
int yyparse (void);
}


vector<AndList> optimizer(vector<AndList> joins, Statistics* stat){
    AndList al;
	string data1;
    int i = 0;
	int sIndex = 0;
	int inc = 0;
	int validate=0;
	string data2;
    double leastValue = -1.0;
	int estimate = 0.0;
    vector<string> joinedData;
	vector<AndList> result;
	result.reserve(joins.size());
	validate++;
	while (joins.size() >= 2){
		while (i < joins.size()){
			al = joins[i];
			validate--;
			stat->processRelationData(al.left->left->right, data2);
            stat->processRelationData(al.left->left->left, data1);
            validate++;
            char* rels[] = { (char*)data1.c_str(), (char*)data2.c_str() };
			if (leastValue == -1.0){
                sIndex = i;
				leastValue = stat->Estimate(&al, rels, 2);
			}else{
				estimate = stat->Estimate(&al, rels, 2);
				if (estimate < leastValue){
					leastValue = estimate;
					sIndex = i;
				}
			}
			i++;
		}
        leastValue = -1.0,i = 0;
        result.push_back(joins[sIndex]);
		joinedData.push_back(data1);
		joinedData.push_back(data2);
		joins.erase(joins.begin() + sIndex);
        inc++;
	}
	result.insert(result.begin() + inc, joins[0]);
	return result;
}


int main(){
	Statistics* stat = new Statistics();
	stat->Read("Statistics.txt");
    int pipeID = 1;
	int check=0;
	cout << "enter: ";
	map<string, double> joinCosts;
    string startProject;
	vector<AndList> joins, selects, joinDS;
	yyparse();
    vector<string> r;
    NodeDataOperations* nodeDataOp = new NodeDataOperations();

    nodeDataOp->getTableData(r);
	check++;
    nodeDataOp->getOperationsData(joins, selects, joinDS, *stat);
	cout << endl << "Number of selects: " << selects.size() << endl;
	check--;
	cout << "Number of joins: " << joins.size() << endl;
    NodeDataQuery* NodeDataTop = NULL;
    NodeDataQuery* NodeDataTraversal;
	NodeDataQuery* appendNodeData = NULL;
    map<string, NodeDataQuery*> leafs;
	TableList* iterTable = tables;
	while (iterTable != 0){
		if (iterTable->aliasAs == 0){
            leafs.insert(std::pair<string, NodeDataQuery*>(iterTable->tableName, new NodeDataQuery()));
		}else{
            leafs.insert(std::pair<string, NodeDataQuery*>(iterTable->aliasAs, new NodeDataQuery()));
            stat->CopyRel(iterTable->tableName, iterTable->aliasAs);
		}
		appendNodeData = leafs[iterTable->aliasAs];
        appendNodeData->schema = new Schema("catalog", iterTable->tableName);
        check++;
		if (iterTable->aliasAs != 0){appendNodeData->schema->updateName(string(iterTable->aliasAs));}
        check--;
		NodeDataTop = appendNodeData;
		appendNodeData->outputNodeId = pipeID++;
        appendNodeData->NodeTypeSetter(SELECTF);
		string base(iterTable->tableName);
		string path("bin/" + base + ".bin");
		appendNodeData->p = strdup(path.c_str());
		iterTable = iterTable->next;
	}
    string table,attribute;
	AndList selectIter;
	for (unsigned i = 0; i < selects.size(); i++){
		selectIter = selects[i];
		(selectIter.left->left->left->code == NAME) ? stat->processRelationData(selectIter.left->left->left, table) : stat->processRelationData(selectIter.left->left->right, table);
        startProject = table;
		NodeDataTraversal = leafs[table];
		check--;
		while (NodeDataTraversal->previous != NULL){
			NodeDataTraversal = NodeDataTraversal->previous;
		}
		appendNodeData = new NodeDataQuery();
		NodeDataTraversal->previous = appendNodeData;
		check++;
        appendNodeData->nodeType = SELECTP;
        appendNodeData->schema = NodeDataTraversal->schema;
        appendNodeData->andList = &selects[i];
        appendNodeData->nodeLeft = NodeDataTraversal;
        appendNodeData->outputNodeId = pipeID++;
		appendNodeData->leftNodeId = NodeDataTraversal->outputNodeId;
		char* statApply = strdup(table.c_str());
		check++;
		stat->Apply(&selectIter, &statApply, 1);
		NodeDataTop = appendNodeData;
	}
	check--;
	if (joins.size() > 1){
		joins = optimizer(joins, stat);
	}
    AndList curJoin;
    NodeDataQuery* rTreeNode;
	NodeDataQuery* lTreeNode;
	string reln1, reln2;
	for (unsigned i = 0; i < joins.size(); i++){
		curJoin = joins[i];
		reln1 = "",reln2 = "";
        stat->processRelationData(curJoin.left->left->right, reln2);
		stat->processRelationData(curJoin.left->left->left, reln1);
        rTreeNode = leafs[reln2],lTreeNode = leafs[reln1];
        table = reln1;
		while (rTreeNode->previous != NULL){
            rTreeNode = rTreeNode->previous;
		}
        while (lTreeNode->previous != NULL){
            lTreeNode = lTreeNode->previous;
        }

		appendNodeData = new NodeDataQuery();
        appendNodeData->andList = &joins[i];
		appendNodeData->rightNodeId = rTreeNode->outputNodeId;
        appendNodeData->leftNodeId = lTreeNode->outputNodeId;
		appendNodeData->outputNodeId = pipeID++;
        appendNodeData->nodeType = JOIN;check--;
        appendNodeData->nodeRight = rTreeNode;
        rTreeNode->previous = appendNodeData;
		appendNodeData->nodeLeft = lTreeNode;check++;
		lTreeNode->previous = appendNodeData;
        appendNodeData->setSchema();
		NodeDataTop = appendNodeData;
	}
	check--;
	for (unsigned i = 0; i < joinDS.size(); i++){
		NodeDataTraversal = NodeDataTop;
		appendNodeData = new NodeDataQuery();
        appendNodeData->nodeLeft = NodeDataTraversal;check--;
		NodeDataTraversal->previous = appendNodeData;
        appendNodeData->nodeType = SELECTP;
		appendNodeData->schema = NodeDataTraversal->schema;check++;
        appendNodeData->leftNodeId = NodeDataTraversal->outputNodeId;
		appendNodeData->andList = &joinDS[i];
		appendNodeData->outputNodeId = pipeID++;
		NodeDataTop = appendNodeData;
	}
	if (finalFunction != 0){
		if (distinctFunc != 0){
			appendNodeData = new NodeDataQuery();
			appendNodeData->nodeLeft = NodeDataTop;check++;
            appendNodeData->schema = NodeDataTop->schema;
            appendNodeData->leftNodeId = NodeDataTop->outputNodeId;
			appendNodeData->outputNodeId = pipeID++;check--;
            NodeDataTop->previous = appendNodeData;
            appendNodeData->nodeType = DISTINCT;
			NodeDataTop = appendNodeData;
		}

		if (groupingAtts  == 0){
			appendNodeData = new NodeDataQuery();
            NodeDataTop->previous = appendNodeData;
			appendNodeData->nodeLeft = NodeDataTop;
			appendNodeData->leftNodeId = NodeDataTop->outputNodeId;
            appendNodeData->schema = NodeDataTop->schema;
			appendNodeData->fOperator = finalFunction;
            appendNodeData->outputNodeId = pipeID++;
            appendNodeData->nodeType = SUM;
			appendNodeData->createNodeTree();
		}else{
			appendNodeData = new NodeDataQuery();
			appendNodeData->nodeLeft = NodeDataTop;check++;
			NodeDataTop->previous = appendNodeData;
            appendNodeData->schema = NodeDataTop->schema;
			appendNodeData->outputNodeId = pipeID++;check++;
            appendNodeData->nodeType = GROUP_BY;
            appendNodeData->leftNodeId = NodeDataTop->outputNodeId;
			appendNodeData->om = new OrderMaker();check--;
			NameList* groupTraverse = groupingAtts;
            vector<int> attsToGroup,whichType;
            int numAttsGrp = 0;
			while (groupTraverse){
				attsToGroup.push_back(appendNodeData->schema->Find(groupTraverse->name));
				whichType.push_back(appendNodeData->schema->FindType(groupTraverse->name));
                numAttsGrp++;check++;
				cout << "GROUPING ON " << groupTraverse->name << endl;
				groupTraverse = groupTraverse->next;
			}
			check++;
			appendNodeData->createOrder(numAttsGrp, attsToGroup, whichType);
			appendNodeData->fOperator = finalFunction;check++;
			appendNodeData->createNodeTree();
		}
		NodeDataTop = appendNodeData;
	}
	if (distinctAtts != 0){
		appendNodeData = new NodeDataQuery();
        NodeDataTop->previous = appendNodeData;
        appendNodeData->schema = NodeDataTop->schema;
		appendNodeData->nodeLeft = NodeDataTop;
        appendNodeData->nodeType = DISTINCT;
		appendNodeData->leftNodeId = NodeDataTop->outputNodeId;
		appendNodeData->outputNodeId = pipeID++;
		NodeDataTop = appendNodeData;
	}
	if (attsToSelect != 0){
		NodeDataTraversal = NodeDataTop;
		appendNodeData = new NodeDataQuery();
        NodeDataTraversal->previous = appendNodeData;check++;
        appendNodeData->nodeLeft = NodeDataTraversal;
		appendNodeData->nodeType = PROJECT;check++;
        appendNodeData->outputNodeId = pipeID++;
        appendNodeData->leftNodeId = NodeDataTraversal->outputNodeId;
        NameList* attsTraverse = attsToSelect;
		Schema* oldSchema = NodeDataTraversal->schema;
		string attribute;check++;
        vector<int> indexOfAttsToKeep;
        while (attsTraverse != 0){
			attribute = attsTraverse->name;check++;
			indexOfAttsToKeep.push_back(oldSchema->Find(const_cast<char*>(attribute.c_str())));
			attsTraverse = attsTraverse->next;
		}
        check--;
		Schema* newSchema = new Schema(oldSchema, indexOfAttsToKeep);
		appendNodeData->schema = newSchema;
		appendNodeData->schema->Print();
	}
	cout << "PRINTING TREE IN ORDER: " << endl << endl;
	check++;
	if (appendNodeData != NULL){
        appendNodeData->orderPrint();
	}
}
