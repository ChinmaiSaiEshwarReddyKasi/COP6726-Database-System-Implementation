#ifndef QUERY_TREE_NODE_H
#define QUERY_TREE_NODE_H

#define P_SIZE 100

#include "Schema.h"
#include "Record.h"
#include "Function.h"
#include "Comparison.h"
#include "RelOp.h"
#include "DBFile.h"

#include <vector>

extern struct FuncOperator *finalFunction;
extern struct NameList *attsToSelect;

enum NodeDataType {SELECTF, SELECTP, PROJECT, JOIN, SUM, GROUP_BY, DISTINCT, WRITE};

class NodeDataQuery {

 public:
	    CNF *cnf;
		Sum *sum;
		string p;
		Join *join;
	  	int *attStore;
		Function *func;
	    Schema *schema;
		DBFile *dbFile;
	    OrderMaker *om;
		int leftNodeId;
	  	int numAttStore;
		int rightNodeId;
		int outputNodeId;
	    AndList *andList;
		Project *project;
		GroupBy *groupBy;
		NodeDataType nodeType;
		WriteOut *writeOut;
		SelectFile *selectFile;
		SelectPipe *selectPipe;
	    FuncOperator *fOperator;
		NodeDataQuery *previous;
	    NodeDataQuery *nodeLeft;
	    NodeDataQuery *nodeRight;
		DuplicateRemoval *duplicateRemoval;


	    NodeDataQuery();
	 	~NodeDataQuery();

		void orderPrint();
	    void nodePrint ();
	    void cnfPrint();
		void idPrint(NodeDataType type);
		void NodeTypeSetter(NodeDataType setter);
		void WaitUntilDone();
	    string nodeTypeGetter ();
	    NodeDataType GetType ();
		void setSchema();
		void createNodeTree();
		void createOrder(int numAtts, vector<int> whichAtts, vector<int> whichTypes);

};

#endif
