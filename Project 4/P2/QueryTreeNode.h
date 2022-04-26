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

enum QueryNodeType {SELECTF, SELECTP, PROJECT, JOIN, SUM, GROUP_BY, DISTINCT, WRITE};

class QueryTreeNode {

 public:
		SelectFile *sf;
		SelectPipe *sp;
		Join *j;
		GroupBy *gb;
		Project *p;
		Sum *s;
		DuplicateRemoval *d;
		WriteOut *h;
		DBFile *db;
		QueryNodeType type;
		QueryTreeNode *parent;
	    QueryTreeNode *left;
	    QueryTreeNode *right;
		int lChildPipeID;
		Pipe *lInputPipe;
		int rChildPipeID;
		Pipe *rInputPipe;
		int outPipeID;
		Pipe *outPipe;

	    QueryTreeNode();
	 	~QueryTreeNode();

		void PrintInOrder();
	    void PrintNode ();
	    void PrintCNF();
		void PrintFunction();
		void printIDs(QueryNodeType type);
		void SetType(QueryNodeType setter);
		void Run();
		void WaitUntilDone();
	    string GetTypeName ();
	    QueryNodeType GetType ();
		//Used for JOIN
		void GenerateSchema();
		//Used for SUM
		void GenerateFunction();
		//Used for GROUP_BY
		void GenerateOM(int numAtts, vector<int> whichAtts, vector<int> whichTypes);
		// For a PROJECT
		int numAttsIn;
		int numAttsOut;
		vector<int> aTK;
	  	int *attsToKeep;
	  	int numAttsToKeep;
		// For various operations
	    AndList *cnf;
	    CNF *opCNF;
	    Schema *schema;
		// For GROUP BY
	     OrderMaker *order;
		// For aggregate
	    FuncOperator *funcOp;
		Function *func;
		// For Write and SelectFile (identifies where files are located)
		string path;
		string lookupKey(string path);
		vector<string> relations;

};

#endif
