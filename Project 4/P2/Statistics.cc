#include "Statistics.h"
#include <sstream>

RelData :: RelData () : checkJoint(false){
}

RelData :: RelData (const RelData& copyMe) : checkJoint(copyMe.checkJoint) , relName(copyMe.relName) , numTuples(copyMe.numTuples) {
    attrMap.insert (copyMe.attrMap.begin (), copyMe.attrMap.end ());
}

RelData :: RelData (string name, int tuples) :  checkJoint(false) , relName(name) , numTuples(tuples) {
}

bool RelData :: isRelationPresent (string _relName) {
    return (relName == _relName) ? true : JointRelation.count(_relName);
}

RelData &RelData :: operator= (const RelData& copyMe) {
    attrMap.insert (copyMe.attrMap.begin (), copyMe.attrMap.end ());
    numTuples = copyMe.numTuples, relName = copyMe.relName;
    checkJoint = copyMe.checkJoint;
	return *this;
}


AttsData :: AttsData () {}

AttsData :: AttsData (string name, int num) : nameAtt(name), differentTuples(num){
}

AttsData :: AttsData (const AttsData& copyMe) : nameAtt(copyMe.nameAtt), differentTuples(copyMe.differentTuples) {
}

AttsData &AttsData :: operator= (const AttsData& copyMe){
    differentTuples = copyMe.differentTuples, nameAtt = copyMe.nameAtt;
	return *this;
}

Statistics :: Statistics () {}

Statistics :: Statistics (Statistics& copyMe) { relMap.insert (copyMe.relMap.begin (), copyMe.relMap.end ()); }

Statistics :: ~Statistics () {}

double Statistics :: oprAnd (AndList* andList, char* relName[], int numJoin) {
	return (andList != NULL) ? (oprOR (andList->left, relName, numJoin)) * (oprAnd (andList->rightAnd, relName, numJoin)) : 1.0;
}

Statistics Statistics :: operator= (Statistics& copyMe) {
    relMap.insert (copyMe.relMap.begin (), copyMe.relMap.end ());
    return *this;
}

double Statistics :: oprOR (OrList* orOP, char* rName[], int join) {
	if (orOP == NULL) {return 0.0;}
	OrList* orLT = orOP->rightOr;
    int c = 1;
    char* nTemp = orOP->left->left->value;
    double left = ComparisonOpr (orOP->left, rName, join);
	while (orLT) {
		if (!strcmp(orLT->left->left->value, nTemp)) {c++;}
		orLT = orLT->rightOr;
	}
	return (c > 1) ? (double) c * left : (double) (1.0 - (1.0 - left)*(1.0 - (oprOR (orOP->rightOr, rName, join))));
}

void Statistics :: AddAtt(char* relName, char* attrName, int numDistincts) {
	string attsData(attrName), relationData(relName);
	AttsData tempAttData(attsData, numDistincts);
	relMap[relationData].attrMap[attsData] = tempAttData;
}

double Statistics :: ComparisonOpr (ComparisonOp* cOP, char* rName[], int join) {
    RelData lRelData, rRelData;
    double l, r;
	int code = cOP->code;
	int lData = GatherRelationDataOpr (cOP->left, rName, join, lRelData);
	int rData = GatherRelationDataOpr (cOP->right, rName, join, rRelData);
	if (cOP->right->code != NAME) {
        r = -1.0;
	} else {
        if (rData != -1) {
            string dataTemp(cOP->right->value);
            r = rRelData.attrMap[dataTemp].differentTuples;
        } else {
            r = 1.0;
        }
	}
    if (cOP->left->code != NAME) {
        l = -1.0;
    } else {
        if (lData != -1) {
            string dataTemp(cOP->left->value);
            l = lRelData.attrMap[dataTemp].differentTuples;
        } else {
            l = 1.0;
        }
    }
    if (code == EQUALS) {
		if (l > r){
			return 1.0 / l;
		} else {
			return 1.0 / r;
		}
    } else{
		return (code == LESS_THAN || code == GREATER_THAN) ? 1.0 / 3.0 : 0.0;
	}
}

void Statistics :: CopyRel (char* oldName, char* newName) {
	string currentName(newName);
	string previousName(oldName);
    AttrMap newAttrMap;
	relMap[currentName] = relMap[previousName];
	relMap[currentName].relName = currentName;
	for (auto attsData : relMap[currentName].attrMap) {
		string newAttrStr = currentName + "." + attsData.first;
		AttsData temp(attsData.second);
		temp.nameAtt = newAttrStr;
		newAttrMap[newAttrStr] = temp;
	}
	relMap[currentName].attrMap = newAttrMap;
}

int Statistics :: GatherRelationDataOpr(Operand* o, char* rName[], int join, RelData& rData) {
    if (o == NULL || rName == NULL) {return -1;}
    string buf(o->value);
    for (auto r : relMap) {
        if (r.second.attrMap.count(buf)) { rData = r.second; return 0; }
    }
    return -1;
}

void Statistics :: AddRel (char *relName, int numTuples) {
    bool check = false;
    string relTemp(relName);
    RelData temp(relTemp, numTuples);
    check = true;
    relMap[relTemp] = temp;
}


void Statistics :: Read (char* fromWhere) {
	ifstream in(fromWhere);
	int count=0;
	int sepCount; 
	string ts;
	int AttsCount;
	int countTuples; 
	int valRelations;
	relMap.clear ();
    string rName;
	string AttsNames;
	in >> valRelations;

	for (int i = 0; i < valRelations; i++) {
		in >> rName;
		in >> countTuples;
		count++;
		RelData tempRel(rName, countTuples);
		relMap[rName] = tempRel;
		count++;
        count++;
		in >> AttsCount;
		for (int j = 0; j < AttsCount; j++) {
		    in >> ts;
			in >> AttsNames;
			in >> sepCount;
			count++;
			AttsData tempAttr(AttsNames, sepCount);
			relMap[rName].attrMap[AttsNames] = tempAttr;
			count--;
		}
	}
}


void Statistics :: Apply (struct AndList* parseTree, char* relNames[], int numToJoin) {
    char* names[100];
    bool validate;
    int idx = 0;
	int joinCount = 0;
	RelData tr;
	while (idx < numToJoin) {
		string tempData(relNames[idx]);
		if (relMap.count(tempData)){
			tr = relMap[tempData];
			names[joinCount++] = relNames[idx];
			validate = false;
			if (tr.checkJoint && tr.JointRelation.size() <= numToJoin) {
			    for (int i = 0; i < numToJoin; i++) {
			        validate = true;
			        string str(relNames[i]);
			        if (tr.JointRelation.count(str) && tr.JointRelation[str] != tr.JointRelation[tempData]) {
			            validate = false;
			            return;
			        }
			    }
			}
		}
		idx++;
	}
	string fRelData(names[0]);
	RelData fRel = relMap[fRelData];
    fRel.numTuples = Estimate (parseTree, names, joinCount);
    fRel.checkJoint = true;
    relMap.erase (fRelData);
    validate = true;
	for(int i = 1;i < joinCount;i++){
		string tempData(names[i]);
		fRel.JointRelation[tempData] = tempData;
		tr = relMap[tempData];
        validate = false;
		relMap.erase (tempData);
		fRel.attrMap.insert (tr.attrMap.begin (), tr.attrMap.end ());
		validate = true;
	}
	relMap[fRelData] = fRel;
}

double Statistics :: Estimate (struct AndList *parseTree, char **relNames, int numToJoin) {
    double res = 1.0;
	for(int i = 0; i < numToJoin;i++) {
		string tData(relNames[i]);
		if (relMap.count(tData)) {
            res *= (double) relMap[tData].numTuples;
        }
	}
	return (parseTree != NULL) ? (oprAnd (parseTree, relNames, numToJoin))*res : res ;
}

void Statistics :: Write (char* toWhere) {
    ofstream ofs (toWhere);
    ofs << relMap.size() << endl;
    for (auto rData : relMap) {
        ofs << rData.second.relName << " ";
        ofs << rData.second.numTuples << " ";
        ofs << rData.second.attrMap.size () << endl;
        for (auto a : rData.second.attrMap) {
            ofs << "# ";
            ofs << a.second.nameAtt << endl;
            ofs << a.second.differentTuples << endl;
        }
    }
    ofs.close ();
}

void Statistics::processRelationData(struct Operand* op, string& r) {

    string data(op->value);
    stringstream ss;

    int i = 0;

    while (data[i] != '_') {

        if (data[i] == '.') {
            r = ss.str();
            return;
        }

        ss << data[i];

        i++;

    }

}