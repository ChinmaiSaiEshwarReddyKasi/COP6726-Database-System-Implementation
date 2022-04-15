#include "Statistics.h"

AttributeInfo :: AttributeInfo () {}

AttributeInfo :: AttributeInfo (string name, int num) : attrName(name), distinctTuples(num){
}

AttributeInfo :: AttributeInfo (const AttributeInfo& copyMe) : attrName(copyMe.attrName), distinctTuples(copyMe.distinctTuples) {
}

AttributeInfo &AttributeInfo :: operator= (const AttributeInfo& copyMe){
    distinctTuples = copyMe.distinctTuples, attrName = copyMe.attrName;
	return *this;
}

RelationInfo :: RelationInfo () : isJoint(false){
}

RelationInfo :: RelationInfo (const RelationInfo& copyMe) : isJoint(copyMe.isJoint) , relName(copyMe.relName) , numTuples(copyMe.numTuples) {
    attrMap.insert (copyMe.attrMap.begin (), copyMe.attrMap.end ());
}

RelationInfo :: RelationInfo (string name, int tuples) :  isJoint(false) , relName(name) , numTuples(tuples) {
}

bool RelationInfo :: isRelationPresent (string _relName) {
    return (relName == _relName) ? true : relJoint.count(_relName);
}

RelationInfo &RelationInfo :: operator= (const RelationInfo& copyMe) {
    attrMap.insert (copyMe.attrMap.begin (), copyMe.attrMap.end ());
    numTuples = copyMe.numTuples, relName = copyMe.relName;
    isJoint = copyMe.isJoint;
	return *this;
}

Statistics :: ~Statistics () {}

Statistics Statistics :: operator= (Statistics& copyMe) {
    relMap.insert (copyMe.relMap.begin (), copyMe.relMap.end ());
    return *this;
}

Statistics :: Statistics () {}

Statistics :: Statistics (Statistics& copyMe) {
	relMap.insert (copyMe.relMap.begin (), copyMe.relMap.end ());
}

double Statistics :: AndOp (AndList* andList, char* relName[], int numJoin) {
	return (andList != NULL) ? (OrOp (andList->left, relName, numJoin)) * (AndOp (andList->rightAnd, relName, numJoin)) : 1.0;
}

double Statistics :: OrOp (OrList* orList, char* relName[], int numJoin) {
	if (orList == NULL) {return 0.0;}
    int count = 1;
    char* attrName = orList->left->left->value;
	OrList* temp = orList->rightOr;
    double left = ComOp (orList->left, relName, numJoin);
	while (temp) {
		if (!strcmp(temp->left->left->value, attrName)) {count++;}
		temp = temp->rightOr;
	}
	return (count > 1) ? (double) count * left : (double) (1.0 - (1.0 - left)*(1.0 - (OrOp (orList->rightOr, relName, numJoin))));
}

double Statistics :: ComOp (ComparisonOp* compOp, char* relName[], int numJoin) {
    double left, right;
    RelationInfo leftRel, rightRel;
	int code = compOp->code,leftResult = GetRelForOp (compOp->left, relName, numJoin, leftRel), rightResult = GetRelForOp (compOp->right, relName, numJoin, rightRel);
	if (compOp->right->code != NAME) {
        right = -1.0;
	} else {
        if (rightResult != -1) {
            string tempStr(compOp->right->value);
            right = rightRel.attrMap[tempStr].distinctTuples;
        } else {
            right = 1.0;
        }
	}
    if (compOp->left->code != NAME) {
        left = -1.0;
    } else {
        if (leftResult != -1) {
            string tempStr(compOp->left->value);
            left = leftRel.attrMap[tempStr].distinctTuples;
        } else {
            left = 1.0;
        }
    }
    if (code == EQUALS) {
        return (left > right) ? 1.0 / left : 1.0 / right;
    } else{
		return (code == LESS_THAN || code == GREATER_THAN) ? 1.0 / 3.0 : 0.0;
	}
}

void Statistics :: AddAtt(char* relName, char* attrName, int numDistincts) {
	string attrStr(attrName), relStr(relName);
	AttributeInfo temp(attrStr, numDistincts);
	relMap[relStr].attrMap[attrStr] = temp;
}

int Statistics :: GetRelForOp(Operand* operand, char* relName[], int numJoin, RelationInfo& relInfo) {
    if (operand == NULL || relName == NULL) {return -1;}
    string buffer(operand->value);
    for (auto rel : relMap) {
        if (rel.second.attrMap.count(buffer)) {
            relInfo = rel.second;
            return 0;
        }
    }
    return -1;
}

void Statistics :: CopyRel (char* oldName, char* newName) {
	string newStr(newName), oldStr(oldName);
    AttrMap newAttrMap;
	relMap[newStr] = relMap[oldStr];
	relMap[newStr].relName = newStr;
	for (auto attr : relMap[newStr].attrMap) {
		string newAttrStr = newStr + "." + attr.first;
		AttributeInfo temp(attr.second);
		temp.attrName = newAttrStr;
		newAttrMap[newAttrStr] = temp;
	}
	relMap[newStr].attrMap = newAttrMap;
}

void Statistics :: Read (char* fromWhere) {
	int relCount, jointCount, numAttr, tuplesCount, distinctsCount,val=0;
	string tempString;
	ifstream in(fromWhere);
	relMap.clear ();
    string relName, jointName, attrName;
	in >> relCount;

	for (int i = 0; i < relCount; i++) {
		in >> relName;
		in >> tuplesCount;
		val++;
		RelationInfo tempRel(relName, tuplesCount);
		relMap[relName] = tempRel;
		val++;
		/*in >> relMap[relName].isJoint;
		if (relMap[relName].isJoint) {
		    val--;
			in >> jointCount;
			for (int j = 0; j < jointCount; j++) {
				in >> jointName;
				val--,relMap[relName].relJoint[jointName] = jointName;
			}
		}*/
        val++;
		in >> numAttr;
		for (int j = 0; j < numAttr; j++) {
		    in >> tempString;
			in >> attrName;
			in >> distinctsCount;
			val++;
			AttributeInfo tempAttr(attrName, distinctsCount);
			relMap[relName].attrMap[attrName] = tempAttr;
			val--;
		}
	}
}

void Statistics :: AddRel (char *relName, int numTuples) {
    bool check = false;
    string relStr(relName);
    RelationInfo temp(relStr, numTuples);
    check = true;
    relMap[relStr] = temp;
}

void Statistics :: Apply (struct AndList* parseTree, char* relNames[], int numToJoin) {
    char* relNamesArr[100];
    int index = 0, numJoin = 0;
    bool check;
	RelationInfo tempRel;
	while (index < numToJoin) {
		string tempStr(relNames[index]);
		if (relMap.count(tempStr)){
			tempRel = relMap[tempStr];
			relNamesArr[numJoin++] = relNames[index];
			check = false;
			if (tempRel.isJoint && tempRel.relJoint.size() <= numToJoin) {
			    for (int i = 0; i < numToJoin; i++) {
			        check = true;
			        string str(relNames[i]);
			        if (tempRel.relJoint.count(str) && tempRel.relJoint[str] != tempRel.relJoint[tempStr]) {
			            check = false;
			            return;
			        }
			    }
			}
		}
		index++;
	}
	string firstRelName(relNamesArr[0]);
	RelationInfo firstRel = relMap[firstRelName];
    firstRel.numTuples = Estimate (parseTree, relNamesArr, numJoin);
    firstRel.isJoint = true;
    relMap.erase (firstRelName);
    check = true;
	for(int i = 1;i < numJoin;i++){
		string tempStr(relNamesArr[i]);
		firstRel.relJoint[tempStr] = tempStr;
		tempRel = relMap[tempStr];
        check = false;
		relMap.erase (tempStr);
		firstRel.attrMap.insert (tempRel.attrMap.begin (), tempRel.attrMap.end ());
		check = true;
	}
	relMap[firstRelName] = firstRel;
}

void Statistics :: Write (char* toWhere) {
    ofstream out (toWhere);
    out << relMap.size() << endl;
    for (auto rel : relMap) {
        out << rel.second.relName << " ";
        out << rel.second.numTuples << " ";
        /*out << rel.second.isJoint << " ";
        if (rel.second.isJoint) {
            out << rel.second.relJoint.size () << endl;
            for (auto relJ : rel.second.relJoint) {
                out << relJ.second << endl;
            }
        }*/
        out << rel.second.attrMap.size () << endl;
        for (auto attr : rel.second.attrMap) {
            out << "# ";
            out << attr.second.attrName << endl;
            out << attr.second.distinctTuples << endl;
        }
    }
    out.close ();
}

double Statistics :: Estimate (struct AndList *parseTree, char **relNames, int numToJoin) {
    double product = 1.0;
	for(int i = 0; i < numToJoin;i++) {
		string tempStr(relNames[i]);
		if (relMap.count(tempStr)) {
            product *= (double) relMap[tempStr].numTuples;
        }
	}
	return (parseTree != NULL) ? (AndOp (parseTree, relNames, numToJoin))*product : product ;
}