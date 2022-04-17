#include "RelOp.h"
using namespace std;
int buffSize = 100;

void RelOp :: Use_n_Pages (int n) {
	runLen = n;
}

void *_StartOp (void *arg) {
    ((RelOp *)arg)->Start ();
}

void RelOp :: WaitUntilDone () {
	pthread_join (t, NULL);
}


void DuplicateRemoval :: Run (Pipe &inPipe, Pipe &outPipe,Schema &mySchema) {
    in = &inPipe, out = &outPipe, schema = &mySchema;
    pthread_create (&t, NULL, _StartOp, (void *) this);
}

void SelectPipe :: Run (Pipe &inPipe,Pipe &outPipe,CNF &selOp,Record &literal) {
    in = &inPipe, out = &outPipe, cnf = &selOp , lit = &literal;
    pthread_create(&t, NULL, _StartOp, (void *) this);
}

void Project :: Run ( Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    in = &inPipe, out = &outPipe, attsToKeep = keepMe, numAttsIn = numAttsInput, numAttsOut = numAttsOutput;
    pthread_create (&t, NULL, _StartOp, (void *) this);
}

void SelectFile :: Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
	file = &inFile, out = &outPipe, cnf = &selOp, lit = &literal;
	pthread_create (&t, NULL, _StartOp, (void *) this);
}

void Join :: Run (Pipe &inL, Pipe &inR, Pipe &outPipe, CNF &selOp, Record &literal) {
    lit = &literal, inPipeL = &inL,out = &outPipe, cnf = &selOp,inPipeR = &inR;
    pthread_create (&t, NULL, _StartOp, (void *) this);
}

void GroupBy :: Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts,Function &computeMe) {
    in = &inPipe, out = &outPipe, order = &groupAtts, compute = &computeMe;
    pthread_create (&t, NULL, _StartOp, (void *) this);
}

void Sum :: Run (Pipe &inPipe,Pipe &outPipe,Function &computeMe) {
    in = &inPipe, out = &outPipe, compute = &computeMe;
    pthread_create (&t, NULL, _StartOp, (void *) this);
}

void WriteOut :: Run (Pipe &inPipe,FILE *outFile,Schema &mySchema) {
    in = &inPipe, file = outFile, schema = &mySchema;
    pthread_create (&t, NULL, _StartOp, (void *) this);
}

void Project :: Start () {
    Record *tempRec = new Record ();
    int count = 0;
    while (in->Remove (tempRec)) {
        tempRec->Project (attsToKeep, numAttsOut, numAttsIn);
        out->Insert (tempRec);
        ++count;
    }
    out->ShutDown ();

    delete tempRec;
}

void SelectFile :: Start () {
	Record *tempRec = new Record ();
	file->MoveFirst();
	while (file->GetNext (*tempRec, *cnf, *lit)) {
		out->Insert (tempRec);
	}
	out->ShutDown ();
	delete tempRec;
}

void Join :: Start () {
	int *tempAtts;
	ComparisonEngine compEng;
    OrderMaker lOrder, rOrder;
	Record *leftRec = new Record ();
	Record *rightRec = new Record ();
    cnf->GetSortOrders(lOrder, rOrder);
    int val = 0,leftNumAtts, rightNumAtts, totalNumAtts;
	if (lOrder.numAtts > 0 && rOrder.numAtts > 0) {
		Pipe leftPipe (buffSize), rightPipe (buffSize);
		BigQ bigqLeft (*inPipeL, leftPipe, lOrder, runLen), bigqRight (*inPipeR, rightPipe, rOrder, runLen);
		bool check = false;
		(!leftPipe.Remove (leftRec)) ? check = true : leftNumAtts = leftRec->GetLength ();
		if (!check && !rightPipe.Remove (rightRec)) {
			check = true;
		} else {
            int i = 0;
			rightNumAtts = rightRec->GetLength ();
	    	totalNumAtts = leftNumAtts + rightNumAtts;
			tempAtts = new int[totalNumAtts];
			while(i < leftNumAtts || i < rightNumAtts){
			    if(i < leftNumAtts){tempAtts[i] = i;}
			    if(i < rightNumAtts){tempAtts[leftNumAtts + i] = i;}
			    i++;
			}
		}

		while (!check) {
			while (compEng.Compare (leftRec, &lOrder, rightRec, &rOrder) > 0) {
				if (!rightPipe.Remove (rightRec)) {
                    val++;
					check = true;
					break;
				}
			}
			while (!check && compEng.Compare (leftRec, &lOrder, rightRec, &rOrder) < 0) {
				if (!leftPipe.Remove (leftRec)) {
				    val++;
					check = true;
					break;
				}
			}
            val--;
			while (!check && compEng.Compare (leftRec, &lOrder, rightRec, &rOrder) == 0) {
				Record *tempRec = new Record ();
				tempRec->MergeRecords (leftRec,rightRec,leftNumAtts,rightNumAtts,tempAtts,totalNumAtts,leftNumAtts);
                val++;
				out->Insert (tempRec);
				if (!rightPipe.Remove (rightRec)) {
				    val++;
					check = true;
					break;
				}
			}
		}
		while (rightPipe.Remove (rightRec));
		while (leftPipe.Remove (leftRec));
	} else {
        Heap HeapDB;
		char fName[100];
        bool check = false;
		sprintf (fName, "temp.tmp");
		HeapDB.Create (fName);
		(!inPipeL->Remove (leftRec)) ? check = true : leftNumAtts = leftRec->GetLength ();
		if (!inPipeR->Remove (rightRec)) {
			check = true;
		} else {
            int i = 0;
			rightNumAtts = rightRec->GetLength ();
			totalNumAtts = leftNumAtts + rightNumAtts;
			tempAtts = new int[totalNumAtts];
            while(i < leftNumAtts || i < rightNumAtts){
                if(i < leftNumAtts){tempAtts[i] = i;}
                if(i < rightNumAtts){tempAtts[leftNumAtts + i] = i;}
                i++;
            }
		}
		if (!check) {
            HeapDB.Add (*leftRec);
		    while(inPipeL->Remove (leftRec)){
                HeapDB.Add (*leftRec);
			}

		    HeapDB.MoveFirst ();
		    Record *tempRec = new Record ();
		    while (HeapDB.GetNext (*leftRec)) {
		        if (compEng.Compare (leftRec, rightRec, lit, cnf)) {
		            val++;
		            tempRec->MergeRecords (leftRec,rightRec,leftNumAtts,rightNumAtts,tempAtts,totalNumAtts,leftNumAtts);
		            out->Insert (tempRec);
		        }
		    }
		    delete tempRec;
		    val++;
	        while (inPipeR->Remove (rightRec)){
                HeapDB.MoveFirst ();
                Record *tempRec = new Record ();
                while (HeapDB.GetNext (*leftRec)) {
                   if (compEng.Compare (leftRec, rightRec, lit, cnf)) {
                       val++;
                       tempRec->MergeRecords (leftRec,rightRec,leftNumAtts,rightNumAtts,tempAtts,totalNumAtts,leftNumAtts);
                       out->Insert (tempRec);
                   }
                }
                delete tempRec;
			}

		}
		HeapDB.Close ();
		remove ("temp.tmp");
	}
	out->ShutDown ();
    delete rightRec, delete leftRec, delete tempAtts;
}

void SelectPipe :: Start () {
    Record *tempRec = new Record ();
    ComparisonEngine compEng;
    int count = 0;
    while (in->Remove (tempRec)) {
        if (compEng.Compare (tempRec, lit, cnf)) {
            out->Insert (tempRec);
            ++count;
        }
    }
    out->ShutDown ();
    delete tempRec;
}

void DuplicateRemoval :: Start () {
    Pipe tempPipe (buffSize);
    Record *prevRec = new Record ();
    OrderMaker sortOrder (schema);
	BigQ bigq (*in, tempPipe, sortOrder, runLen);
    ComparisonEngine compEng;
	tempPipe.Remove (prevRec);
    Record *curRec = new Record ();
	while (tempPipe.Remove (curRec)) {
		if (compEng.Compare (prevRec, curRec, &sortOrder)) {
			out->Insert (prevRec);
			prevRec->Copy (curRec);
		}
	}
	bool check = false;
	if (curRec->bits != NULL && !compEng.Compare (prevRec,curRec, &sortOrder)) {
		out->Insert (prevRec);
        check = true;
		prevRec->Copy (curRec);
	}
	out->ShutDown ();
    delete prevRec,delete curRec;
}

void Sum :: Start () {
	Attribute tempAtts;
	Record *tempRec = new Record ();
    tempAtts.name = "SUM";
	double doubleSum = 0.0, doubleRec;
    int intSum = 0, intRec, val = 0;
	if (!in->Remove (tempRec)) {
		out->ShutDown ();
		return;
	}
	tempAtts.myType = compute->Apply (*tempRec, intRec, doubleRec);
	(tempAtts.myType == Int) ? intSum += intRec : doubleSum += doubleRec;
	while (in->Remove (tempRec)) {
		compute->Apply (*tempRec, intRec, doubleRec);
		(tempAtts.myType == Int) ? intSum += intRec : doubleSum += doubleRec;
	}
    stringstream ss;
    Schema sumSch (NULL, 1, &tempAtts);
	(tempAtts.myType != Int) ?  ss << doubleSum << '|' : ss << intSum << '|';
    tempRec->ComposeRecord (&sumSch, ss.str ().c_str ());
    val = intSum;
	out->Insert (tempRec);
	out->ShutDown ();
}

void WriteOut :: Start () {
    Record *tmp = new Record ();
    while (in->Remove (tmp)) {
        tmp->WriteToFile (file, schema);
    }
    fclose (file);
    delete tmp;
}

void GroupBy :: Start () {
    int *atts = order->whichAtts;
    double doubleSum = 0.0, doubleRec;
    int check = 0,intSum = 0, intRec, numAtts = order->numAtts;
	Type tempType;
	Pipe tempPipe (buffSize);
    int *attsToKeep = new int[numAtts + 1];
    char *sumStr = new char[buffSize];
    attsToKeep[0] = 0;
	BigQ bigq (*in, tempPipe, *order, runLen);
    for (int i = 0; i < numAtts; i++) {
        attsToKeep[i + 1] = atts[i];
    }
    Record *prevRec = new Record ();
	if (tempPipe.Remove (prevRec)) {
		tempType = compute->Apply (*prevRec, intRec, doubleRec);
		(tempType == Int) ? intSum += intRec : doubleSum += doubleRec;
	} else {
		out->ShutDown();
		delete sumStr, delete prevRec;
		exit (-1);
	}
    Attribute tempAtts;
	tempAtts.name = "SUM";
	tempAtts.myType = tempType;
    Schema *sumSch = new Schema (NULL, 1, &tempAtts);
    Record *tempRec = new Record ();
    Record *sumRec = new Record ();
    Record *curRec = new Record ();
	while (tempPipe.Remove (curRec)) {
        ComparisonEngine compEng;
		if (compEng.Compare (prevRec, curRec, order) != 0) {
			(tempType == Int) ? sprintf (sumStr, "%d|", intSum) : sprintf (sumStr, "%f|", doubleSum);
			sumRec->ComposeRecord (sumSch, sumStr);
            check = intSum;
			tempRec->MergeRecords (sumRec, prevRec, 1, prevRec->GetLength (), attsToKeep, numAtts + 1, 1);
            doubleSum = 0.0,intSum = 0;
			out->Insert (tempRec);
			compute->Apply (*curRec, intRec, doubleRec);
			(tempType == Int) ? intSum += intRec : doubleSum += doubleRec;
			prevRec->Consume (curRec);
		} else {
			compute->Apply (*curRec, intRec, doubleRec);
			(tempType == Int) ? intSum += intRec : doubleSum += doubleRec;
		}
	}
	(tempType == Int) ? sprintf (sumStr, "%d|", intSum) :sprintf (sumStr, "%f|", doubleSum);
	sumRec->ComposeRecord (sumSch, sumStr);
    check++;
	tempRec->MergeRecords (sumRec, prevRec, 1, prevRec->GetLength (), attsToKeep, numAtts + 1, 1);
	out->Insert (tempRec);
    check = intSum;
	out->ShutDown ();
	delete sumStr, delete sumSch, delete prevRec, delete curRec, delete sumRec, delete tempRec;
}

