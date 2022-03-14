#include "DBFile.h"

class HeapDBFile : public GenericDBFile{
	File dataFile;
	Page page;
	off_t pageNumber = 0;
	bool stillWriting = false;
	bool fileOpen = false;

public:
	HeapDBFile (); 

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
