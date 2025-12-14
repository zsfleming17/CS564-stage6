#include "catalog.h"
#include "query.h"
#include "stdlib.h"


/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
    Status status;
    AttrDesc attrDesc;
    int intValue;
    float floatValue;
    
    HeapFileScan scanner(relation, status);
    if (status != OK) { return status; }
    
    if (attrValue == NULL) {
        status = scanner.startScan(0, 0, STRING, NULL, EQ);
        if (status != OK) { return status; }
    }
    else {
        status = attrCat->getInfo(relation, attrName, attrDesc);
        if (status != OK) { return status; }
        
        char* filterPtr = NULL;
        
        if (type == INTEGER) {
            intValue = atoi(attrValue);
            filterPtr = (char*)&intValue;
        }
        else if (type == FLOAT) {
            floatValue = atof(attrValue);
            filterPtr = (char*)&floatValue;
        }
        else if (type == STRING) {
            filterPtr = (char*)attrValue;
        }
        
        status = scanner.startScan(attrDesc.attrOffset,
                                   attrDesc.attrLen,
                                   (Datatype)attrDesc.attrType,
                                   filterPtr,
                                   op);
        if (status != OK) { return status; }
    }
    
    RID rid;
    while (scanner.scanNext(rid) == OK) {
        status = scanner.deleteRecord();
        if (status != OK) { return status; }
    }
    
    return OK;
}


