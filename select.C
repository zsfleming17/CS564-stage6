#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;

    Status status;
    int intValue;
    float floatValue;
    int recLen = 0;

    // get AttrDesc for each projection attribute
    AttrDesc projDescs[projCnt];
    for (int i = 0; i < projCnt; i++) {
        status = attrCat->getInfo(projNames[i].relName,
                                projNames[i].attrName,
                                projDescs[i]);
        if (status != OK) return status;
    }

    // compute output record length
    for (int i = 0; i < projCnt; i++) {
        recLen += projDescs[i].attrLen;
    }

    // set up filter for selection
    AttrDesc attrDesc;
    char *filter = NULL;

    if (attr != NULL) {
        status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
        if (status != OK) return status;

        // convert attrValue to proper type
        if (attrDesc.attrType == INTEGER) {
            intValue = atoi(attrValue);
            filter = (char*)&intValue;
        }
        else if (attrDesc.attrType == FLOAT) {
            floatValue = atof(attrValue);
            filter = (char*)&floatValue;
        }
		else
        {
            filter = (char*)attrValue;
        }
    }
    // call ScanSelect()
    return ScanSelect(result, projCnt, projDescs,
                    attr ? &attrDesc : NULL,
                    op, filter, recLen);
}


const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

    Status status;
    RID rid;
    Record rec;
    Record outputRec;
    int outputOffset;

    // get relation name
    string relation = projNames[0].relName;

    // open scanner on the relation
    HeapFileScan scanner(relation, status);
    if (status != OK) return status;

    // start scan with or without filter
    if (attrDesc == NULL) {
        status = scanner.startScan(0, 0, STRING, NULL, EQ);
    }
    else
    {
        status = scanner.startScan(attrDesc->attrOffset,
                                attrDesc->attrLen,
                                (Datatype)attrDesc->attrType,
                                filter,
                                op);
    }
    if (status != OK) return status;

    // open result
    InsertFileScan resultRel(result, status);
    if (status != OK) return status;

    char outputData[reclen];
    outputRec.data = (void*)outputData;
    outputRec.length = reclen;

    // scan and project
    while (scanner.scanNext(rid) == OK) {
        status = scanner.getRecord(rec);
        if (status != OK) return status;

        // project attributes to output
        outputOffset = 0;
        for (int i = 0; i < projCnt; i++) {
            memcpy(outputData + outputOffset,
                (char*)rec.data + projNames[i].attrOffset,
                projNames[i].attrLen);
            outputOffset += projNames[i].attrLen;
        }
        // insert into result
        RID outRID;
        status = resultRel.insertRecord(outputRec, outRID);
        if (status != OK) return status;
    }
    return OK;
}
