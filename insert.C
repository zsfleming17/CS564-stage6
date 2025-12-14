#include "catalog.h"
#include "query.h"

#include <cstring> 
#include <cstdlib>  
#include <new>
/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
// Basic parameter checks
if (relation.empty() || attrCnt <= 0 || attrList == nullptr) {
	return BADCATPARM; 
}
if (attrCat == nullptr) {
	return BADCATPARM;
}

// Get full schema for relation from AttrCatalog
int relAttrCnt = 0;
AttrDesc *relAttrs = nullptr;

Status status = attrCat->getRelInfo(relation, relAttrCnt, relAttrs);
if (status != OK) return status;
if (relAttrCnt <= 0 || relAttrs == nullptr) {
	
	if (relAttrs) delete[] relAttrs;
	return BADCATPARM;
}

// Compute record length
int recLen = 0;
for (int i = 0; i < relAttrCnt; i++) {
	int endPos = relAttrs[i].attrOffset + relAttrs[i].attrLen;
	if (endPos > recLen) recLen = endPos;
}
if (recLen <= 0) {
	delete[] relAttrs;
	return BADCATPARM;
}

// Allocate and zero the record buffer
char *record = new (std::nothrow) char[recLen];
if (!record) {
	delete[] relAttrs;
	return INSUFMEM;
}
memset(record, 0, recLen);

//find the provided attrInfo for an attribute name
auto findProvided = [&](const char *attrName) -> const attrInfo* {
	for (int j = 0; j < attrCnt; j++) {
		if (strcmp(attrList[j].attrName, attrName) == 0) {
			return &attrList[j];
		}
	}
	return nullptr;
};

// For each attribute, find it in attrList and pack at offset
for (int i = 0; i < relAttrCnt; i++) {
	const AttrDesc &schemaA = relAttrs[i];

	const attrInfo *provided = findProvided(schemaA.attrName);

	if (provided == nullptr || provided->attrValue == nullptr) {
		delete[] record;
		delete[] relAttrs;
		return BADCATPARM; 
	}

	const int off = schemaA.attrOffset;

	// Type-check + pack
	if (schemaA.attrType == INTEGER) {
		int v = std::atoi(reinterpret_cast<const char*>(provided->attrValue));
		memcpy(record + off, &v, sizeof(int));

	} else if (schemaA.attrType == FLOAT) {
		float f = static_cast<float>(std::atof(reinterpret_cast<const char*>(provided->attrValue)));
		memcpy(record + off, &f, sizeof(float));

	} else if (schemaA.attrType == STRING) {
		const char *s = reinterpret_cast<const char*>(provided->attrValue);
		strncpy(record + off, s, schemaA.attrLen);

	} else {
		// Unknown datatype
		delete[] record;
		delete[] relAttrs;
		return BADCATPARM;
	}
}

// Inserting the packed record into the heapfile
RID rid;
InsertFileScan ifs(relation, status);
if (status != OK) {
	delete[] record;
	delete[] relAttrs;
	return status;
}

Record rec;
rec.data = record;
rec.length = recLen;

status = ifs.insertRecord(rec, rid);

delete[] record;
delete[] relAttrs;
return status;

}

