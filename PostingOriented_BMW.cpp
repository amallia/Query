/*
 * PostingOriented_BMW.cpp
 *
 *  Created on: Jan 14, 2012
 *      Author: sergeyn
 */


#include "PostingOriented_BMW.h"
#include "globals.h"
#include "profiling.h"

inline bool PostingOriented_BMW::pickCandidate(lptrArray& lps, const float threshold, const int numOfTermsInQ, int& pivot, int& smallestMaxDid, float& sumOfBLockMaxesUptoPivot) {
	pivot = -1; 		// find the pivot term

	//select pivot candidate like you did in wand -- according to list-max-scores
	float sumOfListMaxScores = 0.0;
	for( int i = 0; i < numOfTermsInQ; ++i)		{
		sumOfListMaxScores += lps.getListMaxScore(i);
		if( sumOfListMaxScores > threshold ) {
			//small optimization: if ALL dids after pivot-list are equal, it's safe to move the pivot down
			while( i + 1 < numOfTermsInQ && lps[i + 1]->did == lps[i]->did)
				++i;

			pivot = i;
			break;
		}
	}

	lptr* lpspivot = lps[pivot];
	if(pivot == -1 || lpspivot->did >= CONSTS::MAXD) // pivot is -1 or MAXD, no item will make into topK
		return false;

	//smallestMaxDid is used in handleFake and it is the smallest maxDidPerBlock[] + 1 of the lists up to pivot. Initialization here with pivot list only
	smallestMaxDid = lpspivot->currMax > CONSTS::MAXD ? CONSTS::MAXD : lpspivot->currMax+1; 
	//this is overapprox. of the real sum of scores and used in eval. in handleReal. Initialization here with pivot list only.
	sumOfBLockMaxesUptoPivot = lpspivot->DEPgetMaxScoreOfDeepBlock();

	//in this loop we find smallestMaxDid and  sumOfBLockMaxesUptoPivot
	for(int i = 0; i < pivot; ++i) 	{
		int tempd = lps[i]->DEPshallowMove(lpspivot->did);
		if( smallestMaxDid >  tempd)
			smallestMaxDid = tempd;
		sumOfBLockMaxesUptoPivot += lps[i]->DEPmaxScorePerBlock [ lps[i]->checkblock ];
	}

	return true;
}

inline void PostingOriented_BMW::handleReal(lptrArray& lps, const int pivot, float sumOfBLockMaxesUptoPivot, QpResult* res, const float threshold, const int topK) {
	// evaluate this did only when did is equal to lps[0]->did
	lptr* lpspivot = lps[pivot];
	const int& did = lpspivot->did; // did is initialized with pivot's did
	float sumOfDidScores = 0.0f;
	if( did == lps[0]->did ) { // this dude needs to be evaluated!!
		//++orsize;
		for(int i = 0 ; i < pivot + 1; ++i)	{
			//PROFILER(CONSTS::EVAL);
			const float f = lps[i]->getFreq();//DEPgetFreq();
			const float scoret = lps[i]->calcScore(f,pages[did]); //DEPcalcScore
			sumOfDidScores += scoret;
			sumOfBLockMaxesUptoPivot -= ( lps[i]->DEPgetMaxScoreOfDeepBlock()/*score[ lps[i]->block ]*/ - scoret ); //change block score to real did score
			//early termination after substr. deltas of real scores from approximated upper bound
			if( !(sumOfBLockMaxesUptoPivot > threshold) )
				goto mvptrs;
		}

		if (sumOfDidScores > threshold)
		{  //heapify the new result
			int j;
			for (j = topK-2; (j >= 0) && (sumOfDidScores > res[j].score); j--)
				res[j+1]=res[j];
			res[j+1].setR(did,sumOfDidScores);
		} //end heapify
mvptrs:
		for(int i = 0 ;i < pivot + 1; ++i)  //moving pointers is done here
			{   
				// cout<<"lps["<<i<<"]->did: "<<lps[i]->did<<endl;
		        // cout<<"did: "<<did<<endl;
				lps[i]->did = lps[i]->nextGEQ( did + 1);
				// cout<<"After the moving, lps["<<i<<"]->did: "<<lps[i]->did<<endl;
				// int test;
				// cin>>test;
			}

		for(int i = pivot ; i > -1; i--)//reorder list
			lps.popdown(i);
	} //end: did == lps[0]->did
	else {      //find the shortest the list and move the did pointer forward of the list
		int least_ft = lps[0]->lengthOfList;
		int least_i = 0;
		for(int i = 1; i < pivot ; ++i)
		{
			if( lps[i]->lengthOfList < least_ft &&  lps[i]->did != did )
			{
				least_ft = lps[i]->lengthOfList;
				least_i = i;
			}
		}
		lps[least_i]->did = lps[least_i]->nextGEQ( did );
		lps.popdown(least_i); //sorted by did
	}
}

inline bool PostingOriented_BMW::handleFake(lptrArray& lps, const int pivot, const int smallestMaxDid, const int numOfTermsInQ) {
	int least_ft = lps[0]->lengthOfList;
	int least_i = 0;
	for(int i = 1; i < pivot + 1; ++i) { //find shortest list
		if( lps[i]->lengthOfList < least_ft) {
			least_ft = lps[i]->lengthOfList;
			least_i = i;
		}
	}

	int did = (( pivot < numOfTermsInQ - 1) && smallestMaxDid > lps[ pivot + 1 ]->did)? lps[ pivot + 1 ]->did : smallestMaxDid;   //first set did the bigger one between the smallest within the pivot lists and the first one outside the pivot lists

	if( did <= lps[pivot]->did)    //then if did is smaller than pivot did, set it as pivot did+1
		did = lps[pivot]->did + 1;

	if(did > CONSTS::MAXD)
		return false;

	lps[least_i]->did = lps[least_i]->nextGEQ( did );    //advance the shortest list's pointer
	lps.popdown(least_i);  // sorted by did
	return true;
}

void PostingOriented_BMW::operator() (lptrArray& lps, const int topK, QpResult* res){
	float sumOfBLockMaxesUptoPivot;
	int pivot, smallestMaxDid;
	lptr* lpspivot;
	int numOfTermsInQuery = lps.size();

	for (int i=0; i<topK; ++i) { // initialize results heap
		res[i].did = -1;
		res[i].score = -1.0;
	}

	lps.sort(); //initial sorting by did

	while(true) { // break loop if docID in round is more than maximum docID in the collection
		const float threshold = res[topK-1].score; // update current threshold

		if(! pickCandidate(lps, threshold, numOfTermsInQuery, pivot, smallestMaxDid, sumOfBLockMaxesUptoPivot)) // pick pivot list
			break;
		//pickCandidate according to list max score, so block-max check may fail
		if( sumOfBLockMaxesUptoPivot > threshold ) { // if block-max check passes, do partial evaluation
			handleReal(lps, pivot, sumOfBLockMaxesUptoPivot ,res, threshold, topK);
		} else { // if block-max check fails, skip (as described in the SIGIR11 paper)
			if(! handleFake(lps, pivot, smallestMaxDid, numOfTermsInQuery))
				break;
		}
	}
}
