#include "ListIterator.h"
// #include "pfor.h"
#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include "math.h"
#include <unistd.h>

using namespace std;


float temp_score[CONSTS::MAXD + 128];
unsigned int temp_store[CONSTS::MAXD];

size_t onDemandCpool::counter;
size_t onDemandCpool::capacity;
std::vector<onDemandCpool*> onDemandCpool::storage;
termsMap onDemandCpool::termsToInd;

void onDemandCpool::initPool(size_t cap) {
	counter = 0;
	capacity = cap;

	storage.reserve(cap);
	for(size_t i=0; i<cap; ++i)
		storage.push_back(NULL);
}

unsigned int readEntireFileToTmpBuffer(const std::string term, unsigned int* buffer){
    
    std::string fname = CONSTS::TERM_PATH + "/" + term;

	FILE* fin = fopen(fname.c_str(),"r");
	if(fin == NULL) {
		cout << "couldn't open " << fname << endl;
		return 0;
	}

	unsigned int r1 = 0;
	while( fread(&(buffer[r1]), sizeof(unsigned int), 1, fin) == 1 )
		r1 ++;

	fclose(fin);
	return r1;
}

inline bool isCollision(const std::string& victim, const std::vector<std::string>& peers) {
	for(size_t i=0; i<peers.size(); ++i)
		if(victim == peers[i])
			return true;
	return false;
}

void onDemandCpool::evictData() {
	data.clear();
	data = vecUInt(); //clearing and resizing was causing mem. leaks on my system - hence this
}

unsigned int onDemandCpool::materialize( const std::vector<std::string>& peers) {
	//test if hit
	termsMap::const_iterator ind = termsToInd.find(term);
	if(ind != termsToInd.end() && (*ind).second >= 0)
		return data.size(); //this one is materialized already

    //no hit, so we need to place it in storage
	while(storage[counter] && isCollision(storage[counter]->term,peers)) //pick non-colliding victim for eviction
		++counter;

	if(storage[counter]) { //the bucket is occupied
		storage[counter]->evictData();
		termsToInd[storage[counter]->term]=-1; //evict
		//profiler::getInstance().stepCounter(CONSTS::CNT_EVICTIONS);
	} // when query has duplicate terms this scheme should still work

	termsToInd[term]=counter;
	storage[counter]=this;
	counter = (counter+1)%capacity;

	//now actually get the data, update the buffer pointer and the size
	cout<<"In materialize: "<<term<<endl;
	size_t size = readEntireFileToTmpBuffer(term.c_str(),temp_store);

	data.reserve(size);
	for(unsigned int i=0;i<size;i++)
			data.push_back(temp_store[i]);

	return size;
}


void PostingsBlocker::generateBlocks() {
	unsigned int i = 0;
	assert(0==length%BS); //we currently assume that the postings count is block aligned
	vecUInt indices;
	for(unsigned int b = 0; b < length; ++b ) {
		indices.push_back(b);
		i = (i+1)%BS;
		if(i==0) {
			this->push_back(indices);
			indices.clear();
		}
	}
}

void RawIndexList::rankWithBM25(unsigned int* doclen) {
	float idf;
	scores.reserve(lengthOfList);
	for(size_t i = 0; i<lengthOfList; ++i)	{
		unsigned int did = doc_ids[i];
		float f  = freq_s[i];
		unsigned int dlen = doclen[ did ];
		idf = log( 1+(float)(CONSTS::MAXD - lengthOfList + 0.5)/(float)(lengthOfList + 0.5))/log(2) ;
		scores.push_back(idf * ( (float)(f * 3)/(float)(f + 2*( 0.25 + 0.75 * (float)(dlen)/(float)(130) ) ) ));
		// cout<<"score: "<<scores.back()<<endl;
		//sleep(1);
	}
	maxScoreOfList = *(std::max_element(scores.begin(),scores.end()));
}

template <typename T>
void dumpArrayToFile(const std::string& path, T* data, size_t sz) {
	FILE *fdo = fopen(path.c_str(),"w");
	for (size_t i = 0; i < sz; ++i)
		if (fwrite(&(data[i]), sizeof(T), 1, fdo) != 1)
			cout << "Failed writing to " << path << endl;
	fclose(fdo);
}

template <typename Iterator>
void dumpToFile(const std::string& path, Iterator start, Iterator end) {
	FILE *fdo = fopen(path.c_str(),"w");
	if(! fdo) {
		cout << "Failed writing to " << path << " (Hint: create folder for path?)" << endl;
		return;
	}
	assert(fdo);
	for (; start !=end; ++start)
		if (fwrite(&(*start), sizeof(typename Iterator::value_type), 1, fdo) != 1)
			cout << "Failed writing to " << path << " (Hint: create folder for path?)" << endl;
	fclose(fdo);
}

void CompressedList::serializeToFS(const std::string& rootPath ) {
	const std::string TERM_PATH = rootPath+"pool/";
	const std::string FLAG_PATH =  rootPath+"flag/";
	const std::string MAX_PATH = rootPath+"max/";
	const std::string SCORE_PATH = rootPath+"score/";
	const std::string SIZE_PATH = rootPath+"size/";

	dumpToFile(TERM_PATH+term,compressedVector.begin(),compressedVector.end()); // dump the compressed list
	dumpToFile(FLAG_PATH+term,flag.begin(),flag.end()); // dump the flag

	dumpToFile(SIZE_PATH+term,sizePerBlock.begin(),sizePerBlock.end()); // dump the size for each chunk
	dumpToFile(MAX_PATH+term,maxDidPerBlock.begin(),maxDidPerBlock.end());
	dumpToFile(SCORE_PATH+term,DEPmaxScorePerBlock.begin(), DEPmaxScorePerBlock.end());// dump the max score for each chunk
	// dump the min did for each chunk -- nobody used it
}

// void RawIndexList::rankWithBM25(unsigned int* doclen, unsigned int& unpadded_list_length) {
// 	float idf;
// 	scores.reserve(unpadded_list_length);
// 	for(size_t i = 0; i<unpadded_list_length; ++i)	{
// 		unsigned int did = doc_ids[i];
// 		float f  = freq_s[i];
// 		unsigned int dlen = doclen[ did ];
// 		idf = log( 1+(float)(CONSTS::MAXD - unpadded_list_length + 0.5)/(float)(unpadded_list_length + 0.5))/log(2) ;
// 		scores.push_back(idf * ( (float)(f * 3)/(float)(f + 2*( 0.25 + 0.75 * (float)(dlen)/(float)(130) ) ) ));
// 	}
// 	maxScoreOfList = *(std::max_element(scores.begin(),scores.begin() + unpadded_list_length));
// }

CompressedList::CompressedList(const RawIndexList& rl, const size_t bs) : BS(bs) {
	lengthOfList = rl.lengthOfList;
	unpadded_list_length = rl.unpadded_list_length;
	maxScoreOfList = rl.maxScoreOfList;
	term = rl.term;
	termId = rl.termId;

	compressDocsAndFreqs(rl);
}

void CompressedList::compressDocsAndFreqs(const RawIndexList& rl) {
	size_t ind;
	//BS = 64
	DEPmaxScorePerBlock.reserve(lengthOfList/BS);  
	maxDidPerBlock.reserve(lengthOfList/BS);

    //build delta list for doc ids； Take the d_gap;
	vecUInt deltaList = rl.doc_ids;
	for (ind = lengthOfList-1; ind > 0; ind--)
		deltaList[ind] -= deltaList[ind-1];

	//build delta list for freqs; All minus 1;
	vecUInt freq = rl.freq_s;
	for (ind = 0; ind < lengthOfList; ++ind)
		freq[ind]--;

	assert(lengthOfList == deltaList.size() && lengthOfList == freq.size());

	int j,flag1, flag2, numb;
	unsigned int *start, *ptrOfDelta, *ptrOfFreq;

	vecUInt buffer(lengthOfList*2);
	//buffer.resize(lengthOfList*2);

	flag.clear();
	flag.reserve(lengthOfList/BS);
	sizePerBlock.clear();
	sizePerBlock.reserve(lengthOfList/BS);

	start = &buffer[0];
	PostingsBlocker blocker(deltaList.size(),BS);
	blocker.generateBlocks();

	//blocks iterator:
	//for (size_t i = 0; i < lengthOfList; i += BS)	{
	for(BlocksIterator it = blocker.getIterator(); it!=blocker.getEnd(); ++it) {
		vecUInt deltaTmp = getVaues(*it,deltaList);
		vecUInt freqTmp = getVaues(*it,freq);
		std::vector<float> scoresTmp = getVaues(*it,rl.scores);
		unsigned int* deltaPtr = &(deltaTmp[0]);
		unsigned int* freqPtr = &(freqTmp[0]);

		maxDidPerBlock.push_back(AbsBlocksGen::getMaxDidOfBlock(it,rl.doc_ids)); //(rl.doc_ids[i+BS-1]);
		DEPmaxScorePerBlock.push_back(AbsBlocksGen::getMaxScoreOfBlock(it,rl.scores)); //( *(std::max_element(rl.scores.begin()+i,rl.scores.begin()+i+BS)) );

		flag1 = -1;
		for (j = 0; flag1 < 0; j++)	{
			ptrOfDelta = start;
			flag1 = pack_encode(&ptrOfDelta,deltaPtr, j);
		}

		flag2 = -1;
		for (j = 0; flag2 < 0; j++)	{
			ptrOfFreq = ptrOfDelta;
			flag2 = pack_encode(&ptrOfFreq, freqPtr, j);
		}

		flag.push_back(((unsigned int)(flag1))|(((unsigned int)(flag2))<<16));

		if (ptrOfFreq - start > 256)
			cout << "One block is " << ptrOfFreq - start << " bytes" << endl;

		sizePerBlock.push_back( (unsigned char)((ptrOfFreq - start) - 1));
		numb++;
		start = ptrOfFreq;
	} //end for block

	cpool_leng = start-&(buffer[0]);
	const size_t cells = cpool_leng;//sizeof(unsigned int);
	compressedVector.reserve(cells);
	for(size_t i=0; i<cells; ++i)
	compressedVector.push_back(buffer[i]); //put compressed docs and freqs into comressedVector

	// cout << term << "lengthOfList: " << lengthOfList <<endl<< "deltaList.size: " << deltaList.size() <<endl<< "cpool_leng: " << cpool_leng << endl;

}

void BaseLptr::open(const std::string term, const unsigned int lengthOfLst) {
	reset(); //part of the initialization is done here;
    vecUInt data;
	//materialize compressed data if not present and save its size
	size_t size = readEntireFileToTmpBuffer(term,temp_store);

	data.reserve(size);
	for(unsigned int i=0;i<size;i++)
			data.push_back(temp_store[i]);

	cpool_leng = size;
	cpool.data = data;
	off = &(data[0]);
	currMax = maxDidPerBlock[0];
	decmpPtr = pack_decode(this->dbuf, this->off, this->flag[this->block]&65535, 0);
	did = dbuf[0];
	unpadded_list_length = lengthOfLst;
	// compute pre using the unpadded list length
	pre = (float)3 * (float)(  log( 1 + (float)(CONSTS::MAXD - unpadded_list_length + 0.5)/(float)( unpadded_list_length + 0.5))/log(2));

	// set padded list length without storing anything
	// Note: lengthofList is unsigned int, but the expression can not be less than 0, so it's safe
	lengthOfList = lengthOfLst + (CONSTS::BS - (unpadded_list_length%CONSTS::BS));
}

lptr::lptr(CompressedList & cl){ //by me
	//--------------------this part corrsponds to load func---------------------------
	term = cl.term;
	cpool_leng = cl.cpool_leng;
	info_leng = cl.info_leng;
	maxDidPerBlock = cl.maxDidPerBlock;
	DEPmaxScorePerBlock = cl.DEPmaxScorePerBlock;
	sizePerBlock = cl.sizePerBlock;
	maxScoreOfList = cl.maxScoreOfList;
	flag = cl.flag;
	//--------------------this part corrsponds to open func---------------------------
	block = 0;
	dflag = 1;
	fflag = 0;
	elem = 0;
	checkblock = 0;
	lengthOfList = cl.lengthOfList;
	unpadded_list_length = cl.unpadded_list_length;
	compressedVector = cl.compressedVector;
	off = &(cl.compressedVector[0]);
	currMax = maxDidPerBlock[0];
	decmpPtr = pack_decode(this->dbuf, this->off, this->flag[this->block]&65535, 0);
	did = dbuf[0];

	// compute pre using the unpadded list length
	pre = (float)3 * (float)(  log( 1 + (float)(CONSTS::MAXD - unpadded_list_length + 0.5)/(float)( unpadded_list_length + 0.5))/log(2));
	cout<<"padded_list_length: "<<lengthOfList<<endl;
	lengthOfList = lengthOfList + (CONSTS::BS - (unpadded_list_length%CONSTS::BS));
	cout<<"MAXD: "<<CONSTS::MAXD<<endl;
	cout<<"unpadded_list_length: "<<unpadded_list_length<<endl;
	cout<<"padded_list_length: "<<lengthOfList<<endl;
	cout<<"pre: "<<pre<<endl;
}

void SingleHashBlocker::generateMaxes(const std::vector<float>& scores) {
	size_t block = 0;
	std::vector<float> localScores;
	const size_t numOfBlocks(1+whereDidMapped(CONSTS::MAXD));// CONSTS::MAXD/2^bits+1 blocks

	assert(scores.size() == docIds->size());

	for(size_t i = 0; i<numOfBlocks;  ++i)  //initialize SingleHashBlocker.blockIdToMaxScore to all zeros
		blockIdToMaxScore.push_back(0.0);  //blockIDToMaxScore is to store the Doc-id Oriented Blockmax score

//	if(CONSTS::MAXD < docIds->at( docIds->size()-1) )
	//	COUT << docIds->at(docIds->size()-1) << Log::endl;

	for(size_t i=0; i<docIds->size() && docIds->at(i)<CONSTS::MAXD; ++i) {
		const unsigned int cblock =  whereDidMapped((*docIds)[i]); //as docid increases, cblock will also increase tho after shift operation  
		if(cblock != block) { //when cblock increases, after the shift operation; 
			if(localScores.size())//localScores.size()!=0
				blockIdToMaxScore[block] = *(std::max_element(localScores.cbegin(), localScores.cend()));//put the max score into blockIdToMaxScore
			localScores.clear();
			block = cblock;
		}
		localScores.push_back(scores[i]); //localScores is to store the scores within a block
	}

	if(block == numOfBlocks)
		cout << "ha?" << endl;
	if(localScores.size())  //to complete the last block
		blockIdToMaxScore[block] = *(std::max_element(localScores.cbegin(), localScores.cend()));
}
