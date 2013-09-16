#include <zlib.h>
#include <cstdio>
#include <cstdlib>
#include <string.h>
#include <malloc.h>
#include "parser.h"
#include <algorithm>
#include <vector>
#include <map>
#include <string>
#include <iostream>
#include <locale>
#include "trim.h"
#include <sstream>
#include <time.h>
#include <assert.h>
//#include<stdio.h>
//#include "vbyte.h"



#define INDEX_CHUNK 7000000
#define DATA_CHUNK 9097152
#define INVERTED_INDEX_CHUNK 200000000   // The size of global inverted index ( worid : < DocID Tf> < DocID Tf> < DocID Tf> ...... )in ascii is about 100MB.

using namespace std;


/******************************************
 *Read a gz file into a buffer
 *@param fileName    the filename to be read
 *@param size      the initial size of the buffer
 *@return the buffer
 ******************************************/


vector<pair<string,string> > getFileList()
{
	int N = 83;
	string indexlist[N];
	string datalist[N];

	string basepath = "/home/alex/Downloads/nz2_merged/" ;	
	string index_end = "_index";
	string data_end = "_data";
	
	
	vector<pair<string,string> > filepair;

	for(int i=0; i<N ; i++)
	{
		stringstream ss;
		string num;
		ss<<i;
		ss>>num;
		indexlist[i] = basepath + num + index_end;
		datalist[i] = basepath + num + data_end;
		
		filepair.push_back(make_pair(indexlist[i],datalist[i]));
	}
	return filepair;
}


int vbyte_compress(char *src,unsigned int n)   
{
	int len = 0;

	while(n >= 128)
	{
		*(src+len) = (unsigned char)((n & 0xff)|0x80);   // set the 8th bit 
		n >>= 7;
		len++;
	}

	*(src+len) = (unsigned char)n;
	
	len++;
	
	*(src+len) = 0;

	return len;
}



char *memAlloc(gzFile fileName, int size)    
{
    char *buffer=(char *)malloc(size);
    int oldSize=size;

    int count=0;             //The number of bytes that already read
    while (!gzeof(fileName))
    {       
        count+=gzread(fileName,buffer+count,oldSize);        
        if (count==size)                    // Reallocate when buffer is full
        {
            oldSize=size;
            size*=2;
            buffer=(char *)realloc(buffer,size);
        }
    }
    return buffer;
}



unsigned int readLineMem(char* buffer, unsigned int start)
{
	unsigned int i = start;
	while(buffer[i] != '\0')
	{
		if(buffer[i] == '\n')
		{
			return i-start+1 ;
		}
		i++;
		
	}
	
}


char* getLineCol(char* aString, unsigned int column)
{
    int j, i; 
    char *token[7]; //hold 7 tokens

    token[0] = strtok(aString, " "); //get pointer to first token

    if(column == 0)
	return token[0];
    else
    {
	    i =1;
	    for(;i <= column ;i++ )
	    {
		token[i] = strtok(NULL," ");
	    }

            return token[column];
    }
}


void substr(char *dest, const char* src, unsigned int start, unsigned int count) 
{
   strncpy(dest, src + start, count);
   dest[count] = '\0';
}


map < string, unsigned int > Lexicon;  // < Word, WordID >   TreeMap
map < string,  unsigned int  > URLMap;  // < URL, DocID >    TreeMap
map <unsigned int, vector< pair<unsigned int,unsigned int > > >  TempIndex; // Temporary invered index,  <    WordID,  <pair1<DocID, TF>,pair2<DocID,TF>,...>     >   
vector < vector < pair < unsigned int, unsigned int > > >  IndexV;  //  Total/Global inverted index,          WordID ==> Vector< <DocID, TF>, <DocID, TF>, ...> 
//map < unsigned int, unsigned int > MaxDocID;           // <WordID, MaxDocID>  the word's Largest DocID, which is easy for us to update when adding a new page
//map < unsigned int, unsigned int > WordTFSum;          // <WordID,WordTFSum>  the word's Term Frequency Sum
vector< pair <string,unsigned int> > LexiconByValue;     // Lexicon sorted by value (WordID).
vector< pair <string,unsigned int> > URLMapByValue;      // URL sorted by value (DocID).


///////////////////////////////////  Sort by value (ID) rather than key(string)  //////////////////////////////////////////////////

int cmp(const pair<string,unsigned int>& x,const pair<string,unsigned int>& y)  
{  
    return x.second<y.second;  
}
  
void sortMapByValue(map < string,unsigned int >& tMap,vector< pair< string,unsigned int> >& tVector)  
{  
      for(map<string, unsigned int>::iterator curr=tMap.begin();curr!=tMap.end();curr++)  
      {  
         tVector.push_back(make_pair(curr->first,curr->second));  
      }  

      sort(tVector.begin(),tVector.end(),cmp);  
}  

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////



unsigned int Build_Lexicon(string word)
{
	unsigned int WordID; 
        map <string, unsigned int >::iterator idx;
	idx = Lexicon.find(word);

	if(idx == Lexicon.end())    // this is a new word.
	{
		WordID = Lexicon.size();  // Stored numbers are from 0 to URLMap.size-1 ; so new WordID can be the value of URLMap.size.
		Lexicon.insert(pair<string, unsigned int>(word, WordID));
		return WordID;
	}
	else
	{
		return idx->second;
	}
}


unsigned int Build_URLMap(string URL)
{
	unsigned int DocID; 
        map <string, unsigned int >::iterator idx;
	idx = URLMap.find(URL);

	if(idx == URLMap.end())   // this is a new URL.
	{
		DocID = URLMap.size();    // Stored numbers are from 0 to URLMap.size-1 ; so new DocID can be the value of URLMap.size.  
		URLMap.insert(pair<string, unsigned int>(URL, DocID));
		return DocID;
	}
	else
	{
		return idx->second;
	}
}




void Build_Gloal_Index(unsigned int WordID, unsigned int DocID)
{
    unsigned int  len = IndexV.size();    // max_WordID

    vector < pair < unsigned int, unsigned int > > postinglist;

    if( WordID >= len)      // it means this is a new word
    {
    	postinglist.push_back(make_pair( DocID, 1));
	IndexV.push_back(postinglist);
    }
    else                  // it means this is an old word
    {

	if(IndexV[WordID].back().first == DocID) 
		IndexV[WordID].back().second++;
	else
		IndexV[WordID].push_back(make_pair(DocID,1));
    }
}


void Build_Temp_Index(unsigned int WordID, unsigned int DocID)
{

    map <unsigned int, vector< pair<unsigned int,unsigned int > > >::iterator iter;

    iter = TempIndex.find(WordID); 

    if(  iter != TempIndex.end() )
    {
		vector< pair<unsigned int,unsigned int> >::iterator cur = iter->second.end();
		--cur;  // ==> iter->second.back();

	if( cur->first  == DocID )
		cur->second++;
	else
	{
		pair< unsigned int,unsigned int > posting;
		posting = make_pair(DocID,1);
		iter->second.push_back(posting);
	}
	
    }
    else 
    {
		vector< pair<unsigned int,unsigned int > > postinglist;
		pair< unsigned int,unsigned int > posting;
		posting = make_pair(DocID,1);
		postinglist.push_back(posting);
		TempIndex.insert(make_pair(WordID, postinglist));
    }
}


int saveTempIndex(string tempIndexFilepath)
{
	FILE* f = fopen(tempIndexFilepath.data(), "w");
	if(f == NULL) return 1;


	map <unsigned int, vector< pair<unsigned int,unsigned int > > >::iterator  it = TempIndex.begin();


        for (; it != TempIndex.end(); ++it )
        {
			fprintf(f, "%d : ", it->first);
			vector< pair<unsigned int,unsigned int> >::iterator iter = it->second.begin(), end = it->second.end();

					for( ; iter != end; ++iter)
					{
				fprintf(f, "< %d %d > ", iter->first, iter->second);
					}
			fprintf(f, "\n");
        }

	fclose(f);
	return 0;
}



int saveTempIndex2(string tempIndexFilepath)                           // <1, 300, 10> <2,400,2>  ..... stored in the way rather than "saveTempIndex"
{
	FILE* f = fopen(tempIndexFilepath.data(), "w");
	if(f == NULL) return 1;
	unsigned int tempbuf[3];


	map <unsigned int, vector< pair<unsigned int,unsigned int > > >::iterator  it = TempIndex.begin();

       

        for (; it != TempIndex.end(); ++it )
        {

				vector< pair<unsigned int,unsigned int> >::iterator iter = it->second.begin(), end = it->second.end();

                for( ; iter != end; ++iter)
                {
					tempbuf[0]=it->first;
					tempbuf[1]=iter->first;
					tempbuf[2]=iter->second;
					fwrite(tempbuf,12,1,f);
                }
        }

	fclose(f);
	return 0;
}

int saveURLMap(string URLMapFilepath)
{
	FILE* f = fopen(URLMapFilepath.data(), "w");
	if(f == NULL) return 1;

	map<string, unsigned int>::iterator itr = URLMap.begin();

	for(; itr != URLMap.end(); ++itr) {
		fprintf(f, "%d %s\n", itr->second ,itr->first.data() );
	}

	fclose(f);
	return 0;
}

int saveURLMapByID(string URLMapFilepath)
{

	FILE* f = fopen(URLMapFilepath.data(), "w");
	if(f == NULL) return 1;

	sortMapByValue(URLMap,URLMapByValue); 

	vector< pair <string,unsigned int> > ::iterator itr = URLMapByValue.begin();

	for(; itr != URLMapByValue.end(); ++itr) {
		fprintf(f, "%d %s\n", itr->second, itr->first.data());
	}

	fclose(f);
	//cout<<"saveURLMapByID OK"<<endl;
	return 0;
}

int saveLexicon(string LexFilepath)
{
	FILE* f = fopen(LexFilepath.data(), "w");
	if(f == NULL) return 1;

	map<string, unsigned int>::iterator itr = Lexicon.begin();

	for(; itr != Lexicon.end(); ++itr) {
		fprintf(f, "%s %d\n", itr->first.data(), itr->second);
	}

	fclose(f);
	return 0;
}


int saveLexiconByID(string LexFilepath)   // Lexicon sorted by value (WordID).
{


	FILE* f = fopen(LexFilepath.data(), "w");
	if(f == NULL) return 1;

	sortMapByValue(Lexicon,LexiconByValue); 

	vector< pair <string,unsigned int> > ::iterator itr = LexiconByValue.begin();

	for(; itr != LexiconByValue.end(); ++itr) {
		fprintf(f, "%d %s\n", itr->second, itr->first.data());
	}

	fclose(f);
	//cout<<"saveLexiconBYID OK"<<endl;
	return 0;

}


int saveGlobalIndex(string TempIndexpath)
{
	FILE* f = fopen(TempIndexpath.data(), "w");
	if(f == NULL) return 1;

	vector < vector < pair < unsigned int, unsigned int > > > ::iterator itr1 = IndexV.begin(), end1 = IndexV.end();
	
	for(; itr1 != end1 ; ++itr1)
	{	
		vector < pair < unsigned int, unsigned int > >::iterator itr2 = itr1->begin(), end2 = itr1->end();
		
		fprintf(f, "%d : ", itr1-IndexV.begin());
		for(; itr2 != end2; ++itr2)
			fprintf(f, " %d %d",  itr2->first, itr2->second);
	
		fprintf(f, "\n");
	}

	fclose(f);
	//cout<<"saveGlobalindex OK"<<endl;
	return 0;
}



/* 
 * save the global inverted index in the binary format using variable bytes coding algorithm
 */

int IndexGlobalCompress(string Indexpath)
{	
	char* BufOut = (char*) malloc(INVERTED_INDEX_CHUNK);

	if(BufOut == NULL)
	{
		cout << "IndexGlobalCompress Memory Exhausted" << endl;
		exit(1);
	}
	else {cout<<"go"<<endl;}

	char* pointer ;
	unsigned int n = 0, lenRecord = 0;
	
	if(BufOut == NULL) return 0; // fail to allocate the buffer
	char Delimiter;
	
	Delimiter = (char)((n | 0xff) & 0x80);
	
	pointer = BufOut;

	char temp[20];   // it's large enough to hold a vbyte number.
	int len;

	vector < vector < pair < unsigned int, unsigned int > > > ::iterator itr1 = IndexV.begin(), end1 = IndexV.end();

	for(; itr1 != end1 ; ++itr1)
	{	
		vector < pair < unsigned int, unsigned int > >::iterator itr2 = itr1->begin(), end2 = itr1->end();
		unsigned int *word_id;
		*word_id = itr1-IndexV.begin();  // the value of Word_ID
		memcpy(pointer,(void *)word_id, 4);    // unsigned int is 4 bytes.  I don't need to use  (void *) BufOut   here.
		pointer = pointer + 4;
		lenRecord = lenRecord + 4;


		for(; itr2 != end2; ++itr2)
		{
			len = vbyte_compress(temp,itr2->first);
			lenRecord = lenRecord + len;
			memcpy(pointer, temp, len);
			pointer = pointer + len;

			len = vbyte_compress(temp,itr2->second);
			lenRecord = lenRecord + len;
			memcpy(pointer,temp,len);
			pointer = pointer + len;
		}
		memcpy(pointer, &Delimiter, 1);    // 0x80,  which is 1000 0000 in binary, dinstiguishes 2 word_postinglist.
		lenRecord = lenRecord + 1;  
	}
	

	FILE* f = fopen(Indexpath.data(), "w");
	if(f == NULL) return 1;

	cout<<lenRecord<<endl;

	fwrite(BufOut,lenRecord,1,f);
	fclose(f);

	free(BufOut);
	
	return 0;
	
} 


vector<string> split(string& src, string separate_character)  
{  
    vector<string> strs;  
      
    int separate_characterLen = separate_character.size();
    int lastPosition = 0,index = -1;  
    while (-1 != (index = src.find(separate_character,lastPosition)))  
    {  
        strs.push_back(src.substr(lastPosition,index - lastPosition));  
        lastPosition = index + separate_characterLen;  
    }  
    string lastString = src.substr(lastPosition);
    if (!lastString.empty())  
        strs.push_back(lastString);
    return strs;  
}



// To process one pair of index and data gzip files.

void runOnce(string indexfile, string datafile, int number)
{
    gzFile cData,cIndex;
    char *indexBuffer, *dataBuffer, *pool, *TempIndexBuffer;
    char textStr[1000000];
    char indexStr[1000];
    unsigned int indexStartPoint,dataStartPoint,len,offset,lengOfIndex, lengOfPage, ret;
    unsigned int LexWordID ;
    unsigned int URLDocID ;
    cIndex=gzopen(indexfile.data(),"r");
    cData=gzopen(datafile.data(),"r");

    indexBuffer = memAlloc(cIndex, INDEX_CHUNK);

    if(indexBuffer == NULL)
    {
		exit(1);
    }

    dataBuffer = memAlloc(cData, DATA_CHUNK);

    if(dataBuffer == NULL)
    {
		exit(1);
    }

    lengOfIndex = strlen(indexBuffer);

    indexStartPoint = 0;
    dataStartPoint = 0;

    while(1)
    {
		if(indexStartPoint >= lengOfIndex)
			break;
		len = readLineMem(indexBuffer, indexStartPoint);

		substr(indexStr,indexBuffer,indexStartPoint,len);
		

		offset = atoi( getLineCol(indexStr,3) );

		indexStartPoint = indexStartPoint + len;

		substr(textStr,dataBuffer,dataStartPoint,offset);
		dataStartPoint = dataStartPoint + offset;

			lengOfPage = offset;

		pool = (char*)malloc(2*lengOfPage+1);

		URLDocID = Build_URLMap(getLineCol(indexStr,0));

		ret = parser(getLineCol(indexStr,0), textStr, pool, 4*lengOfPage+1);

			
		if (ret > 0)
		{
			string s(pool);

			vector<string>  strs = split( s, "   ");

			for ( unsigned int i = 0; i < strs.size(); i++)    
			{    
				
				trim( strs[i] );
				if(strs[i].empty()) continue;
				LexWordID = Build_Lexicon( strs[i] ); 
				Build_Gloal_Index(LexWordID,URLDocID);
				Build_Temp_Index(LexWordID, URLDocID);
			}

			s.clear();
		}
		free(pool);
    }

    stringstream ss;
    string numStr;
    ss<<number;
    ss>>numStr;

    saveTempIndex("/home/alex/mycode/readgz/savedata/tempindex"+numStr);
  

    TempIndex.clear();

    free(indexBuffer);
    free(dataBuffer);

    gzclose(cIndex);
    gzclose(cData);
	
}



int main (int argc, char * argv[])
{
	clock_t start_time=clock();	

	vector<pair<string,string> >  filelist;
	filelist = getFileList();
	
	vector<pair<string,string> >::iterator itr = filelist.begin(), end = filelist.end();

	for(;itr != end; ++itr)
	{
		runOnce(itr->first,itr->second,itr-filelist.begin());
		
	}

  	saveLexiconByID("/home/alex/mycode/readgz/savedata/llexfileByIDs");
    saveURLMapByID("/home/alex/mycode/readgz/savedata/uurlmapByIDs");
	saveGlobalIndex("/home/alex/mycode/readgz/savedata/gglobalIndex");	
	//IndexGlobalCompress("/home/alexwang/mycode/readgz/savedata/cprglobalidx");

	clock_t end_time=clock();
	
	cout<< "Running time is: "<<static_cast<double>(end_time-start_time)/CLOCKS_PER_SEC*1000<<"ms"<<endl;

	return 0;
}




