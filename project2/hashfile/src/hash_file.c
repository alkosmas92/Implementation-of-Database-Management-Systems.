#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hash_file.h"
#define MAX_OPEN_FILES 20


#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HT_ERROR;        \
  }                         \
}

typedef struct{
	int record_count;
	Record records[8];
	int next_block;
}Block;


int array_files[MAX_OPEN_FILES]={-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1};
int array_files_counter=0;


int HASH_FUNCTION(int id,int buckets);

Record * CopyRecords(Record* source, Record* dest);

HT_ErrorCode HT_Init() {
  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_CreateIndex(const char *filename, int buckets) {

	if(BF_CreateFile(filename) < 0){
		printf("Error 1 \n");
	    return -1;
	}

	BF_Block *block;

	int fd,n;
	int i = 0;
	int* x;
	int	filetype = 1;
	BF_CreateFile(filename);

	BF_OpenFile(filename,&fd);
	BF_Block_Init(&block);
	BF_AllocateBlock(fd,block);


	BF_GetBlock(fd,0,block);
		void* data =BF_Block_GetData(block);

	    memcpy( data,&filetype,sizeof(int)); //epeidi fitype einai type int

	   memcpy( data+sizeof(int),&buckets,sizeof(int));
	BF_Block_SetDirty(block);
	BF_UnpinBlock(block);
	//free(information);
	BF_Block_Destroy(&block);

	BF_Block *block1;
		BF_Block_Init(&block1);

		BF_AllocateBlock(fd,block1);

	BF_GetBlock(fd,1,block1);
	void* data1=BF_Block_GetData(block1);
	filetype = -1;
	for (i=0;i<buckets;i++){
 		memcpy( data1+i*sizeof(int),&filetype,sizeof(int));

 	}
	BF_Block_SetDirty(block1);
	BF_UnpinBlock(block1);
	BF_Block_Destroy(&block1);
	BF_CloseFile(fd);
  	return HT_OK;
}

HT_ErrorCode HT_OpenIndex(const char *fileName, int *indexDesc){
int fd;
BF_Block *block;
BF_Block_Init(&block);

BF_OpenFile( fileName,&fd);

if(array_files_counter<=19){
	array_files[array_files_counter]=fd;
	*indexDesc=array_files_counter;
	array_files_counter++;
}
else{
	return HT_ERROR;
}


BF_GetBlock(*indexDesc,0,block);
void* data = BF_Block_GetData(block);
int temp;
memcpy(&temp,data,sizeof(int));

 if(temp == 1){
      BF_UnpinBlock(block);
      BF_Block_Destroy(&block);
      return HT_OK;
  }
  else{
      BF_UnpinBlock(block);
      BF_Block_Destroy(&block);
      return HT_ERROR;

  }
  return HT_OK;
}


HT_ErrorCode HT_CloseFile(int indexDesc) {
	int filedesc;
  	filedesc = array_files[indexDesc];
  	array_files[indexDesc]=-1;
  	array_files_counter--;
	BF_CloseFile(filedesc);
  //insert code here*/
  return HT_OK;
}

HT_ErrorCode HT_InsertEntry(int indexDesc, Record record) {
	//int c;
	int buckets;
	BF_Block *block;
 	 int filedesc;
  	filedesc = array_files[indexDesc];
	BF_Block_Init(&block);
	BF_GetBlock(filedesc,0,block);
	void* data =BF_Block_GetData(block);

	memcpy(&buckets,data+sizeof(int),sizeof(int));

	BF_Block_SetDirty(block);
	BF_UnpinBlock(block);
	BF_Block_Destroy(&block);
	int hash_result=HASH_FUNCTION(record.id,buckets);
//block1
	BF_Block *block1;
	BF_Block_Init(&block1);
	BF_GetBlock(filedesc,1,block1);
	void* data1=BF_Block_GetData(block1);
	int temp;
	memcpy(&temp,data1+hash_result*sizeof(int),sizeof(int));

//block2
	BF_Block *block2;
	BF_Block_Init(&block2);
	int blocks_num;
	Block *x;
	x=malloc(sizeof(Block));
	 if (temp == -1) {
		BF_AllocateBlock(filedesc,block2);
		BF_GetBlockCounter(filedesc,&blocks_num);
		blocks_num -- ;
		memcpy(data1+hash_result*sizeof(int),&blocks_num,sizeof(int));
		BF_Block_SetDirty(block1);
		BF_UnpinBlock(block1);
		BF_Block_Destroy(&block1);

		void* data2=BF_Block_GetData(block2);
		memcpy(x, data2, sizeof(Block));
        x->records[x->record_count] = record;
        x->record_count++;
        x->next_block=-2;
	      memcpy(data2,x,sizeof(Block));
	      BF_Block_SetDirty(block2);
          CALL_BF(BF_UnpinBlock(block2));
	      BF_Block_Destroy(&block2);
	      free(x);

   } else {
     BF_Block_SetDirty(block1);
 	 BF_UnpinBlock(block1);
     BF_Block_Destroy(&block1);
     BF_Block_Destroy(&block2);
        int before;
while(temp!=-2){
         	before=temp;
     		BF_Block *block3;
     		BF_Block_Init(&block3);
     		Block *y;
     		y=malloc(sizeof(Block));
     		BF_GetBlock(filedesc,temp,block3);
	 		void* data3=BF_Block_GetData(block3);

	 		memcpy(y,data3,sizeof(Block));
	 		temp=y->next_block;
	 			if(temp==-2){
	 				if(y->record_count<=7){
	 					y->records[y->record_count]=record;
	 					y->record_count++;
	 					memcpy(data3,y,sizeof(Block));
	 					BF_Block_SetDirty(block3);
 						BF_UnpinBlock(block3);
     					BF_Block_Destroy(&block3);
	 				}

	 				else {
						BF_Block *block4;
	 					BF_Block_Init(&block4);
	 					BF_AllocateBlock(filedesc,block4);
	       				BF_GetBlockCounter(filedesc,&blocks_num);



						y->next_block=blocks_num-1;
	 					memcpy(data3,y,sizeof(Block));
	 					BF_Block_SetDirty(block3);
 						BF_UnpinBlock(block3);
     					BF_Block_Destroy(&block3);


     					void * data4=BF_Block_GetData(block4);
     					Block *z;
     					z=malloc(sizeof(Block));
     					memcpy(z,data4,sizeof(Block));
     					z->record_count=0;
     					z->next_block=-2;
     					temp=z->next_block;
     					z->records[z->record_count]=record;
     					z->record_count++;
     					memcpy(data4,z,sizeof(Block));
     					BF_Block_SetDirty(block4);
 						BF_UnpinBlock(block4);
     					BF_Block_Destroy(&block4);
	 				}
				}
		 		else{
					 BF_Block_SetDirty(block3);
 	 				 BF_UnpinBlock(block3);
     				 BF_Block_Destroy(&block3);
    		}
    	}
	}
  //insert code here*/
  return HT_OK;
}

HT_ErrorCode HT_PrintAllEntries(int indexDesc, int *id) {


	int buckets;
	BF_Block *block;
 	int filedesc;
  	filedesc = array_files[indexDesc];
	BF_Block_Init(&block);
	BF_GetBlock(filedesc,0,block);
	void* data =BF_Block_GetData(block);
	memcpy(&buckets,data+sizeof(int),sizeof(int));
	BF_Block_SetDirty(block);
	BF_UnpinBlock(block);
	BF_Block_Destroy(&block);



int temp;
if(id!=NULL){
	int help;
	int hash_result=HASH_FUNCTION(*id,buckets);

	BF_Block *block1;
	BF_Block_Init(&block1);
	BF_GetBlock(filedesc,1,block1);
	void* data1=BF_Block_GetData(block1);
	memcpy(&temp,data1+hash_result*sizeof(int),sizeof(int));
	//printf("o temp einai %d\n",temp);
	BF_UnpinBlock(block1);
	BF_Block_Destroy(&block1);

	BF_Block * block2;
	BF_Block_Init(&block2);
	BF_GetBlock(filedesc,temp,block2);
	void * data2=BF_Block_GetData(block2);
	Block * x;
	x=malloc(sizeof(Block));
	memcpy(x,data2,sizeof(Block));
	x->record_count=0;
	if(x->records[x->record_count].id==*id){
			printf("%d ",x->records[0].id);
			printf("%s ",x->records[0].name);
			printf("%s ",x->records[0].surname);
			printf("%s ",x->records[0].city);
			printf("\n");
			BF_UnpinBlock(block2);
			BF_Block_Destroy(&block2);
			return HT_OK;
	}
	help=temp;
	int i=0;
	int beforehelp;
	while(x->records[x->record_count].id!=*id){
		x->record_count++;
		if(x->records[x->record_count].id==*id){
		printf("%d ",x->records[x->record_count].id);
		printf("%s ",x->records[x->record_count].name);
		printf("%s ",x->records[x->record_count].surname);
		printf("%s ",x->records[x->record_count].city);
		printf("\n");
		BF_UnpinBlock(block2);
		BF_Block_Destroy(&block2);
		return HT_OK;
		}
		if(x->record_count==7){
			beforehelp=help;
			//printf("o before einai %d\n",beforehelp);
			help=x->next_block;
			if(help==-2){
				//printf("ok");
				Block *last;
				last=malloc(sizeof(Block));
				BF_Block *block0;
				BF_Block_Init(&block0);
				BF_GetBlock(filedesc,beforehelp,block0);
				void* data0=BF_Block_GetData(block1);
				memcpy(last,data0,sizeof(Block));
				last->record_count--;
					while(last->record_count!=-1){
						if(last->records[last->record_count].id==*id){
						printf("%d ",last->records[last->record_count].id);
						printf("%s ",last->records[last->record_count].name);
						printf("%s ",last->records[last->record_count].surname);
						printf("%s ",last->records[last->record_count].city);
						printf("\n");
						BF_UnpinBlock(block0);
						BF_Block_Destroy(&block0);
						return HT_OK;
						}
					last->record_count--;

					}
				BF_UnpinBlock(block0);
				BF_Block_Destroy(&block0);
				printf("Entry doesn't exist\n");
				return HT_OK;
			}

			BF_UnpinBlock(block2);
			BF_Block_Destroy(&block2);

			BF_Block * block2;
			BF_Block_Init(&block2);
			BF_GetBlock(filedesc,help,block2);
			void * data3=BF_Block_GetData(block2);
			memcpy(x,data3,sizeof(Block));
			x->record_count=0;
			if(x->records[x->record_count].id==*id){
				printf("%d ",x->records[0].id);
				printf("%s ",x->records[0].name);
				printf("%s ",x->records[0].surname);
				printf("%s ",x->records[0].city);
				printf("\n");
				BF_UnpinBlock(block2);
				BF_Block_Destroy(&block2);
				return HT_OK;
			}
		}
	}


}


else{
	int i;
	BF_Block *block1;
	BF_Block_Init(&block1);
	BF_GetBlock(filedesc,1,block1);
	void* data1=BF_Block_GetData(block1);

	for(i=0;i<buckets;i++){
		memcpy(&temp,data1+i*sizeof(int),sizeof(int));
		Block * x;
		x=malloc(sizeof(Block));
			while(temp!=-2){
				BF_Block * block2;
				BF_Block_Init(&block2);
				BF_GetBlock(filedesc,temp,block2);
				void * data2=BF_Block_GetData(block2);
				memcpy(x,data2,sizeof(Block));
				x->record_count=0;
				if(x->records[x->record_count].id==0){
						printf("%d ",x->records[x->record_count].id);
						printf("%s ",x->records[x->record_count].name);
						printf("%s ",x->records[x->record_count].surname);
						printf("%s ",x->records[x->record_count].city);
						printf("\n");
					x->record_count++;
				}
					while(x->record_count<=7){
						if(x->records[x->record_count].id!=0){
						printf("%d ",x->records[x->record_count].id);
						printf("%s ",x->records[x->record_count].name);
						printf("%s ",x->records[x->record_count].surname);
						printf("%s ",x->records[x->record_count].city);
						printf("\n");
					}
						x->record_count++;
					}
					temp=x->next_block;
					BF_UnpinBlock(block2);
					BF_Block_Destroy(&block2);
			}
			free(x);
	}
	BF_UnpinBlock(block1);
	BF_Block_Destroy(&block1);
}

  //insert code here
  return HT_OK;
}

HT_ErrorCode HT_DeleteEntry(int indexDesc, int id) {
  int buckets;
	BF_Block *block;
 	int filedesc;
  filedesc = array_files[indexDesc];
  	BF_Block_Init(&block);
  	BF_GetBlock(filedesc,0,block);
	void* data =BF_Block_GetData(block);
	memcpy(&buckets,data+sizeof(int),sizeof(int));
  	BF_Block_SetDirty(block);
  	BF_UnpinBlock(block);
  	BF_Block_Destroy(&block);

  int temp;
	int hash_result=HASH_FUNCTION(id,buckets);
  	BF_Block *block1;
  	BF_Block_Init(&block1);
  	BF_GetBlock(filedesc,1,block1);
	void* data1=BF_Block_GetData(block1);
	memcpy(&temp,data1+hash_result*sizeof(int),sizeof(int));
  	BF_UnpinBlock(block1);
  	BF_Block_Destroy(&block1);


int helpcount;
int map=temp;
int help;
int p_temp;
Block* y;
y=malloc(sizeof(Block));
Record *metavatiko;
metavatiko=malloc(sizeof(Record));

    while(temp!=-2){
      BF_Block *block3;
      BF_Block_Init(&block3);
      BF_GetBlock(filedesc,temp,block3);
      void * data3=BF_Block_GetData(block3);
      memcpy(y,data3,sizeof(Block));
      y->record_count=0;
      while(y->record_count<8 && y->records[y->record_count].id!=0) {
        helpcount=y->record_count;
        y->record_count++;
      }
      p_temp=temp;
      temp=y->next_block;
      BF_UnpinBlock(block3);
      BF_Block_Destroy(&block3);
    }

     BF_Block *block3;
     BF_Block_Init(&block3);
     BF_GetBlock(filedesc,p_temp,block3);
     void * datadata=BF_Block_GetData(block3);
     memcpy(y,datadata,sizeof(Block));
     metavatiko=CopyRecords(metavatiko,&(y->records[helpcount]));
     y->records[helpcount].id=0;
     memcpy(datadata,y,sizeof(Block));
     BF_Block_SetDirty(block3);
     BF_UnpinBlock(block3);
     BF_Block_Destroy(&block3);

    BF_Block * block2;
  	BF_Block_Init(&block2);
  	BF_GetBlock(filedesc,map,block2);

	void * data2=BF_Block_GetData(block2);
	Block * x;
	x=malloc(sizeof(Block));
	memcpy(x,data2,sizeof(Block));
	x->record_count=0;
	int plus;
  	while(x->records[x->record_count].id!=id){
  			x->record_count++;
  		if(x->records[x->record_count].id==id){
      		plus=x->record_count;
        	 x->records[plus].id=metavatiko->id;
      		 strcpy((x->records[plus].name),(metavatiko->name));
      		 strcpy((x->records[plus].surname),(metavatiko->surname));
      	     strcpy((x->records[plus].city),(metavatiko->city));
        	 memcpy(data2,x,sizeof(Block));
        	 BF_Block_SetDirty(block2);
      		 BF_UnpinBlock(block2);
      		 BF_Block_Destroy(&block2);
  		      return HT_OK;
  	    }
  		if(x->record_count==7){
  			help=x->next_block;
  			BF_UnpinBlock(block2);
  			BF_Block_Destroy(&block2);
  			BF_Block * block2;
  			BF_Block_Init(&block2);
  			BF_GetBlock(filedesc,help,block2);
  			data2=BF_Block_GetData(block2);
  			memcpy(x,data2,sizeof(Block));
  			x->record_count=0;
  			if(x->records[x->record_count].id==id){
            	x->records[plus].id=metavatiko->id;
      		 	strcpy((x->records[plus].name),(metavatiko->name));
      		 	strcpy((x->records[plus].surname),(metavatiko->surname));
      	     	strcpy((x->records[plus].city),(metavatiko->city));
            	memcpy(data2,x,sizeof(Block));
            	BF_Block_SetDirty(block2);
  				BF_UnpinBlock(block2);
  				BF_Block_Destroy(&block2);
  				return HT_OK;
  			}
  		}
  	}
  //insert code here
  return HT_OK;
}

int HASH_FUNCTION(int id,int buckets){
	int hash;
	hash=id%buckets;
	return hash;
}

Record * CopyRecords(Record* source, Record* dest){

	source->id = dest->id;
	strcpy(source->name,dest->name);
	strcpy(source->surname,dest->surname);
	strcpy(source->city,dest->city);
	return source;
}
