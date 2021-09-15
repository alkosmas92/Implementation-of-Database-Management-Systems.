#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "heap_file.h"


#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}

HP_ErrorCode HP_Init() {

  return HP_OK;
}





HP_ErrorCode HP_CreateFile(const char *filename) {

    BF_Block *block;
    int fileDesc;

    BF_Block_Init(&block);
  CALL_BF(BF_CreateFile(filename));

  CALL_BF(BF_OpenFile(filename,&fileDesc));

    HP_info* info = malloc(sizeof(HP_info));
    info->filetype=5;

  CALL_BF(BF_AllocateBlock(fileDesc,block));


    void* data =BF_Block_GetData(block);
    memset( data,*(int *)info,sizeof(HP_info));           //Copies data into the first n characters of the block of data pointed by str

  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));

  free(info);
  BF_Block_Destroy(&block);
  CALL_BF(BF_CloseFile(fileDesc));

  return HP_OK;
}

HP_ErrorCode HP_OpenFile(const char *fileName, int *fileDesc){
BF_Block *block;
BF_Block_Init(&block);
HP_info* info;

CALL_BF (BF_OpenFile( fileName,fileDesc));
//BF_ErrorCode BF_GetBlockCounter(fileDesc,blocks_num);
CALL_BF(BF_GetBlock(*fileDesc,0,block));
void* data = BF_Block_GetData(block);
 if(*(int *)data == 5){
      CALL_BF(BF_UnpinBlock(block));
      BF_Block_Destroy(&block);
      return HP_OK;
  }
  else{
      CALL_BF(BF_UnpinBlock(block));
      BF_Block_Destroy(&block);
      return HP_ERROR;

  }
}
HP_ErrorCode HP_CloseFile(int fileDesc) {
  CALL_BF(BF_CloseFile(fileDesc));
  return HP_OK;
}

HP_ErrorCode HP_InsertEntry(int fileDesc,Record record) {
int blocks_num,n;
  long* count;
  BF_Block *block;
  BF_Block_Init(&block);

  Block *x=malloc(sizeof(Block));

  CALL_BF(BF_GetBlockCounter(fileDesc,&blocks_num));
 //
  blocks_num--;

  if(blocks_num==0){
    CALL_BF(BF_AllocateBlock(fileDesc,block));
    CALL_BF(BF_UnpinBlock(block));
     //x->count=0;
  }
  CALL_BF(BF_GetBlockCounter(fileDesc,&blocks_num));
  blocks_num--;

  CALL_BF(BF_GetBlock(fileDesc,blocks_num,block));
  printf("blocks_num = %d\n", blocks_num);
  printf("filedesc:%d\n",fileDesc );

   void* data=BF_Block_GetData(block);
   memcpy(x, data, sizeof(Block));



   if(x->count < 7) {

      x->count++;
      x->records[x->count] = record;
   //printf("x->count = %d\n", x->count);
   //printf("%d \n",x->records[x->count].id );
   //printf("%s \n",x->records[x->count].name );


      memcpy(data,x,sizeof(Block));
      BF_Block_SetDirty(block);
      CALL_BF(BF_UnpinBlock(block));
      BF_Block_Destroy(&block);
    }

   else {
    CALL_BF(BF_UnpinBlock(block));
    CALL_BF(BF_AllocateBlock(fileDesc,block));

    void* newdata =BF_Block_GetData(block);
    Block *newx=malloc(sizeof(Block));

    memcpy(newx, newdata, sizeof(Block));
    newx->count = 1;
    newx->records[1] = record;
    memcpy(newdata, newx, sizeof(Block));
     BF_Block_SetDirty(block);
      CALL_BF(BF_UnpinBlock(block));
      BF_Block_Destroy(&block);
      //free(newx);


   }
   //free(x);

}

HP_ErrorCode HP_PrintAllEntries(int fileDesc ,char * attrName,void * value){
/*
  int i=0;
  int count=1;
  int blocks_num,n;
  BF_Block *block;

  BF_Block_Init(&block);
  CALL_BF(BF_GetBlockCounter(fileDesc,&blocks_num));

  for(count ; count < blocks_num ; count++ ) {
   // printf("%d\n",count );
     CALL_BF(BF_GetBlock(fileDesc,count,block));
        void* data=BF_Block_GetData(block);
        Block *x =malloc(sizeof(Block));
         memcpy(x, data, sizeof(Block));
        for(i=0;i<=7;i++){
          if(attrName==NULL){
            printf("%d,\"%s\",\"%s\",\"%s\"\n",x->records[i].id,x->records[i].name, x->records[i].surname,x->records[i].city);
          }else if(!(strcmp(attrName,"name"))){
             if(!(strcmp(value,x->records[i].name)))  printf("%d,\"%s\",\"%s\",\"%s\"\n",x->records[i].id,x->records[i].name, x->records[i].surname,x->records[i].city);
          }else if(!(strcmp(attrName,"surname"))){
            if(!(strcmp(value,x->records[i].surname)))  printf("%d,\"%s\",\"%s\",\"%s\"\n",x->records[i].id,x->records[i].name, x->records[i].surname,x->records[i].city);
          }else if(!(strcmp(attrName,"city"))){
            if(!(strcmp(value,x->records[i].city)))  printf("%d,\"%s\",\"%s\",\"%s\"\n",x->records[i].id,x->records[i].name, x->records[i].surname,x->records[i].city);
          }

    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
    //printf("o blocknumber einai %d\n",blocks_num);
    //printf("o count einai %d\n",count)
    }
  }*/
    //free(x);
  return HP_OK;

}

HP_ErrorCode HP_GetEntry(int fileDesc, int rowId, Record *record) {
/*
  int blocks_num,record_num;
  BF_Block *block;
  Block *x =malloc(sizeof(Block));
  BF_Block_Init(&block);
  blocks_num=rowId/7;
  record_num=rowId%7;

  if(record_num==0){
    record_num=record_num+7;
  }else{
    blocks_num++;
  }

   CALL_BF(BF_GetBlock(fileDesc,blocks_num,block));
    void* data=BF_Block_GetData(block);

    memcpy(x, data, sizeof(Record));

    memcpy(&record, x->records[record_num], sizeof(Record));

  // printf("%s\n",record->city );

 CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);
 // printf("%d\n",blocks_num );

*/

  return HP_OK;
}
