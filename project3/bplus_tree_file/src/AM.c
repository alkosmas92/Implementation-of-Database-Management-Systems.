#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "AM.h"
#include "bf.h"
#include "defn.h"
#define MAX_OPEN_FILES 20

int AM_errno = AME_OK;
int number_of_records=0;
int Open_file[MAX_OPEN_FILES][5];
int sum(int *, int);


void AM_Init() {
	BF_PrintError(BF_Init(LRU));
	for(int i=0 ; i<=4 ; i++){
	Open_file[i][0]=-1;
	}
	return;
}


int AM_CreateIndex(char *fileName,
           char attrType1,
           int attrLength1,
           char attrType2,
           int attrLength2) {


						int file_desc;
					 	BF_OpenFile(fileName, &file_desc);
						BF_Block *block;
						BF_Block_Init(&block);

						BF_AllocateBlock(file_desc, block);

						char* data = BF_Block_GetData(block);

					//	 memset	(data, 'B',sizeof(char));  // file is Hash file
						number_of_records= 512 / (attrLength1+attrLength2);

						 memcpy	(data  , &attrType1 , sizeof(char));
						 memcpy (data  + sizeof(char) ,&attrLength1 , sizeof(int));
						 memcpy (data  + sizeof(char) + sizeof(int) , &attrType2 , sizeof(char));
						 memcpy (data  + sizeof(char) + sizeof(int) + sizeof(char) , &attrLength2, sizeof(int));
						 memcpy (data  + sizeof(char) + sizeof(int) + sizeof(char)+ sizeof(int),&number_of_records, sizeof(int));


						BF_Block_SetDirty(block);

						BF_UnpinBlock(block);

						BF_Block_Destroy(&block);

						BF_CloseFile(file_desc);

		 return AME_OK;
}


int AM_DestroyIndex(char *fileName) {
	int error=1;
	int test = remove(fileName);

	if (test==1){
		return AM_errno=AME_OK;
	}
	else{
			return AM_errno=error;
		}
}


int AM_OpenIndex (char *fileName) {
	int file_desc;
	int i=0;
	while(Open_file[i][0]!=-1){
		i++;
		}
		BF_OpenFile(fileName,&file_desc);

		Open_file[i][0]=file_desc;
		BF_Block *block;
		BF_Block_Init(&block);
		BF_GetBlock(file_desc,0,block);
		void* data = BF_Block_GetData(block);
		char temp;
		int int_temp;
		memcpy(&temp,data,sizeof(char));
		Open_file[i][1]=temp;
		memcpy(&int_temp,data+sizeof(char),sizeof(int));
		Open_file[i][2]=int_temp;
		memcpy(&temp,data+sizeof(int)+sizeof(char),sizeof(char));
		Open_file[i][3]=temp;
		memcpy(&int_temp,data+sizeof(int)+2*sizeof(char),sizeof(int));
		Open_file[i][4]=int_temp;

//		printf("file_desc is: %d , attrType1 is : %c,attrLength1 is : %d , attrType2 is: %c , attrLength2 is: %d \n",Open_file[i][0],Open_file[i][1],Open_file[i][2],Open_file[i][3],Open_file[i][4] );
		BF_UnpinBlock(block);
		BF_Block_Destroy(&block);

	return file_desc;
}


int AM_CloseIndex (int indexDesc) {

	int  fileDesc=Open_file[indexDesc][0];
	Open_file[indexDesc][0]=-1;
	BF_CloseFile(fileDesc);
  return AME_OK;
}


int AM_InsertEntry(int indexDesc, void *value1, void *value2) {
	//printf("index :%d\n",indexDesc );
	int fileDesc=Open_file[indexDesc][0];
	int temp,blocks_num,number_of_records,attrLength1,attrLength2;
	int pointer_right;
	int pointer_before;
	char attrType1,attrType2;


	BF_Block *block;
	BF_Block_Init(&block);
	BF_GetBlock(fileDesc,0,block);
	void* data =BF_Block_GetData(block);

	memcpy (&attrType1 , data  ,  sizeof(char));
	memcpy (&attrLength1 , data  + sizeof(char) , sizeof(int));
	memcpy (&attrType2  , data  + sizeof(char) + sizeof(int) ,  sizeof(char));
	memcpy (&attrLength2 ,data  + sizeof(char) + sizeof(int) + sizeof(char) ,  sizeof(int));
	memcpy (&number_of_records , data  + sizeof(char) + sizeof(int) + sizeof(char)+ sizeof(int), sizeof(int));

	BF_Block_SetDirty(block);
	BF_UnpinBlock(block);
	BF_Block_Destroy(&block);

	 typedef struct{
	 	int va,vb;
	 	float vaa,vbb;
		char v1[attrLength1];
		char v2[attrLength2];
	 }Record;
 	Record record;
 	char integer='i';
 	char floater='f';
 	char string='c';

 	if(attrType1==string){
	 		strcpy(record.v1,value1);
	 		//printf("%s\n",record.v1);
	 	}
	 	else if(attrType1==integer){
	 		record.va = *(int*)value1;
	 	}
	 	else if(attrType1==floater){
	 		record.vaa = *(float*)value1;
	 	}

	 	if(attrType2==string){
	 		strcpy(record.v1,value2);
	 		//printf("%s\n",record.v2);
	 	}
	 	else if(attrType2==integer){
	 		record.vb = *(int*)value2;
	 	}
	 	else if(attrType2==floater){
	 		record.vbb = *(float*)value2;
 	}

	typedef struct {
		 Record records[7];
		 int counter;
		 int pointer[8];
		 int before;
	 }Block;

	BF_GetBlockCounter(fileDesc, &blocks_num);
	if(--blocks_num==0){
		Block *x = malloc(sizeof(Block));
		BF_Block *block;
		BF_Block_Init(&block);
		BF_AllocateBlock(fileDesc,block);
		BF_GetBlock(fileDesc,1,block);
		void* data = BF_Block_GetData(block);
		memcpy(x,data,sizeof(Block));
		x->records[x->counter]=record;
		x->counter++;
		memcpy(data,x,sizeof(Block));
		BF_Block_SetDirty(block);
		BF_UnpinBlock(block);
		BF_Block_Destroy(&block);
	}

	BF_GetBlockCounter(fileDesc, &blocks_num);
	blocks_num--;
	Block *x = malloc(sizeof(Block));
	BF_Block *block1;
	BF_Block_Init(&block1);
	BF_GetBlock(fileDesc,blocks_num,block1);

	void* data1 = BF_Block_GetData(block1);
	memcpy(x,data1,sizeof(Block));


	if(x->counter<7 && blocks_num==1){
			x->records[x->counter]=record;
			x->counter++;
	int	count=x->counter;
				count--;
				Record rec;
				if(attrType1==string ){
					for(int k=0; k<count; k++){
						for(int i=0; i<count; i++){
							if(strcmp(x->records[i].v1,x->records[i+1].v1)>0){
								strcpy(rec.v1,x->records[i+1].v1);
								strcpy(x->records[i+1].v1,x->records[i].v1);
								strcpy(x->records[i].v1,rec.v1);
								if(attrType2==string){
									strcpy(rec.v2,x->records[i+1].v2);
									strcpy(x->records[i+1].v2,x->records[i].v2);
									strcpy(x->records[i].v2,rec.v2);
								}
								else if(attrType2==integer){
									int d = x->records[i+1].vb;
									x->records[i+1].vb = x->records[i].vb;
									x->records[i].vb = d;
								}
								else if(attrType1==floater){
									float j = x->records[i+1].vbb;
									x->records[i+1].vbb = x->records[i].vbb;
									x->records[i].vbb = j;
								}
								int pinakas;
								pinakas=x->pointer[i+1];
								x->pointer[i+1]=x->pointer[i];
								x->pointer[i]=pinakas;
							}
						}
					}
				}
				else if(attrType1==integer){
					for(int k=0; k<count; k++){
						for(int i=0; i<count; i++){
							if(x->records[i].va>x->records[i+1].va){
									int d = x->records[i+1].va;
									x->records[i+1].va = x->records[i].va;
									x->records[i].va = d;
								if(attrType2==string){
									strcpy(rec.v2,x->records[i+1].v2);
									strcpy(x->records[i+1].v2,x->records[i].v2);
									strcpy(x->records[i].v2,rec.v2);
								}
								else if(attrType2==integer){
									int dd= x->records[i+1].vb;
									x->records[i+1].vb = x->records[i].vb;
									x->records[i].vb = dd;
								}
								else if(attrType1==floater){
									float j = x->records[i+1].vbb;
									x->records[i+1].vbb = x->records[i].vbb;
									x->records[i].vbb = j;

								}
								int pinakas;
								pinakas=x->pointer[i+1];
								x->pointer[i+1]=x->pointer[i];
								x->pointer[i]=pinakas;
							}
						}

					}
				}
				else{
					for(int k=0; k<count; k++){
						for(int i=0; i<count; i++){
							if(x->records[i].vaa>x->records[i+1].vaa){
								float j = x->records[i+1].vaa;
									x->records[i+1].vaa = x->records[i].vaa;
									x->records[i].vaa = j;
								if(attrType2==string){
									strcpy(rec.v2,x->records[i+1].v2);
									strcpy(x->records[i+1].v2,x->records[i].v2);
									strcpy(x->records[i].v2,rec.v2);
								}
								else if(attrType2==integer){
									int d = x->records[i+1].vb;
									x->records[i+1].vb = x->records[i].vb;
									x->records[i].vb = d;
								}
								else if(attrType1==floater){
									float jj = x->records[i+1].vbb;
									x->records[i+1].vbb = x->records[i].vbb;
									x->records[i].vbb = jj;
								}



								int pinakas;
								pinakas=x->pointer[i+1];
								x->pointer[i+1]=x->pointer[i];
								x->pointer[i]=pinakas;
							}
						}
					}
				}
			if(x->counter==7 && x->before==0 ){
						x->counter++;
							Block *left=malloc(sizeof(Block));
							Block *right=malloc(sizeof(Block));
							Block *zero=malloc(sizeof(Block));

							memcpy(left,x,sizeof(Block));
							left->before=blocks_num;
							memcpy(right,x,sizeof(Block));
							 right->before=blocks_num;

									memcpy(&x->records[0],&x->records[3],sizeof(Record));
									x->counter=1;
									for(int k=0;k<=6;k++){
										if(k!=0){
											memcpy(&x->records[k],&zero->records[0],sizeof(Record));
										}
									}

							BF_Block *block2;
							BF_Block_Init(&block2);
							BF_AllocateBlock(fileDesc,block2);
							BF_GetBlockCounter(fileDesc, &blocks_num);
							blocks_num--;

							BF_GetBlock(fileDesc,blocks_num,block2);

								x->pointer[0]=blocks_num; //deiktis aristerou

							void* data2 = BF_Block_GetData(block2);

							BF_Block *block3;
							BF_Block_Init(&block3);
							BF_AllocateBlock(fileDesc,block3);
							BF_GetBlockCounter(fileDesc, &blocks_num);
							blocks_num--;
							BF_GetBlock(fileDesc,blocks_num,block3);
									x->pointer[1]=blocks_num;	//diktis deksiou paidiou
							void* data3 = BF_Block_GetData(block3);

						for(int t=0 ; t<=6 ; t++){
								 if(t>=3){
										memcpy(&left->records[t],&zero->records[t],sizeof(Record));
								}
						}
						left->counter=3;
									memcpy(&right->records[0],&right->records[4],sizeof(Record));
									memcpy(&right->records[1],&right->records[5],sizeof(Record));
									memcpy(&right->records[2],&right->records[6],sizeof(Record));
						right->counter=3;

						for(int t=0 ; t<=6 ; t++){
								if(t>=3){
										memcpy(&right->records[t],&zero->records[t],sizeof(Record));
								}
			}

				memcpy(data3,right,sizeof(Block));

				BF_Block_SetDirty(block3);
				BF_UnpinBlock(block3);
				BF_Block_Destroy(&block3);

				memcpy(data2,left,sizeof(Block));

				BF_Block_SetDirty(block2);
				BF_UnpinBlock(block2);
				BF_Block_Destroy(&block2);
			}

	}
			memcpy(data1,x,sizeof(Block));
			BF_Block_SetDirty(block1);
			BF_UnpinBlock(block1);
			BF_Block_Destroy(&block1);

			Block *y = malloc(sizeof(Block));
			BF_Block *block4;
			BF_Block_Init(&block4);
			BF_GetBlock(fileDesc,1,block4);
			void* data4 = BF_Block_GetData(block4);
			memcpy(y,data4,sizeof(Block));
			int test=sum(y->pointer,8);
			int u;
			u=y->counter;
			int num_block;
		while(test != 0){
			 if(attrType1==string){
				for(int i=0; i<u; i++){
					if(strcmp(record.v1,y->records[i].v1)<=0){
						//printf("o counter %d\n",u);
							num_block=y->pointer[i];

						Block *d = malloc(sizeof(Block));
						BF_Block *block5;
						BF_Block_Init(&block5);
						BF_GetBlock(fileDesc,num_block,block5);
						void* data5 = BF_Block_GetData(block5);
						memcpy(d,data5,sizeof(Block));
						test=sum(d->pointer,8);
						memcpy(y,d,sizeof(Block));
						u=y->counter;
						BF_UnpinBlock(block5);
						BF_Block_Destroy(&block5);
						break;
					}
					if(i==u-1 && strcmp(record.v1,y->records[i].v1)>0){

						num_block=y->pointer[i+1];
						Block *d = malloc(sizeof(Block));
						BF_Block *block5;
						BF_Block_Init(&block5);
						BF_GetBlock(fileDesc,num_block,block5);
						void* data5 = BF_Block_GetData(block5);
						memcpy(d,data5,sizeof(Block));
						test=sum(d->pointer,8);
						memcpy(y,d,sizeof(Block));
						BF_UnpinBlock(block5);
						BF_Block_Destroy(&block5);
						break;
					}
				}
			}
			else if(attrType1==integer){
				for(int i=0; i<u; i++){
					if(record.va<=y->records[i].va){
						num_block=y->pointer[i];
						Block *d = malloc(sizeof(Block));
						BF_Block *block5;
						BF_Block_Init(&block5);
						BF_GetBlock(fileDesc,num_block,block5);
						void* data5 = BF_Block_GetData(block5);
						memcpy(d,data5,sizeof(Block));
						test=sum(d->pointer,8);
						memcpy(y,d,sizeof(Block));
						u=y->counter;
						BF_UnpinBlock(block5);
						BF_Block_Destroy(&block5);
						break;
					}
					if(i==u-1 && record.va>y->records[i].va){
						num_block=y->pointer[i+1];
						Block *d = malloc(sizeof(Block));
						BF_Block *block5;
						BF_Block_Init(&block5);
						BF_GetBlock(fileDesc,num_block,block5);
						void* data5 = BF_Block_GetData(block5);
						memcpy(d,data5,sizeof(Block));
						test=sum(d->pointer,8);
						memcpy(y,d,sizeof(Block));
						BF_UnpinBlock(block5);
						BF_Block_Destroy(&block5);
						break;
					}
				}
			}
			else{
				for(int i=0; i<u; i++){
					if(record.vaa <= y->records[i].vaa){
						num_block=y->pointer[i];
						Block *d = malloc(sizeof(Block));
						BF_Block *block5;
						BF_Block_Init(&block5);
						BF_GetBlock(fileDesc,num_block,block5);
						void* data5 = BF_Block_GetData(block5);
						memcpy(d,data5,sizeof(Block));
						test=sum(d->pointer,8);
						memcpy(y,d,sizeof(Block));
						u=y->counter;
						BF_UnpinBlock(block5);
						BF_Block_Destroy(&block5);
						break;
					}
					if(i==u-1 && record.vaa > y->records[i].vaa){
						num_block=y->pointer[i+1];
						Block *d = malloc(sizeof(Block));
						BF_Block *block5;
						BF_Block_Init(&block5);
						BF_GetBlock(fileDesc,num_block,block5);
						void* data5 = BF_Block_GetData(block5);
						memcpy(d,data5,sizeof(Block));
						test=sum(d->pointer,8);
						memcpy(y,d,sizeof(Block));
						BF_UnpinBlock(block5);
						BF_Block_Destroy(&block5);
						break;
					}
				}
			}
		}

		int bike;
		bike=1;
	while(bike==1){
				if(y->counter<7){
					bike=0;
					y->records[y->counter]=record;
					y->counter++;
						if(sum(y->pointer,8)!=0){
							y->pointer[y->counter]=pointer_right;
							}
							int	count=y->counter;
								count--;
								Record rec;
								if(attrType1==string ){
									for(int k=0; k<count; k++){
										for(int i=0; i<count; i++){
											if(strcmp(y->records[i].v1,y->records[i+1].v1)>0){
												strcpy(rec.v1,y->records[i+1].v1);
												strcpy(y->records[i+1].v1,y->records[i].v1);
												strcpy(y->records[i].v1,rec.v1);
												if(attrType2==string){
													strcpy(rec.v2,y->records[i+1].v2);
													strcpy(y->records[i+1].v2,y->records[i].v2);
													strcpy(y->records[i].v2,rec.v2);
												}
												else if(attrType2==integer){
													int d = y->records[i+1].vb;
													y->records[i+1].vb = y->records[i].vb;
													y->records[i].vb = d;
												}
												else if(attrType1==floater){
													float j = y->records[i+1].vbb;
													y->records[i+1].vbb = y->records[i].vbb;
													y->records[i].vbb = j;
												}
												int pinakas;
												pinakas=y->pointer[i+1];
												y->pointer[i+1]=y->pointer[i];
												y->pointer[i]=pinakas;
											}
										}
									}
								}
								else if(attrType1==integer){
									for(int k=0; k<count; k++){
										for(int i=0; i<count; i++){
											if(y->records[i].va>y->records[i+1].va){
													int d = y->records[i+1].va;
													y->records[i+1].va = y->records[i].va;
													y->records[i].va = d;
												if(attrType2==string){
													strcpy(rec.v2,y->records[i+1].v2);
													strcpy(y->records[i+1].v2,y->records[i].v2);
													strcpy(y->records[i].v2,rec.v2);
												}
												else if(attrType2==integer){
													int dd= y->records[i+1].vb;
													y->records[i+1].vb = y->records[i].vb;
													y->records[i].vb = dd;
												}
												else if(attrType1==floater){
													float j = y->records[i+1].vbb;
													y->records[i+1].vbb = y->records[i].vbb;
													y->records[i].vbb = j;

												}
												int pinakas;
												pinakas=y->pointer[i+1];
												y->pointer[i+1]=y->pointer[i];
												y->pointer[i]=pinakas;
											}
										}

									}


								}
								else{
									for(int k=0; k<count; k++){
										for(int i=0; i<count; i++){
											if(y->records[i].vaa>y->records[i+1].vaa){
												float j = y->records[i+1].vaa;
													y->records[i+1].vaa = y->records[i].vaa;
													y->records[i].vaa = j;
												if(attrType2==string){
													strcpy(rec.v2,y->records[i+1].v2);
													strcpy(y->records[i+1].v2,y->records[i].v2);
													strcpy(y->records[i].v2,rec.v2);
												}
												else if(attrType2==integer){
													int d = y->records[i+1].vb;
													y->records[i+1].vb = y->records[i].vb;
													y->records[i].vb = d;
												}
												else if(attrType1==floater){
													float jj = y->records[i+1].vbb;
													y->records[i+1].vbb = y->records[i].vbb;
													y->records[i].vbb = jj;
												}
												int pinakas;
												pinakas=y->pointer[i+1];
												y->pointer[i+1]=y->pointer[i];
												y->pointer[i]=pinakas;
											}
										}
									}
					}

							if(y->counter==7){
								bike=1;
								printf("okkkk\n" );
								int check=0;
								check++;
								printf("check is:%d\n",check);
								memcpy(&record,&y->records[3],sizeof(Record));
								Block *right=malloc(sizeof(Block));
								Block *zero=malloc(sizeof(Block));

								BF_Block *block_r;
								BF_Block_Init(&block_r);
								BF_AllocateBlock(fileDesc,block_r);
								BF_GetBlockCounter(fileDesc, &blocks_num);
						 		 pointer_right=blocks_num-1;
								 pointer_before=y->before;
								BF_GetBlock(fileDesc,blocks_num,block_r);
								void* data_r = BF_Block_GetData(block_r);
											memcpy(&right->records[0],&y->records[4],sizeof(Record));
											memcpy(&right->records[1],&y->records[5],sizeof(Record));
											memcpy(&right->records[2],&y->records[6],sizeof(Record));
								right->counter = 3;
								right->before = pointer_before;

								memcpy(data_r,right,sizeof(Block));
								BF_Block_SetDirty(block_r);
								BF_UnpinBlock(block_r);
								BF_Block_Destroy(&block_r);

								for(int t=0 ; t<=6 ; t++){
										if(t>=3){
												memcpy(&y->records[t],&zero->records[t],sizeof(Record));
										}
									}
									y->counter=3;
									y->before = pointer_before;
							}

					BF_Block_SetDirty(block4);
					printf("y->before : %d\n",y->before );
					BF_UnpinBlock(block4);
					BF_Block_Destroy(&block4);

					if(bike==1){
						Block *y = malloc(sizeof(Block));
						BF_Block *block4;
						BF_Block_Init(&block4);
						BF_GetBlock(fileDesc,pointer_before,block4);
						void* data4 = BF_Block_GetData(block4);
						memcpy(y,data4,sizeof(Block));
					}
				}
		}
		return AME_OK;

}


int AM_OpenIndexScan(int fileDesc, int op, void *value) {
  return AME_OK;
}


void *AM_FindNextEntry(int scanDesc) {

}


int AM_CloseIndexScan(int scanDesc) {
  return AME_OK;
}


void AM_PrintError(char *errString) {

}

void AM_Close() {

}

int sum(int arr[], int n)
{
    int sum = 0;
    for (int i = 0; i < n; i++){
    sum += arr[i];
	}
    return sum;
}
