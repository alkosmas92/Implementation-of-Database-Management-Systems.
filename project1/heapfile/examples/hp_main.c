#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "heap_file.h"

#define RECORDS_NUM 1700

const char* names[] = {
  "Yannis",
  "Christofos",
  "Sofia",
  "Marianna",
  "Vagelis",
  "Maria",
  "Iosif",
  "Dionisis",
  "Konstantina",
  "Theofilos",
  "Giorgos",
  "Dimitris"
};

const char* surnames[] = {
  "Ioannidis",
  "Svingos",
  "Karvounari",
  "Rezkalla",
  "Nikolopoulos",
  "Berreta",
  "Koronis",
  "Gaitanis",
  "Oikonomou",
  "Mailis",
  "Michas",
  "Halatsis"
};

const char* cities[] = {
  "Athens",
  "San Francisco",
  "Los Angeles",
  "Amsterdam",
  "London",
  "New York",
  "Tokyo",
  "Hong Kong",
  "Munich",
  "Miami"
};

#define CALL_OR_DIE(call)     \
  {                           \
    HP_ErrorCode code = call; \
    if (code != HP_OK) {      \
      printf("Error\n");      \
      exit(code);             \
    }                         \
  }

#define RUN_AND_TIME(call)                                      \
  {                                                             \
    clock_t start = clock();                                    \
    call;                                                       \
    clock_t end = clock();                                      \
    printf("CPU Time: %lf\n", (end - start) / (double)CLOCKS_PER_SEC);  \
  }

void TestFileScan(int fileDesc) {
  Record record;
  // printf("id,name,surname,city\n");
  for (int j = 1; j <= 500; ++j) {
    for (int id = RECORDS_NUM; id != 0; --id) {
      CALL_OR_DIE(HP_GetEntry(fileDesc, id, &record));
      // printf("%d,\"%s\",\"%s\",\"%s\"\n",
      //        record.id, record.name, record.surname, record.city);
    }
  }
}

int main() {
  
  BF_Init(LRU);
  //BF_Init(MRU);

  CALL_OR_DIE(HP_Init());

  int fd;
  CALL_OR_DIE(HP_CreateFile("data.txt"));
  CALL_OR_DIE(HP_OpenFile("data.txt", &fd));

  Record record;
  srand(12569874);
  int r;
  printf("Insert Entries\n");
  for (int id = 0; id < RECORDS_NUM; ++id) {
    record.id = id;
    r = rand() % 12;
    memcpy(record.name, names[r], strlen(names[r]) + 1);
    r = rand() % 12;
    memcpy(record.surname, surnames[r], strlen(surnames[r]) + 1);
    r = rand() % 10;
    memcpy(record.city, cities[r], strlen(cities[r]) + 1);

    CALL_OR_DIE(HP_InsertEntry(fd, record));
  }

  printf("RUN TestFileScan\n");
  RUN_AND_TIME(TestFileScan(fd));

  printf("RUN PrintAllEntries\n");
  CALL_OR_DIE(HP_PrintAllEntries(fd,"city", (void *)"San Francisco"));

  printf("Get Entry with rowid 1000\n");
  CALL_OR_DIE(HP_GetEntry(fd, 1000, &record));
  printf("%d,\"%s\",\"%s\",\"%s\"\n",
      record.id, record.name, record.surname, record.city);

  CALL_OR_DIE(HP_CloseFile(fd));
  BF_Close();
}

