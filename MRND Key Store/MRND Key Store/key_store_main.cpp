#include<stdio.h>
#include<string.h>
#include<stdlib.h>

const int MAX_TABLES_IN_DB = 100;
const int MAX_ROWS_IN_DB = 1000;
const int MAX_COLS_IN_DB = 100;
const int MAX_SIZE_OF_DATA = 100;


typedef struct columnlist{
	char *value;
	struct columnlist *prev;
	int version;
};

typedef struct rowlist{
	char *id;
	int current;
	int commit;
	struct columnlist *col;
};

typedef struct table{
	int current_rows_count;
	int current_cols_count;
	char *tablename;
	char **colnames;
	struct rowlist *rows;
};

typedef struct tables{
	int current_tables_count;
	struct table *t;
};


void storetodisk(tables *ts){
	int i = 0, j = 0, k = 0;
	FILE *fp;
	char filename[30], colname[30];


	fp = fopen("db.txt", "w");
	fprintf(fp, "%d\n", ts->current_tables_count);
	
	for (i = 0; i < ts->current_tables_count; i++){
		fprintf(fp, "%s\n", ts->t[i].tablename);
	}
	fclose(fp);

	for (i = 0; i < ts->current_tables_count; i++){
		strcpy(filename, ts->t[i].tablename);
		strcat(filename, ".txt");
		fp = fopen(filename, "w");
		fprintf(fp, "%d\n", ts->t[i].current_rows_count);
		fprintf(fp, "%d\n", ts->t[i].current_cols_count);
		for (j = 0; j < ts->t[i].current_cols_count - 1; j++){
			fprintf(fp, "%s\n", ts->t[i].colnames[j]);
		}
		fprintf(fp, "%s\n", ts->t[i].colnames[j]);
		fclose(fp);
	}

	for (i = 0; i < ts->current_tables_count; i++){		
		for (j = 0; j < ts->t[i].current_cols_count; j++){
			strcpy(filename, ts->t[i].tablename);
			strcat(filename, "_");
			strcat(filename, ts->t[i].colnames[j]);
			strcat(filename, ".txt");
			fp = fopen(filename, "w");
			for (k = 0; k < ts->t[i].current_rows_count; k++){
				if (strcmp(ts->t[i].rows[k].id, " ") != 0){
					if (ts->t[i].rows[j].col[k].version >= 1){
						fprintf(fp, "%s,", ts->t[i].rows[k].id);
						fprintf(fp, "%s\n", ts->t[i].rows[k].col[j].value);
					}
				}
				
			}
			fclose(fp);
		}
	}	
}


void command_processor(char *command, tables *ts);

void createtable(char *name,table *t){
	
	
t->tablename = (char*)malloc(strlen(name)*sizeof(char));
strcpy(t->tablename, name);
t->colnames = (char**)malloc(MAX_COLS_IN_DB*sizeof(char*));
t->current_cols_count = 0;
t->current_rows_count = 0;
t->rows = (rowlist*)malloc(MAX_ROWS_IN_DB*sizeof(rowlist));

return;
}

void appendtorow(char *colname, char *rowid, table t, char *data){
	int i = 0, colnum = 0, rownum = 0;
	for (i = 0; i < t.current_cols_count; i++){
		if (strcmp(t.colnames[i], colname) == 0){
			colnum = i;
			break;
		}
	}
	if (i == t.current_cols_count){
		printf("Colname %s not found", colname);
		return;
	}
	for (i = 0; i < t.current_rows_count; i++){
		if (strcmp(rowid, t.rows[i].id) == 0){
			rownum = i;
			break;
		}
	}

	if (i == t.current_rows_count){
		printf("Row Id %s not found", rowid);
		return;
	}
	strcat(t.rows[rownum].col[colnum].value, "|");
	strcat(t.rows[rownum].col[colnum].value, data);

}

void Put(char *colname, char *rowid, table *t, char *data){
	int i = 0, j = 0, colnum = 0, rownum = 0;
	columnlist *c, *temp;
	for (i = 0; i < t->current_cols_count; i++){
		if (strcmp(t->colnames[i], colname) == 0){
			colnum = i;
			break;
		}
	}
	if (i == t->current_cols_count){
		t->colnames[t->current_cols_count] = (char*)malloc(strlen(colname)*sizeof(char));
		strcpy(t->colnames[t->current_cols_count], colname);
		colnum = t->current_cols_count;
		t->current_cols_count = t->current_cols_count + 1;
	}
	for (i = 0; i < t->current_rows_count; i++){
		if (strcmp(rowid, t->rows[i].id) == 0){
			rownum = i;
			break;
		}
	}
	if (i == t->current_rows_count){
		t->rows[t->current_rows_count].id = (char*)malloc(strlen(rowid)*sizeof(char));
		strcpy(t->rows[t->current_rows_count].id, rowid);
		t->rows[t->current_rows_count].col = (columnlist*)malloc(sizeof(columnlist)*MAX_COLS_IN_DB);
		for (j = 0; j < MAX_COLS_IN_DB; j++){
			t->rows[t->current_rows_count].col[j].prev = NULL;
			t->rows[t->current_rows_count].col[j].version = 0;
		}
		rownum = t->current_rows_count;
		t->current_rows_count = t->current_rows_count + 1;
	}
	if (t->rows[rownum].col[colnum].version == 0){
		t->rows[rownum].col[colnum].value = (char*)malloc(MAX_SIZE_OF_DATA*sizeof(char));
		strcpy(t->rows[rownum].col[colnum].value, data);
		t->rows[rownum].col[colnum].version = 1;
	}
	else{
		c = (columnlist*)malloc(sizeof(columnlist));
		c->value = (char*)malloc(MAX_SIZE_OF_DATA*sizeof(char));
		c->version = t->rows[rownum].col[colnum].version;
		strcpy(c->value, t->rows[rownum].col[colnum].value);
		strcpy(t->rows[rownum].col[colnum].value, data);
		t->rows[rownum].col[colnum].version = t->rows[rownum].col[colnum].version + 1;
		c->prev = t->rows[rownum].col[colnum].prev;
		t->rows[rownum].col[colnum].prev = c;

	}
}

void Get(char *rowid, table *t){
	int i = 0, rownum = 0;
	for (i = 0; i < t->current_rows_count; i++){
		if (strcmp(rowid, t->rows[i].id) == 0){
			rownum = i;
			break;
		}
	}
	if (i == t->current_rows_count){
		printf("Row %s did not exist in the table\n", rowid);
		return;
	}
	if (strcmp(t->rows[rownum].id, " ") != 0){
		printf("___________________________________________________________\n\n");
		for (i = 0; i < t->current_cols_count; i++){
			printf("%-20s:%20s\n", t->colnames[i], t->rows[rownum].col[i].value);
		}
		printf("___________________________________________________________\n\n");
	}
	

	return;
}

void scan(table *t, int startid, int endid){
	int i = 0,j = 0;
	for (j = 0; j < t->current_cols_count; j++){
		printf("%20s", t->colnames[j]);
	}
	printf("\n\t___________________________________________________________\n\n");
	
	for (i = startid; i <= endid; i++){
		for (j = 0; j < t->current_cols_count; j++){
			printf("%20s", t->rows[i].col[j].value);
		}
		printf("\n");
	}
}




void Delete(char *rowid,table *t){
	int i = 0, rownum = 0;
	for (i = 0; i < t->current_rows_count; i++){
		if (strcmp(rowid, t->rows[i].id) == 0){
			rownum = i;
			break;
		}
	}
	if (i == t->current_rows_count){
		printf("Row %s did not exist in the table\n", rowid);
		return;
	}

	strcpy(t->rows[i].id, " ");

	return;
}


void loadfromdisk(tables *ts){

	int nooftables, i = 0, j = 0, k = 0, colcount = 0, rowcount = 0;
	char token[100], filename[30], colname[30],id[30];
	FILE *fp, *tp;

	fp = fopen("db.txt", "r");
	fgets(token, 100, fp);
	token[strlen(token) - 1] = '\0';
	nooftables = atoi(token);

	ts->current_tables_count = nooftables;

	for (i = 0; i < nooftables; i++){
		fgets(token, 100, fp);
		token[strlen(token) - 1] = '\0';
		createtable(token, &ts->t[i]);
		strcat(token, ".txt");
		tp = fopen(token, "r");

		fgets(token, 100, tp);
		token[strlen(token) - 1] = '\0';
		colcount = atoi(token);
		ts->t[i].current_rows_count = 0;
		//ts->t[i].current_rows_count = atoi(token);
		fgets(token, 100, tp);
		token[strlen(token) - 1] = '\0';
		rowcount = atoi(token);
		ts->t[i].current_cols_count = 0;
		//ts->t[i].current_cols_count = atoi(token);

		for (j = 0; j < colcount; j++){
			fgets(token, 100, tp);
			token[strlen(token) - 1] = '\0';
			ts->t[i].colnames[j] = (char*)malloc(strlen(token));
			strcpy(ts->t[i].colnames[j], token);
		}
		
		fclose(tp);
	}
	fclose(fp);

	

	for (i = 0; i < nooftables; i++){
		
		for (j = 0; j < colcount; j++){
			strcpy(filename, ts->t[i].tablename);
			strcat(filename, "_");
			strcat(filename, ts->t[i].colnames[j]);
			strcat(filename, ".txt");
			fp = fopen(filename, "r");
			while (fgets(token, 100, fp)){
				token[strlen(token) - 1] = '\0';
				for (k = 0; token[k] != ','; k++){
					id[k] = token[k];
				}
				id[k] = '\0';
				Put(ts->t[i].colnames[j], id, &ts->t[i], &token[k+1]);
			}
			fclose(fp);

		}
	}


}







int main(){
	char command[200];
	tables *ts = (tables*)malloc(sizeof(tables));
	ts->t = (table*)malloc(MAX_TABLES_IN_DB*sizeof(table));
	ts->current_tables_count = 0;
	loadfromdisk(ts);
	while (1){
		printf("MRND_SQL>>>");
		gets(command);
		if (strcmp(command, "exit") == 0){

			storetodisk(ts);

			break;
		}
		command_processor(command,ts);
	}
}








void create_processor(char *command, tables *ts){
	int i = 0, j = 0;
	char token[40];
	for (; command[i] == ' '; i++);
	for (; !(command[i] == ' ' || command[i] == '\0' || command[i] == '\n'); i++, j++){
		token[j] = command[i];
	}
	token[j] = '\0';
	createtable(token, &(ts->t[ts->current_tables_count]));
	ts->current_tables_count = ts->current_tables_count + 1;

}
void get_processor(char *command, tables *ts){
	int i = 0, j = 0,tno;
	char token[40],value[40];
	for (; command[i] == ' '; i++);
	for (; command[i] != ' '; i++, j++){
		token[j] = command[i];
	}
	token[j] = '\0';
	for (j = 0; j < ts->current_tables_count; j++){
		if (strcmp(token, ts->t[j].tablename) == 0){
			tno = j;
			break;
		}
	}
	if (j == ts->current_tables_count){
		printf("Table did not found retry\n");
		return;
	}
	j = 0;
	for (; command[i] == ' '; i++);
	for (; !(command[i] == ' '||command[i]==':'); i++, j++){
		value[j] = command[i];
	}
	value[j] = '\0';
	if (strcmp(value, "id") != 0){
		printf("Right now get command only work with Row Id");
		return;
	}
	j = 0;
	for (; command[i] == ' '; i++);
	i++;
	for (; command[i] == ' '; i++);
	for (; !(command[i] == ' ' || command[i] == '\0' || command[i] == '\n'); i++, j++){
		value[j] = command[i];
	}
	value[j] = '\0';

	Get(value, &(ts->t[tno]));

}
void put_sub_processor(char *command, tables *ts,int tno,char *rowvalue){
	int i = 0, j = 0;
	char col[40], value[40];
	for (; command[i] == ' '; i++);
	for (; !(command[i] == ' '||command[i]==':'); i++, j++){
		col[j] = command[i];
	}
	col[j] = '\0';
	j = 0;
	for (; command[i] == ' '; i++);
	i++;
	for (; command[i] == ' '; i++);
	for (; !(command[i] == ' ' || command[i] == '\n'||command[i]=='\0'); i++, j++){
		value[j] = command[i];
	}
	value[j] = '\0';
	Put(col, rowvalue, &(ts->t[tno]), value);
	for (; command[i] == ' '; i++);
	if (command[i] == '\0' || command[i] == '\n'){
		return;
	}
	else{
		put_sub_processor(&command[i], ts, tno, rowvalue);
	}

}
void put_processor(char *command, tables *ts){
	int i = 0, j = 0, tno;
	char token[40], row[40],col[40],value[40];
	for (; command[i] == ' '; i++);
	for (; command[i] != ' '; i++, j++){
		token[j] = command[i];
	}
	token[j] = '\0';
	for (j = 0; j < ts->current_tables_count; j++){
		if (strcmp(token, ts->t[j].tablename) == 0){
			tno = j;
			break;
		}
	}
	if (j == ts->current_tables_count){
		printf("Table did not found retry\n");
		return;
	}
	j = 0;
	for (; command[i] == ' '; i++);
	for (; command[i] != ' '; i++, j++){
		row[j] = command[i];
	}
	row[j] = '\0';

	put_sub_processor(&command[i], ts, tno, row);





	return;

}


void delete_processor(char *command, tables *ts){
	int i = 0, j = 0, tno;
	char token[40], value[40];
	for (; command[i] == ' '; i++);
	for (; command[i] != ' '; i++, j++){
		token[j] = command[i];
	}
	token[j] = '\0';
	for (j = 0; j < ts->current_tables_count; j++){
		if (strcmp(token, ts->t[j].tablename) == 0){
			tno = j;
			break;
		}
	}
	if (j == ts->current_tables_count){
		printf("Table did not found retry\n");
		return;
	}
	j = 0;
	for (; command[i] == ' '; i++);
	for (; command[i] != ' '; i++, j++){
		value[j] = command[i];
	}
	value[j] = '\0';
	if (strcmp(value, "id") != 0){
		printf("Right now delete only works with id");
		return;
	}
	j = 0;
	for (; command[i] == ' '; i++);
	i++;
	for (; command[i] == ' '; i++);
	for (; !(command[i] == ' ' || command[i] == '\0' || command[i] == '\n'); i++, j++){
		value[j] = command[i];
	}
	value[j] = '\0';

	Delete(value, &(ts->t[tno]));
	return;
}


void scan_processor(char *command, tables *ts){
	int i = 0, j = 0, tno, startidnum = 0, endidnum = 0;
	char token[40], col[40], value[40];
	for (; command[i] == ' '; i++);
	for (; command[i] != ' '; i++, j++){
		token[j] = command[i];
	}
	token[j] = '\0';
	for (j = 0; j < ts->current_tables_count; j++){
		if (strcmp(token, ts->t[j].tablename) == 0){
			tno = j;
			break;
		}
	}
	if (j == ts->current_tables_count){
		printf("Table did not found retry\n");
		return;
	}
	

	j = 0;
	for (; command[i] == ' '; i++);
	for (; !(command[i] == ' ' || command[i] == ','); i++, j++){
		col[j] = command[i];
	}
	col[j] = '\0';
	j = 0;
	for (; command[i] == ' '; i++);
	i++;
	for (; command[i] == ' '; i++);
	for (; !(command[i] == ' ' || command[i] == '\n' || command[i] == '\0'); i++, j++){
		value[j] = command[i];
	}
	value[j] = '\0';
	for (j = 0; j < ts->t[tno].current_rows_count; j++){
		if (strcmp(col, ts->t[tno].rows[j].id) == 0){
			startidnum = j;
			break;
		}
	}

	if (j == ts->t[tno].current_rows_count){
		printf("No %s Id found in table retry\n",col);
	}

	for (j = 0; j < ts->t[tno].current_rows_count; j++){
		if (strcmp(value, ts->t[tno].rows[j].id) == 0){
			endidnum = j;
			break;
		}
	}
	if (j == ts->t[tno].current_rows_count){
		printf("No %s Id found in table retry\n",value);
	}
	scan(&ts->t[tno], startidnum, endidnum);
	
}


void command_processor(char *command, tables *ts){
	int i = 0, j = 0;
	char token[40];
	for (i = 0; command[i] == ' '; i++);
	for (; command[i] != ' '; i++,j++){
		token[j] = command[i];
	}
	token[j] = '\0';
	if (strcmp(token, "create")==0){
		create_processor(&command[i], ts);
	}
	else if(strcmp(token, "get")==0){
		get_processor(&command[i], ts);
	}
	else if (strcmp(token, "put") == 0){
		put_processor(&command[i], ts);
	}
	else if (strcmp(token, "delete") == 0){
		delete_processor(&command[i], ts);
	}
	else if (strcmp(token, "scan") == 0){
		scan_processor(&command[i], ts);
	}
	return;

}



/*

create users
put users id1 Name:Raju Age:21 Place:Tuni
put users id2 Name:Anil Age:20 Place:VSKP
put users id3 Name:Swapna Age:20 Place:SKLM
create students
put students id1 Name:Raju Age:21 Place:Tuni
put students id2 Name:Anil Age:20 Place:VSKP
put students id3 Name:Swapna Age:20 Place:SKLM
scan students id1,id2


get students id:id1
get users id:id3



*/