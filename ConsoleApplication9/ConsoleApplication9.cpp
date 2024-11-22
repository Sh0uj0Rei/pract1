#include <iostream>
#include <sstream>
#include <string>
#include <filesystem>
#include <regex>

using namespace std;

struct Row {
    string* data;
    int size;

    Row(int size) : size(size) {
        data = new string[size];
    }

    ~Row() {
        delete[] data;
    }
};

struct Table {
    string name;
    string* columns;
    int columnCount;
    Row** rows;
    int rowCount;
    int capacity;

    Table(const string& name, const string* columns, int columnCount)
        : name(name), columnCount(columnCount), rowCount(0), capacity(10) {
        this->columns = new string[columnCount];
        for (int i = 0; i < columnCount; ++i) {
            this->columns[i] = columns[i];
        }
        rows = new Row * [capacity];
    }

    ~Table() {
        delete[] columns;
        for (int i = 0; i < rowCount; ++i) {
            delete rows[i];
        }
        delete[] rows;
    }

    void insertRow(const string* values, int size) {
        if (size != columnCount) {
            cerr << "Error: Number of values does not match number of columns.\n";
            return;
        }
        if (rowCount == capacity) {
            capacity *= 2;
            Row** newRows = new Row * [capacity];
            for (int i = 0; i < rowCount; ++i) {
                newRows[i] = rows[i];
            }
            delete[] rows;
            rows = newRows;
        }
        rows[rowCount++] = new Row(size);
        for (int i = 0; i < size; ++i) {
            rows[rowCount - 1]->data[i] = values[i];
        }
    }

    void select(const string* selectColumns, int selectCount, const string& conditionCol, const string& conditionVal) {
        int* selectIndices = new int[selectCount];
        int conditionIndex = -1;

        for (int i = 0; i < selectCount; ++i) {
            bool found = false;
            for (int j = 0; j < columnCount; ++j) {
                if (columns[j] == selectColumns[i]) {
                    selectIndices[i] = j;
                    found = true;
                    break;
                }
            }
            if (!found) {
                cerr << "Error: Column " << selectColumns[i] << " not found.\n";
                delete[] selectIndices;
                return;
            }
        }

        if (!conditionCol.empty()) {
            for (int i = 0; i < columnCount; ++i) {
                if (columns[i] == conditionCol) {
                    conditionIndex = i;
                    break;
                }
            }
            if (conditionIndex == -1) {
                cerr << "Error: Condition column " << conditionCol << " not found.\n";
                delete[] selectIndices;
                return;
            }
        }

        for (int i = 0; i < rowCount; ++i) {
            if (conditionIndex != -1 && rows[i]->data[conditionIndex] != conditionVal) {
                continue;
            }
            for (int j = 0; j < selectCount; ++j) {
                cout << rows[i]->data[selectIndices[j]] << " ";
            }
            cout << endl;
        }

        delete[] selectIndices;
    }

    void deleteRows(const string& conditionCol, const string& conditionVal) {
        int conditionIndex = -1;

        if (!conditionCol.empty()) {
            for (int i = 0; i < columnCount; ++i) {
                if (columns[i] == conditionCol) {
                    conditionIndex = i;
                    break;
                }
            }
            if (conditionIndex == -1) {
                cerr << "Error: Condition column " << conditionCol << " not found.\n";
                return;
            }
        }

        int newRowCount = 0;
        for (int i = 0; i < rowCount; ++i) {
            if (conditionIndex == -1 || rows[i]->data[conditionIndex] == conditionVal) {
                delete rows[i];
            }
            else {
                rows[newRowCount++] = rows[i];
            }
        }
        rowCount = newRowCount;

        cout << "Rows matching the condition were deleted.\n";
    }
};

struct Database {
    Table** tables;
    int tableCount;
    int capacity;

    Database() : tableCount(0), capacity(10) {
        tables = new Table * [capacity];
    }

    ~Database() {
        for (int i = 0; i < tableCount; ++i) {
            delete tables[i];
        }
        delete[] tables;
    }

    void createTable(const string& name, const string* columns, int columnCount) {
        for (int i = 0; i < tableCount; ++i) {
            if (tables[i]->name == name) {
                cerr << "Error: Table " << name << " already exists.\n";
                return;
            }
        }
        if (tableCount == capacity) {
            capacity *= 2;
            Table** newTables = new Table * [capacity];
            for (int i = 0; i < tableCount; ++i) {
                newTables[i] = tables[i];
            }
            delete[] tables;
            tables = newTables;
        }
        tables[tableCount++] = new Table(name, columns, columnCount);
        cout << "Table " << name << " created with columns: ";
        for (int i = 0; i < columnCount; ++i) {
            cout << columns[i] << " ";
        }
        cout << endl;
    }

    Table* getTable(const string& name) {
        for (int i = 0; i < tableCount; ++i) {
            if (tables[i]->name == name) {
                return tables[i];
            }
        }
        cerr << "Error: Table " << name << " not found.\n";
        return nullptr;
    }
};

bool tableExist(const string& tableName, Table** tables, int tableCount) {
    for (int i = 0; i < tableCount; ++i) {
        if (tables[i]->name == tableName) {
            return true;
        }
    }
    return false;
}

bool columnExist(const string& tableName, const string& columnName, Table** tables, int tableCount) {
    for (int i = 0; i < tableCount; ++i) {
        if (tables[i]->name == tableName) {
            for (int j = 0; j < tables[i]->columnCount; ++j) {
                if (tables[i]->columns[j] == columnName) {
                    return true;
                }
            }
        }
    }
    return false;
}

void separateDot(const string& input, string& table, string& column) {
    size_t dotPos = input.find('.');
    if (dotPos != string::npos) {
        table = input.substr(0, dotPos);
        column = input.substr(dotPos + 1);
    }
}

void select(string command, Database& db) {
    regex selectPattern(R"(SELECT\s+(.+?)\s+FROM\s+(\w+)\s*(WHERE\s+(\w+)\s*=\s*(\w+|\".*?\"))?)");
    smatch match;

    if (regex_search(command, match, selectPattern)) {
        string columnsStr = match[1].str();
        string tableName = match[2].str();
        string conditionCol = match[4].str();
        string conditionVal = match[5].str();

        // Удаляем кавычки, если они есть
        if (conditionVal.front() == '"' && conditionVal.back() == '"') {
            conditionVal = conditionVal.substr(1, conditionVal.size() - 2);
        }

        string* selectColumns = new string[10];
        int selectCount = 0;
        istringstream colStream(columnsStr);
        string column;
        while (getline(colStream, column, ',')) {
            selectColumns[selectCount++] = column;
        }

        Table* table = db.getTable(tableName);
        if (table) {
            table->select(selectColumns, selectCount, conditionCol, conditionVal);
        }
        else {
            cerr << "Error: Table " << tableName << " not found.\n";
        }
        delete[] selectColumns;
    }
    else {
        cerr << "Invalid SELECT command.\n";
    }
}

void deleteRows(string command, Database& db) {
    regex deletePattern(R"(DELETE\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*=\s*(\w+|\".*?\"))");
    smatch match;

    if (regex_search(command, match, deletePattern)) {
        string tableName = match[1].str();
        string conditionCol = match[2].str();
        string conditionVal = match[3].str();

        // Удаляем кавычки, если они есть
        if (conditionVal.front() == '"' && conditionVal.back() == '"') {
            conditionVal = conditionVal.substr(1, conditionVal.size() - 2);
        }

        Table* table = db.getTable(tableName);
        if (table) {
            table->deleteRows(conditionCol, conditionVal);
        }
        else {
            cerr << "Error: Table " << tableName << " not found.\n";
        }
    }
    else {
        cerr << "Invalid DELETE command.\n";
    }
}

void executeCommand(Database& db, const string& command) {
    istringstream ss(command);
    string action;
    ss >> action;

    if (action == "CREATE") {
        string tableName;
        ss >> tableName;

        string columnsStr;
        getline(ss, columnsStr, ')');
        columnsStr = columnsStr.substr(columnsStr.find('(') + 1);

        string* columns = new string[10];
        int columnCount = 0;
        istringstream colStream(columnsStr);
        string column;
        while (getline(colStream, column, ',')) {
            columns[columnCount++] = column;
        }

        db.createTable(tableName, columns, columnCount);
        delete[] columns;

    }
    else if (action == "INSERT") {
        string tableName;
        ss >> tableName;

        string valuesStr;
        getline(ss, valuesStr, ')');
        valuesStr = valuesStr.substr(valuesStr.find('(') + 1);

        string* values = new string[10];
        int valueCount = 0;
        istringstream valueStream(valuesStr);
        string value;
        while (getline(valueStream, value, ',')) {
            if (!value.empty() && value.front() == '"' && value.back() == '"') {
                value = value.substr(1, value.size() - 2);
            }
            values[valueCount++] = value;
        }

        Table* table = db.getTable(tableName);
        if (table) {
            table->insertRow(values, valueCount);
        }
        else {
            cerr << "Error: Table " << tableName << " not found.\n";
        }
        delete[] values;

    }
    else if (action == "SELECT") {
        select(command, db);

    }
    else if (action == "DELETE") {
        deleteRows(command, db);

    }
    else if (action == "EXIT") {
        exit(0);
    }
    else {
        cerr << "Unknown command: " << action << endl;
    }
}

int main() {
    setlocale(LC_ALL, "RUSSIAN");
    Database db;
    string command;

    cout << "Enter commands (EXIT to quit):\n";

    while (true) {
        cout << "> ";
        getline(cin, command);
        executeCommand(db, command);
    }

    return 0;
}