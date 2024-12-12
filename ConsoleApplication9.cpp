#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <filesystem>
#include <mutex>
#include <thread>
#include <algorithm>
#include "nlohmann/json.hpp" // Подключите библиотеку JSON (nlohmann/json.hpp)

namespace fs = std::filesystem;
using json = nlohmann::json;

// Структура для хранения схемы данных
struct Schema {
    std::string name;
    int tuples_limit;
    std::map<std::string, std::vector<std::string>> structure;
};

// Основной класс СУБД
class Database {
private:
    Schema schema;
    std::mutex lock;

    std::string getTableDir(const std::string& tableName) {
        return schema.name + "/" + tableName;
    }

    std::string getPrimaryKeyFile(const std::string& tableName) {
        return getTableDir(tableName) + "/" + tableName + "_pk_sequence";
    }

    std::string getLockFile(const std::string& tableName) {
        return getTableDir(tableName) + "/" + tableName + "_lock";
    }

    int getNextPrimaryKey(const std::string& tableName) {
        std::string pkFile = getPrimaryKeyFile(tableName);
        int pk = 1;

        std::ifstream inFile(pkFile);
        if (inFile.is_open()) {
            inFile >> pk;
            inFile.close();
        }

        std::ofstream outFile(pkFile, std::ios::trunc);
        outFile << pk + 1;
        outFile.close();

        return pk;
    }

    void lockTable(const std::string& tableName) {
        std::string lockFile = getLockFile(tableName);
        while (fs::exists(lockFile)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        std::ofstream(lockFile).close();
    }

    void unlockTable(const std::string& tableName) {
        std::string lockFile = getLockFile(tableName);
        fs::remove(lockFile);
    }

public:
    Database(const Schema& schema) : schema(schema) {
        if (!fs::exists(schema.name)) {
            fs::create_directory(schema.name);
        }
        for (const auto& [tableName, columns] : schema.structure) {
            std::string tableDir = getTableDir(tableName);
            if (!fs::exists(tableDir)) {
                fs::create_directory(tableDir);
                std::ofstream pkFile(getPrimaryKeyFile(tableName));
                pkFile << 1;
                pkFile.close();
            }
        }
    }

    void insertInto(const std::string& tableName, const std::vector<std::string>& values) {
        if (schema.structure.find(tableName) == schema.structure.end()) {
            throw std::runtime_error("Table does not exist: " + tableName);
        }

        lockTable(tableName);

        std::string tableDir = getTableDir(tableName);
        int pk = getNextPrimaryKey(tableName);
        std::string newRow = std::to_string(pk) + "," + join(values, ",");

        for (int fileIndex = 1;; ++fileIndex) {
            std::string fileName = tableDir + "/" + std::to_string(fileIndex) + ".csv";
            if (!fs::exists(fileName)) {
                std::ofstream file(fileName);
                file << tableName + "_pk," + join(schema.structure[tableName], ",") << "\n";
                file << newRow << "\n";
                break;
            }

            std::ifstream file(fileName);
            int lineCount = std::count(std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>(), '\n');
            file.close();

            if (lineCount < schema.tuples_limit + 1) {
                std::ofstream file(fileName, std::ios::app);
                file << newRow << "\n";
                break;
            }
        }

        unlockTable(tableName);
    }

    void deleteFrom(const std::string& tableName, int pk) {
        if (schema.structure.find(tableName) == schema.structure.end()) {
            throw std::runtime_error("Table does not exist: " + tableName);
        }

        lockTable(tableName);

        std::string tableDir = getTableDir(tableName);
        for (int fileIndex = 1;; ++fileIndex) {
            std::string fileName = tableDir + "/" + std::to_string(fileIndex) + ".csv";
            if (!fs::exists(fileName)) {
                break;
            }

            std::ifstream inFile(fileName);
            std::string line;
            std::vector<std::string> rows;

            while (std::getline(inFile, line)) {
                if (!line.empty() && std::stoi(line.substr(0, line.find(','))) != pk) {
                    rows.push_back(line);
                }
            }
            inFile.close();

            std::ofstream outFile(fileName, std::ios::trunc);
            for (const std::string& row : rows) {
                outFile << row << "\n";
            }
            outFile.close();
        }

        unlockTable(tableName);
    }

    void select(const std::string& tableName, const std::map<std::string, std::string>& conditions) {
        if (schema.structure.find(tableName) == schema.structure.end()) {
            throw std::runtime_error("Table does not exist: " + tableName);
        }

        std::string tableDir = getTableDir(tableName);

        for (int fileIndex = 1;; ++fileIndex) {
            std::string fileName = tableDir + "/" + std::to_string(fileIndex) + ".csv";
            if (!fs::exists(fileName)) {
                break;
            }

            std::ifstream inFile(fileName);
            std::string line;
            std::getline(inFile, line); // Пропускаем заголовок
            while (std::getline(inFile, line)) {
                std::vector<std::string> row = split(line, ',');

                bool matches = true;
                for (const auto& [column, value] : conditions) {
                    int colIndex = getColumnIndex(tableName, column);
                    if (colIndex == -1 || row[colIndex] != value) {
                        matches = false;
                        break;
                    }
                }
                if (matches) {
                    std::cout << line << "\n";
                }
            }
            inFile.close();
        }
    }

    void crossJoin(const std::string& table1, const std::string& table2) {
        if (schema.structure.find(table1) == schema.structure.end()) {
            throw std::runtime_error("Table does not exist: " + table1);
        }
        if (schema.structure.find(table2) == schema.structure.end()) {
            throw std::runtime_error("Table does not exist: " + table2);
        }

        std::vector<std::vector<std::string>> rows1 = readAllRows(table1);
        std::vector<std::vector<std::string>> rows2 = readAllRows(table2);

        std::vector<std::string> headers1 = schema.structure[table1];
        std::vector<std::string> headers2 = schema.structure[table2];

        std::cout << table1 + "_pk," + join(headers1, ",") + "," + table2 + "_pk," + join(headers2, ",") << "\n";

        for (const auto& row1 : rows1) {
            for (const auto& row2 : rows2) {
                std::cout << join(row1, ",") + "," + join(row2, ",") << "\n";
            }
        }
    }

private:
    std::string join(const std::vector<std::string>& parts, const std::string& delimiter) {
        std::ostringstream oss;
        for (size_t i = 0; i < parts.size(); ++i) {
            oss << parts[i];
            if (i != parts.size() - 1) {
                oss << delimiter;
            }
        }
        return oss.str();
    }

    std::vector<std::string> split(const std::string& str, char delimiter) {
        std::vector<std::string> parts;
        std::istringstream iss(str);
        std::string part;
        while (std::getline(iss, part, delimiter)) {
            parts.push_back(part);
        }
        return parts;
    }

    std::vector<std::vector<std::string>> readAllRows(const std::string& tableName) {
        std::vector<std::vector<std::string>> rows;
        std::string tableDir = getTableDir(tableName);

        for (int fileIndex = 1;; ++fileIndex) {
            std::string fileName = tableDir + "/" + std::to_string(fileIndex) + ".csv";
            if (!fs::exists(fileName)) {
                break;
            }

            std::ifstream inFile(fileName);
            std::string line;
            bool isHeader = true;

            while (std::getline(inFile, line)) {
                if (isHeader) {
                    isHeader = false; // Пропускаем заголовок
                    continue;
                }
                rows.push_back(split(line, ','));
            }
        }

        return rows;
    }

    int getColumnIndex(const std::string& tableName, const std::string& columnName) {
        auto it = schema.structure.find(tableName);
        if (it == schema.structure.end()) {
            return -1;
        }

        const auto& columns = it->second;
        auto colIt = std::find(columns.begin(), columns.end(), columnName);
        if (colIt == columns.end()) {
            return -1;
        }

        return std::distance(columns.begin(), colIt);
    }
};

Schema loadSchema(const std::string& filePath) {
    Schema schema;
    std::ifstream inFile(filePath);

    if (!inFile.is_open()) {
        throw std::runtime_error("Could not open schema file");
    }

    json schemaJson;
    inFile >> schemaJson;

    schema.name = schemaJson["name"];
    schema.tuples_limit = schemaJson["tuples_limit"];
    for (const auto& [tableName, columns] : schemaJson["structure"].items()) {
        schema.structure[tableName] = columns.get<std::vector<std::string>>();
    }

    return schema;
}

// Функция для обработки SQL-запросов
void processQuery(Database& db, const std::string& query) {
    std::istringstream iss(query);
    std::string command;
    iss >> command;

    if (command == "INSERT") {
        std::string tableName;
        std::string valuesSegment;
        iss >> tableName;
        iss.ignore(6); // Пропустить "INTO"
        std::getline(iss, valuesSegment, ')');
        valuesSegment.erase(0, valuesSegment.find('(') + 1);
        std::vector<std::string> values;
        std::istringstream vs(valuesSegment);
        std::string value;
        while (std::getline(vs, value, ',')) {
            values.push_back(value);
        }
        db.insertInto(tableName, values);
    } else if (command == "SELECT") {
        std::string tableName;
        iss >> tableName;
        db.select(tableName, {});
    } else if (command == "DELETE") {
        std::string tableName;
        int pk;
        iss >> tableName >> pk;
        db.deleteFrom(tableName, pk);
    }
}


int main() {
    try {
        // Загрузка схемы из файла
        Schema schema = loadSchema("schema.json");
        Database db(schema);

        std::string query;
        while (true) {
            std::cout << "Введите запрос: ";
            std::getline(std::cin, query);
            if (query == "EXIT") {
                break;
            }
            processQuery(db, query);
        }
    } catch (const std::exception& ex) {
        std::cerr << "Ошибка: " << ex.what() << std::endl;
    }

    return 0;
}