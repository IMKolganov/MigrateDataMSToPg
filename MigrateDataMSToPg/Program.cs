using System.Data;
using System.Data.SqlClient;
using MigrateDataMSToPg;
using MigrateDataMSToPg.Configuries;
using Newtonsoft.Json;
using Npgsql;


string configFilePath = "db_config.json";
var config = LoadConfig(configFilePath);

string mssqlConnectionString = $"Server={config.MSSQL.Server};Database={config.MSSQL.Database};User Id={config.MSSQL.User};Password={config.MSSQL.Password};";
string pgConnectionString = $"Host={config.PostgreSQL.Host};Port={config.PostgreSQL.Port};Database={config.PostgreSQL.Database};Username={config.PostgreSQL.User};Password={config.PostgreSQL.Password}";

Console.WriteLine("MSSQL Connection String: " + mssqlConnectionString);
Console.WriteLine("PostgreSQL Connection String: " + pgConnectionString);

try
{
    var msDatabaseHelper = new MSDatabaseHelper();
    // Получаем список таблиц и их столбцов из MSSQL
    var tableColumns = msDatabaseHelper.GetTableColumns(mssqlConnectionString, pgConnectionString);


    string processedTablesFilePath = "processedTables.txt"; // Путь к файлу с пройденными таблицами

    // Считываем все обработанные таблицы из файла
    var processedTables = new HashSet<string>();
    if (File.Exists(processedTablesFilePath))
    {
        processedTables = new HashSet<string>(File.ReadAllLines(processedTablesFilePath));
    }


    // Перенос данных из MSSQL в PostgreSQL
    foreach (var table in tableColumns.Keys)
    {
        // Пропускаем таблицу, если она уже обработана
        if (processedTables.Contains(table.ToLower()))
        {
            Console.WriteLine($"Таблица {table} уже обработана, пропуск...");
            continue;
        }

        var fetchSize = 10000;

        using (SqlConnection msConn = new SqlConnection(mssqlConnectionString))
        using (var pgConn = new NpgsqlConnection(pgConnectionString))
        {
            msConn.Open();
            pgConn.Open();

            int offset = 0;
            int totalRowsProcessed = 0;

            while (true)
            {
                // Загружаем данные порциями из MSSQL
                DataTable dataTable =
                    msDatabaseHelper.GetMSSQLTableData(msConn, table, tableColumns[table], offset, fetchSize);

                if (dataTable.Rows.Count == 0)
                {
                    // Если данные закончились, выходим из цикла
                    break;
                }

                // Вставляем данные порциями в PostgreSQL
                PGDatabaseHelper.BulkInsertIntoPostgreSQL(pgConn, msConn, config.PostgreSQL.Schema, table,
                    tableColumns[table], dataTable);
                totalRowsProcessed += dataTable.Rows.Count;

                // Увеличиваем offset для следующей порции данных
                offset += fetchSize;
            }

            // После обработки всех данных, добавляем таблицу в список обработанных
            File.AppendAllText(processedTablesFilePath, table.ToLower() + Environment.NewLine);
            Console.WriteLine($"Таблица {table} успешно обработана. Всего строк обработано: {totalRowsProcessed}");
        }
    }

    Console.WriteLine("Данные успешно перенесены.");
}
catch (Exception ex)
{
    Console.WriteLine($"Произошла ошибка: {ex.Message}");
}



static DatabaseConfig LoadConfig(string filePath)
{
    try
    {
        string json = File.ReadAllText(filePath);
        var config = JsonConvert.DeserializeObject<DatabaseConfig>(json);
        return config;
    }
    catch (Exception ex)
    {
        Console.WriteLine("Ошибка при загрузке конфигурации: " + ex.Message);
        return null;
    }
}