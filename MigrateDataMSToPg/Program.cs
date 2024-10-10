using System.Data;
using System.Data.SqlClient;
using MigrateDataMSToPg;
using Npgsql;


string mssqlServer = "RACKOT\\MSSQLSERVER01";
string mssqlDatabase = "Nestle_CRM_Development_reserv";
string mssqlUser = "migrate";
string mssqlPassword = "1234";

// Параметры подключения к PostgreSQL
string pgHost = "localhost";
string pgDatabase = "Nestle_CRM_Development_AWS_without_constainces";
string pgUser = "babelfish_user";
string pgPassword = "12345678";
int pgPort = 5433;
string pgTableSchema = "nestle_crm_development_reserv_dbo";

string mssqlConnectionString = $"Server={mssqlServer};Database={mssqlDatabase};User Id={mssqlUser};Password={mssqlPassword};";
string pgConnectionString = $"Host={pgHost};Port={pgPort};Database={pgDatabase};Username={pgUser};Password={pgPassword}";


try
{
    
    // Создаем подключения к MSSQL и PostgreSQL


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
                DataTable dataTable = msDatabaseHelper.GetMSSQLTableData(msConn, table, tableColumns[table], offset, fetchSize);

                if (dataTable.Rows.Count == 0)
                {
                    // Если данные закончились, выходим из цикла
                    break;
                }

                // Вставляем данные порциями в PostgreSQL
                PGDatabaseHelper.BulkInsertIntoPostgreSQL(pgConn, msConn, pgTableSchema, table, tableColumns[table], dataTable);
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
