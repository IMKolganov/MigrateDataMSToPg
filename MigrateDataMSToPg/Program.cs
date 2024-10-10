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

var msDatabaseHelper = new MSDatabaseHelper(mssqlConnectionString);

try
{
    
    List<TableInfo> tables = msDatabaseHelper.GetTablesAndSizes();

    msDatabaseHelper.GetColumnsForTables(tables);

    var consolePrinter = new ConsolePrinter();
    consolePrinter.PrintTableInfo(tables);
    
    var tableColumns = msDatabaseHelper.GetTableColumns(mssqlConnectionString);
    // Генерируем INSERT запросы
    foreach (var table in tableColumns.Keys)
    {
        string insertQuery = msDatabaseHelper.GenerateInsertQuery(table, tableColumns[table]);
        Console.WriteLine(insertQuery);
        Console.WriteLine();
    }
}
catch (Exception ex)
{
    Console.WriteLine($"Произошла ошибка: {ex.Message}");
}

try
{
    // Получаем список таблиц и их столбцов из MSSQL
    var tableColumns = msDatabaseHelper.GetTableColumns(mssqlConnectionString);

    // Перенос данных из MSSQL в PostgreSQL
    foreach (var table in tableColumns.Keys)
    {
        // Создаем подключения к MSSQL и PostgreSQL
        using (SqlConnection msConn = new SqlConnection(mssqlConnectionString))
        using (var pgConn = new NpgsqlConnection(pgConnectionString))
        {
            DataTable dataTable = msDatabaseHelper.GetMSSQLTableData(msConn, table, tableColumns[table]);
            msConn.Open();
            pgConn.Open();
            PGDatabaseHelper.BulkInsertIntoPostgreSQL(pgConn, msConn, pgTableSchema,table, tableColumns[table],
                dataTable);
        }
    }

    Console.WriteLine("Данные успешно перенесены.");
}
catch (Exception ex)
{
    Console.WriteLine($"Произошла ошибка: {ex.Message}");
}
