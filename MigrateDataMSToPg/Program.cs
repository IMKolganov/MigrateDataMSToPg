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
    using (SqlConnection msConn = new SqlConnection(mssqlConnectionString))
    using (var pgConn = new NpgsqlConnection(pgConnectionString))
    {
        msConn.Open();
        pgConn.Open();
        
        var msDatabaseHelper = new MSDatabaseHelper(msConn);
        // Получаем список таблиц и их столбцов из MSSQL
        var tableColumns = msDatabaseHelper.GetTableColumns(mssqlConnectionString);

        // Перенос данных из MSSQL в PostgreSQL
        foreach (var table in tableColumns.Keys)
        {
            DataTable dataTable = msDatabaseHelper.GetMSSQLTableData(msConn, table, tableColumns[table]);
            PGDatabaseHelper.BulkInsertIntoPostgreSQL(pgConn, msConn, pgTableSchema, table, tableColumns[table],
                dataTable);

        }
    }

    Console.WriteLine("Данные успешно перенесены.");
}
catch (Exception ex)
{
    Console.WriteLine($"Произошла ошибка: {ex.Message}");
}
