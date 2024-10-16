﻿using System.Data;
using System.Data.SqlClient;
using System.Text;
using Npgsql;

namespace MigrateDataMSToPg;

public class PGDatabaseHelper
{
    public static void InsertDataIntoPostgreSQL(string connectionString, string tableName, string tableSchema,
        List<(string ColumnName, string DataType)> columns, DataTable dataTable)
    {
        using (var conn = new NpgsqlConnection(connectionString))
        {
            conn.Open();

            if (!TableExists(conn, tableName, tableSchema))
            {
                Console.WriteLine($"Таблица '{tableName}' не найдена в базе данных PostgreSQL.");
                return;
            }

            var filteredColumns = columns.ToList();


            Console.WriteLine($"Таблица '{tableName}' найдена в базе данных PostgreSQL. Подготовка и импорт данных...");
            foreach (DataRow row in dataTable.Rows)
            {
                string columnNames = string.Join(", ", filteredColumns.ConvertAll(c => $"\"{c.ColumnName}\""));
                string columnValues = string.Join(", ", filteredColumns.ConvertAll(c => "@" + c.ColumnName));

                string insertQuery =
                    $"INSERT INTO {tableSchema}.\"{tableName.ToLower()}\" ({columnNames}) OVERRIDING SYSTEM VALUE VALUES ({columnValues});";

                Console.WriteLine($"Executing query: {insertQuery}");

                using (var cmd = new NpgsqlCommand(insertQuery, conn))
                {
                    foreach (var column in filteredColumns)
                    {
                        var parameterValue = row[column.ColumnName];

                        if (column.DataType == "timestamp")
                        {
                            parameterValue = DBNull.Value;
                        }

                        if (column.DataType == "bit")
                        {
                            parameterValue = parameterValue != DBNull.Value && (bool)parameterValue ? 1 : 0;
                        }


                        cmd.Parameters.AddWithValue($"@{column.ColumnName}", parameterValue);

                        Console.WriteLine($"@{column.ColumnName} = {parameterValue}");
                    }

                    cmd.ExecuteNonQuery();
                }
            }

            Console.WriteLine(new string('-', 50)); // Разделитель для лучшей читаемости вывода
        }
    }

    public static void BulkInsertIntoPostgreSQL(NpgsqlConnection pgConnection, SqlConnection msConn, string tableSchema,
        string tableName, List<(string ColumnName, string DataType)> filteredColumns, DataTable dataTable)
    {
        try
        {
            int batchSize = 1000; // Размер батча для пакетной вставки
            int rowCount = dataTable.Rows.Count;

            // Формируем строку с именами столбцов
            string columnNames = GetColumnNames(filteredColumns);

            // Проверяем, существует ли таблица
            if (!TableExists(pgConnection, tableName, tableSchema))
            {
                Console.WriteLine($"Таблица '{tableName}' не найдена в базе данных PostgreSQL.");
                return; // Выходим, если таблицы нет
            }

            int msCount = GetRowCountInMsSql(msConn, tableName);
            int pgCount = GetRowCountInPostgreSQL(pgConnection, tableSchema, tableName);

            Console.WriteLine($"Таблица: {tableName} - Количество записей в MSSQL: {msCount}, в PostgreSQL: {pgCount}");

            // Проверяем, если в PostgreSQL больше или столько же записей, пропускаем таблицу
            if (pgCount >= msCount)
            {
                Console.WriteLine($"Пропуск таблицы '{tableName}' — записи уже существуют.");
                return;
            }

            using (var transaction = pgConnection.BeginTransaction()) // Используем транзакцию
            {
                try
                {
                    for (int i = 0; i < rowCount; i += batchSize)
                    {
                        int currentBatchSize = Math.Min(batchSize, rowCount - i);
                        var insertQuery = PrepareInsertQuery(filteredColumns, tableSchema, tableName, dataTable, i,
                            currentBatchSize);

                        ExecuteBatchInsert(pgConnection, insertQuery);

                        Console.WriteLine($"Batch of {currentBatchSize} rows inserted into {tableName}.");
                    }

                    transaction.Commit(); // Коммит транзакции
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error during bulk insert: {ex.Message}");
                    transaction.Rollback(); // Откат транзакции в случае ошибки
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error during bulk insert: {ex.Message}");
        }
    }


// Метод для проверки, существует ли таблица в базе данных PostgreSQL
    public static bool TableExists(NpgsqlConnection conn, string tableName, string tableSchema)
    {
        string checkQuery = @$"SELECT EXISTS (
                            SELECT FROM information_schema.tables 
                            WHERE table_schema = '{tableSchema}'
                            AND table_name = '{tableName.ToLower()}'
                            );";

        using (var cmd = new NpgsqlCommand(checkQuery, conn))
        {
            cmd.Parameters.AddWithValue("@TableName", tableName);
            return (bool)cmd.ExecuteScalar(); // Возвращает true, если таблица существует
        }
    }

    public static int GetRowCountInMsSql(SqlConnection msConn, string tableName)
    {
        string query = $"SELECT COUNT(*) FROM {tableName};";
        using (var cmd = new SqlCommand(query, msConn))
        {
            return (int)cmd.ExecuteScalar(); // Возвращаем количество записей в таблице MSSQL
        }
    }

    public static int GetRowCountInPostgreSQL(NpgsqlConnection pgConn, string tableSchema, string tableName)
    {
        string query = $"SELECT COUNT(*) FROM {tableSchema}.\"{tableName.ToLower()}\";";
        using (var cmd = new NpgsqlCommand(query, pgConn))
        {
            long rowCount = (long)cmd.ExecuteScalar();
            return (int)rowCount; // Приводим long к int, но будьте осторожны с большими значениями
        }
    }

    private static string GetColumnNames(List<(string ColumnName, string DataType)> filteredColumns)
    {
        return string.Join(", ", filteredColumns.ConvertAll(c =>
            c.ColumnName.Equals("User", StringComparison.OrdinalIgnoreCase)
                ? "\"user\""
                : c.ColumnName.Equals("Order", StringComparison.OrdinalIgnoreCase)
                    ? "\"order\""
                    : c.ColumnName.Equals("Description", StringComparison.OrdinalIgnoreCase)
                        ? "\"description\""
                        : c.ColumnName.Equals("From", StringComparison.OrdinalIgnoreCase)
                            ? "\"from\""
                            : c.ColumnName.Equals("Group", StringComparison.OrdinalIgnoreCase)
                                ? "\"group\""
                                : c.ColumnName.Equals("Table", StringComparison.OrdinalIgnoreCase)
                                    ? "\"table\""
                                    : $"\"{c.ColumnName.ToLower()}\""));
    }

    private static StringBuilder PrepareInsertQuery(List<(string ColumnName, string DataType)> filteredColumns,
        string tableSchema,
        string tableName, DataTable dataTable, int startIndex, int batchSize)
    {
        var insertQuery =
            new StringBuilder(
                $"INSERT INTO {tableSchema}.\"{tableName.ToLower()}\" ({GetColumnNames(filteredColumns)}) OVERRIDING SYSTEM VALUE VALUES ");

        var allRows = new List<string>();
        for (int j = startIndex; j < startIndex + batchSize; j++)
        {
            var rowValues = PrepareRowValues(filteredColumns, dataTable.Rows[j]);
            allRows.Add($"({string.Join(", ", rowValues)})");
        }

        insertQuery.Append(string.Join(", ", allRows));
        insertQuery.Append(";");

        return insertQuery;
    }


    private static List<string> PrepareRowValues(List<(string ColumnName, string DataType)> filteredColumns,
        DataRow row)
    {
        var rowValues = new List<string>();

        foreach (var column in filteredColumns)
        {
            var parameterValue = row[column.ColumnName];

            // Преобразование строк и UUID
            if (parameterValue is string || parameterValue is Guid)
            {
                // Проверяем, если строка пустая, для полей типа "time" ставим NULL
                if (column.DataType == "time" && string.IsNullOrEmpty(parameterValue.ToString()))
                {
                    rowValues.Add("NULL");
                }
                else
                {
                    rowValues.Add($"'{parameterValue.ToString().Replace("'", "''")}'");
                }
            }
            // Преобразуем дату в нужный формат
            else if (parameterValue is DateTime dateTimeValue)
            {
                rowValues.Add($"'{dateTimeValue.ToString("yyyy-MM-dd HH:mm:ss")}'");
            }
            // Если тип данных - timestamp, ставим NULL
            else if (column.DataType == "timestamp")
            {
                rowValues.Add("NULL");
            }
            // Преобразование для типа time
            else if (column.DataType == "time")
            {
                if (parameterValue == DBNull.Value || string.IsNullOrEmpty(parameterValue.ToString()))
                {
                    rowValues.Add("NULL"); // Для пустых значений ставим NULL
                }
                else
                {
                    rowValues.Add($"'{parameterValue.ToString()}'"); // Преобразуем в формат time
                }
            }
            else if (parameterValue is byte[] byteArray)
            {
                rowValues.Add($"'{Convert.ToBase64String(byteArray)}'"); // Преобразуем в base64 для PostgreSQL
            }
            // Преобразование bit в целое
            else if (column.DataType == "bit")
            {
                rowValues.Add(parameterValue != DBNull.Value && (bool)parameterValue ? "1" : "0");
            }
            // Обработка числовых значений с точкой как разделителем дробной части
            else if (parameterValue is double || parameterValue is float || parameterValue is decimal)
            {
                rowValues.Add(Convert.ToString(parameterValue, System.Globalization.CultureInfo.InvariantCulture));
            }
            // Обрабатываем NULL значения
            else if (parameterValue == DBNull.Value)
            {
                rowValues.Add("NULL");
            }
            else
            {
                rowValues.Add(parameterValue.ToString());
            }
        }

        return rowValues;
    }

    private static void ExecuteBatchInsert(NpgsqlConnection pgConnection, StringBuilder insertQuery)
    {
        using (var cmd = new NpgsqlCommand(insertQuery.ToString(), pgConnection))
        {
            // Console.WriteLine(insertQuery.ToString());
            cmd.ExecuteNonQuery();
        }
    }
}