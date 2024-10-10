using System.Data;
using Npgsql;

namespace MigrateDataMSToPg;

using System;
using System.Collections.Generic;
using System.Data.SqlClient;


public class MSDatabaseHelper
{
    public MSDatabaseHelper()
    {

    }
    
    
    
    // Метод для получения списка столбцов для каждой таблицы
    internal Dictionary<string, List<(string ColumnName, string DataType)>> GetTableColumns(string mssqlConnectionString, string pgConnectionString)
    {
        using (SqlConnection msConn = new SqlConnection(mssqlConnectionString))
        using (var pgConn = new NpgsqlConnection(pgConnectionString))
        {
            msConn.Open();
            pgConn.Open();

            var tableColumns = new Dictionary<string, List<(string ColumnName, string DataType)>>();

            string query = @"
                SELECT
                    C.TABLE_NAME,
                    C.COLUMN_NAME,
                    C.DATA_TYPE
                FROM 
                    INFORMATION_SCHEMA.COLUMNS C
                INNER JOIN 
                    INFORMATION_SCHEMA.TABLES T
                    ON C.TABLE_NAME = T.TABLE_NAME
                    AND C.TABLE_SCHEMA = T.TABLE_SCHEMA
                WHERE 1=1
                    AND C.TABLE_SCHEMA = 'dbo'
                    AND T.TABLE_TYPE = 'BASE TABLE'
                ORDER BY 
                    C.TABLE_NAME, C.ORDINAL_POSITION;
            ";

            using (SqlCommand cmd = new SqlCommand(query, msConn))
            {
                using (SqlDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string tableName = reader["TABLE_NAME"].ToString();
                        string columnName = reader["COLUMN_NAME"].ToString();
                        string dataType = reader["DATA_TYPE"].ToString();

                        if (!tableColumns.ContainsKey(tableName))
                        {
                            tableColumns[tableName] = new List<(string ColumnName, string DataType)>();
                        }

                        tableColumns[tableName].Add((columnName, dataType));
                    }
                }
            }

            return tableColumns;
        }
    }
    
    // Метод для получения данных из MSSQL
    public DataTable GetMSSQLTableData(SqlConnection msConn, string tableName, List<(string ColumnName, string DataType)> columns, int offset, int fetchSize)
    {
        DataTable dataTable = new DataTable();

        // Формируем SQL-запрос для постраничной выборки данных
        string query = $@"
        SELECT {string.Join(", ", columns.ConvertAll(c => $"[{c.ColumnName}]"))}
        FROM [{tableName}]
        ORDER BY (SELECT NULL) -- Добавляем ORDER BY, чтобы избежать ошибки
        OFFSET {offset} ROWS FETCH NEXT {fetchSize} ROWS ONLY;";

        using (SqlCommand cmd = new SqlCommand(query, msConn))
        {
            using (SqlDataAdapter adapter = new SqlDataAdapter(cmd))
            {
                adapter.Fill(dataTable);
            }
        }

        return dataTable;
    }

}