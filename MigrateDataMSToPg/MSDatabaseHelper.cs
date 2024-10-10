using System.Data;
using System.Text;

namespace MigrateDataMSToPg;

using System;
using System.Collections.Generic;
using System.Data.SqlClient;


public class MSDatabaseHelper
{
    private readonly SqlConnection _msSQLConnection;
    

    public MSDatabaseHelper(SqlConnection msSQLConnection)
    {
        _msSQLConnection = msSQLConnection;
    }

    // Метод для получения списка таблиц и их размеров
    public List<TableInfo> GetTablesAndSizes()
    {
        List<TableInfo> tables = new List<TableInfo>();

        string query = @"
            SELECT
                SCHEMA_NAME(t.schema_id) AS TableSchema,
                t.name AS TableName,
                SUM(p.rows) AS RowCounts,
                SUM(a.total_pages) * 8.0 / 1024 AS TotalSpaceMB,
                SUM(a.used_pages) * 8.0 / 1024 AS UsedSpaceMB,
                SUM(a.data_pages) * 8.0 / 1024 AS DataSpaceMB
            FROM 
                sys.tables t
            INNER JOIN      
                sys.indexes i ON t.OBJECT_ID = i.object_id
            INNER JOIN 
                sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
            INNER JOIN 
                sys.allocation_units a ON p.partition_id = a.container_id
            WHERE 
                t.is_ms_shipped = 0
                AND i.index_id <= 1
            GROUP BY 
                SCHEMA_NAME(t.schema_id),
                t.name
            ORDER BY 
               SCHEMA_NAME(t.schema_id), t.name;
            ";

        using (SqlCommand cmd = new SqlCommand(query, _msSQLConnection))
        {
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    string schema = reader["TableSchema"].ToString();
                    string tableName = reader["TableName"].ToString();
                    double sizeMB = Convert.ToDouble(reader["TotalSpaceMB"]);

                    tables.Add(new TableInfo
                    {
                        TableName = $"{schema}.{tableName}",
                        SizeMB = sizeMB
                    });
                }
            }
        }

        return tables;
    }

    // Метод для получения столбцов для каждой таблицы
    public void GetColumnsForTables(List<TableInfo> tables)
    {
        string query = @"
                SELECT 
                    TABLE_SCHEMA,
                    TABLE_NAME,
                    COLUMN_NAME
                FROM 
                    INFORMATION_SCHEMA.COLUMNS
                ORDER BY 
                    TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
            ";


        using (SqlCommand cmd = new SqlCommand(query, _msSQLConnection))
        {
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    string schema = reader["TABLE_SCHEMA"].ToString();
                    string tableName = reader["TABLE_NAME"].ToString();
                    string columnName = reader["COLUMN_NAME"].ToString();

                    string fullTableName = $"{schema}.{tableName}";
                    TableInfo table = tables.Find(t =>
                        t.TableName.Equals(fullTableName, StringComparison.OrdinalIgnoreCase));

                    if (table != null)
                    {
                        table.Columns.Add(columnName);
                    }
                }
            }
        }
    }
    
    // Метод для получения списка столбцов для каждой таблицы
    internal Dictionary<string, List<(string ColumnName, string DataType)>> GetTableColumns(string connectionString)
    {
        var tableColumns = new Dictionary<string, List<(string ColumnName, string DataType)>>();

        string query = @"
                SELECT
                    TABLE_NAME,
                    COLUMN_NAME,
                    DATA_TYPE
                FROM 
                    INFORMATION_SCHEMA.COLUMNS
                WHERE 
                    TABLE_SCHEMA = 'dbo'
                ORDER BY 
                    TABLE_NAME, ORDINAL_POSITION;
            ";

        using (SqlCommand cmd = new SqlCommand(query, _msSQLConnection))
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
    
    // Метод для генерации INSERT запроса для каждой таблицы
    public string GenerateInsertQuery(string tableName, List<(string ColumnName, string DataType)> columns)
    {
        StringBuilder sb = new StringBuilder();
        StringBuilder columnNames = new StringBuilder();
        StringBuilder columnValues = new StringBuilder();

        sb.AppendLine($"-- INSERT для таблицы {tableName}");

        sb.Append($"INSERT INTO {tableName} (");

        foreach (var column in columns)
        {
            columnNames.Append($"{column.ColumnName}, ");
            columnValues.Append($"@{column.ColumnName}, ");
        }

        // Убираем последние запятые
        columnNames.Length -= 2;
        columnValues.Length -= 2;

        sb.Append(columnNames);
        sb.AppendLine(") VALUES (");
        sb.Append(columnValues);
        sb.AppendLine(");");

        return sb.ToString();
    }
    
    // Метод для получения данных из MSSQL
    public DataTable GetMSSQLTableData(SqlConnection msConn, string tableName, List<(string ColumnName, string DataType)> columns)
    {
        DataTable dataTable = new DataTable();
        string query = $"SELECT {string.Join(", ", columns.ConvertAll(c => $"[{c.ColumnName}]"))} FROM [{tableName}];";

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