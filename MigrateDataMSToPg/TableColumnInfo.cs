namespace MigrateDataMSToPg;

public class TableColumnInfo
{
    public string TableName { get; set; }      // Имя таблицы
    public string ColumnName { get; set; }     // Имя столбца
    public string DataType { get; set; }       // Тип данных столбца
    public bool IsNullable { get; set; }       // Может ли столбец быть NULL
}