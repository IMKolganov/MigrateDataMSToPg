namespace MigrateDataMSToPg;

public class TableInfo
{
    public string TableName { get; set; }
    public List<string> Columns { get; set; } = new List<string>();
    public double SizeMB { get; set; }
}