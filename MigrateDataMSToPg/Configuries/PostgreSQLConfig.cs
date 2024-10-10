namespace MigrateDataMSToPg.Configuries;

public class PostgreSQLConfig
{
    public string Host { get; set; }
    public string Database { get; set; }
    public string User { get; set; }
    public string Password { get; set; }
    public int Port { get; set; }
    public string Schema { get; set; }
}