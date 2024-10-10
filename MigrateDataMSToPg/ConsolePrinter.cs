namespace MigrateDataMSToPg;

public class ConsolePrinter
{
    // Метод для вывода информации о таблицах в консоль
    public void PrintTableInfo(List<TableInfo> tables)
    {
        foreach (var table in tables)
        {
            Console.WriteLine($"Таблица: {table.TableName}");
            Console.WriteLine($"Размер: {table.SizeMB:F2} MB");
            Console.WriteLine("Столбцы:");

            foreach (var column in table.Columns)
            {
                Console.WriteLine($"\t- {column}");
            }

            Console.WriteLine(new string('-', 50));
        }
    }
}