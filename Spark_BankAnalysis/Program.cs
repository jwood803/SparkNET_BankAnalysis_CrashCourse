using Microsoft.Spark.Sql;
using System;
using System.Collections.Generic;

namespace Spark_BankAnalysis
{
    class Program
    {
        static void Main(string[] args)
        {
            // Create session
            var session = SparkSession.Builder().AppName("bank-analysis").GetOrCreate();

            // Set logging level
            session.SparkContext.SetLogLevel("ERROR");

            // Read in CSV into DataFrame
            var df = session
                .Read()
                .Options(new Dictionary<string, string>
                {
                    { "header", "true" },
                    { "inferSchema", "true" },
                    { "delimiter", ";" }
                })
                .Csv("bank.csv");

            df.Show();

            // Schema
            df.PrintSchema();

            // Describe
            df.Describe().Show();

            // Row and column counts
            Console.WriteLine($"Count - {df.Count()}");
            Console.WriteLine($"Columns - {df.Columns().Count}");
            Console.WriteLine(Environment.NewLine);

            // Select single column
            var age = df.Select("age");

            age.Show();

            var multipleColumns = df.Select("age", "balance", "job");

            multipleColumns.Show();

            var nullValues = df.Count() - df.Na().Drop().Count();

            Console.WriteLine($"Null value count - {nullValues}");
            Console.WriteLine(Environment.NewLine);

            var balanceFilter = df.Filter("balance < 0");

            balanceFilter.Show();

            var balanceFilter2 = df.Filter(df["balance"] > 0);

            balanceFilter2.Show();

            var jobGroup = df.GroupBy("job").Count();

            jobGroup.Show();

            var sort = df.Sort("balance");

            sort.Show();

            // Drop columns
            var dropDf = df.Drop("contact", "day", "month", "duration", "campaign", "pdays", "previous", "poutcome", "y", "housing", "loan");

            dropDf.Show();

            // Rename columns
            var renamedDf = dropDf.WithColumnRenamed("default", "hasDefaulted").WithColumnRenamed("loan", "hasLoan");

            renamedDf.Show();

            // Change column values
            var valuesDf = renamedDf.WithColumn("hasDefaulted", Functions.When(Functions.Col("hasDefaulted") == "y", 1).Otherwise(0));

            valuesDf.Show();
        }
    }
}
