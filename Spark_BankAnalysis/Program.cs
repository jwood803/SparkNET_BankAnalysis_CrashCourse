using Microsoft.Spark.Sql;
using System;
using System.Collections.Generic;

namespace Spark_BankAnalysis
{
    class Program
    {
        static void Main(string[] args)
        {
            var session = SparkSession.Builder().AppName("bank-analysis").GetOrCreate();

            session.SparkContext.SetLogLevel("ERROR");

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

            Console.WriteLine($"Count - {df.Count()}");
            Console.WriteLine($"Columns - {df.Columns().Count}");
            Console.WriteLine(Environment.NewLine);

            var age = df.Select("age");

            age.Show();

            var nullValues = df.Count() - df.Na().Drop().Count();

            Console.WriteLine($"Null value count - {nullValues}");
            Console.WriteLine(Environment.NewLine);

            var balanceFilter = df.Filter("balance < 0");

            balanceFilter.Show();

            var balanceFilter2 = df.Filter(df["balance"] > 0);

            balanceFilter2.Show();

            var jobGroup = df.GroupBy("job").Count();

            jobGroup.Show();


        }
    }
}
