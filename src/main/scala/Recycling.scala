import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Recycling {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName(name = "Recycling_Spark")
      .config("spark.streaming.concurrentJobs", 3)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    //Preprocessing
    //Read the .csv file containing the data
    val data = spark.read.format("csv").option("header", "true").load("src/main/datasets/recycling.csv")
    //Create a dataframe containing the data
    var df = data.toDF()
    //Print the dataset and the column types in the output
    df.show(truncate = false)
    df.printSchema()

    // Rename columns
    df = df.withColumnRenamed("DATE", "Date")
      .withColumnRenamed("MONTH", "Month")
      .withColumnRenamed("TYPE", "Type")
      .withColumnRenamed("TOTAL (IN TONS)", "Tons")

    //Count the missing values from dataframe
    println("      Null Values")
    df.select(df.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*).show
    println("      NaN Values")
    df.select(df.columns.map(c => sum(col(c).isNaN.cast("int")).alias(c)): _*).show

    //Extract year from the date column
    var df2 = df.select(col("Date"), substring_index(col("Date"), "/", -1).as("Year"))
    df = df.withColumn("rowId1", monotonically_increasing_id())
    df2 = df2.withColumn("rowId2", monotonically_increasing_id())
    df = df.as("df").join(df2.as("df2"), df("rowId1") === df2("rowId2"), "inner").select("df.Month", "df2.Year", "df.Type", "df.Tons")

    //Change Tons from String to Integer
    df = df.withColumn("Tons", col("Tons").cast(IntegerType))
    df.show(truncate = false)
    df.printSchema()

    // Query for the first question
    println("           Q U E R Y   1         ")
    // show the average number of tons for each Type
    df.groupBy("Type").avg("Tons").show(truncate = false)

    //Query for the second question
    println("           Q U E R Y   2         ")
    //Aggregate the total number of tons per material per year and sort them in descending order
    df2 = df.groupBy(col("Year"), col("Type"))
      .agg(sum(col("Tons")).as("Tons Per Year"))
      .orderBy(col("Year").asc, col("Tons Per Year").desc)
    //Count the years that each type of material is recycled
    df2.select("Type", "Year")
      .groupBy("Type")
      .agg(countDistinct("Year"))
      .show(100)
    // select only the years where Scrap metal has more tons than 5 other materials and show them
    df2 = df2.filter((df2("Type") === "Scrap Metal") || (df2("Type") === "Haz Waste") || (df2("Type") === "Bottle Bill")
      || (df2("Type") === "E-Waste") || (df2("Type") === "Recycled Tires") || (df2("Type") === "Misc. Recycling"))
    // add a new column for the ranking of the materials per year, based on the number of tons recycled.
    df2 = df2.withColumn("rank", row_number().over(Window.partitionBy("Year").orderBy(col("Tons Per Year").desc)))
    df2 = df2.filter(df2("Type") === "Scrap Metal" && df2("rank") === 1)
    df2.show(truncate = false)
    var total = df2.count()
    println("Scrap Metal's number of tons per year was greater than the other 5 types selected for " + total + " years.")

    //Query for the third question
    println("               Q U E R Y   3         ")
    //Aggregate the total number of tons per material per year and sort them in descending order
    var df3 = df.groupBy(col("Year"), col("Type"))
      .agg(sum(col("Tons")).as("Tons Per Year"))
      .orderBy(col("Year").asc, col("Tons Per Year").desc)
    // add a new column for the ranking of the materials per year, based on the number of tons recycled.
    df3 = df3.withColumn("rank", row_number().over(Window.partitionBy("Year").orderBy(col("Tons Per Year").desc)))
    df3 = df3.filter(df3("rank") < 6)
    df3.show(60)

    //Query for the fourth question
    println("           Q U E R Y   4         ")
    //Aggregate the total number of tons per material per year and sort them in descending order
    var df4 = df.groupBy(col("Type"), col("Year"))
      .agg(sum(col("Tons")).as("Tons Per Year"))
      .orderBy(col("Type").asc, col("Tons Per Year").asc)

    //Create a window partitioned and sorted by the different types
    val windowDept = Window.partitionBy("Type").orderBy(col("Type").desc)

    //Create a dataframe containing the minimum of ton per year for each type
    var df44 = df4.withColumn("min", min("Tons Per Year").over(windowDept))
    df44 = df44.filter(df44("Tons Per Year") === df44("min"))
      .drop("Tons Per Year")
    df44.show(50)

    //Create a dataframe containing the maximum of ton per year for each type
    var df45 = df4.withColumn("max", max("Tons Per Year").over(windowDept))
    df45 = df45.filter(df45("Tons Per Year") === df45("max"))
      .drop("Tons Per Year")
    df45.show(50)

    //Query for the fifth question
    println("       Q U E R Y   5         ")
    var df5 = df.groupBy("Month")
      .agg(sum(col("Tons")).as("Tons Per Month"))
      .orderBy(desc("Tons Per Month"))
    df5.show(5, truncate = false)

    //Query for the sixth question
    println("   Q U E R Y   6   ")
    var df6 = df.groupBy("Year")
      .agg(sum(col("Tons")).as("Tons Per Year"))
      .orderBy(asc("Year"))
    df6.show(12, truncate = false)

    //Query for the seventh question
    println("           Q U E R Y   7         ")
    var df7 = df.groupBy("Type")
      .agg(sum(col("Tons")).as("Tons Per Material"))
      .orderBy(desc("Tons Per Material"))
    df7.show(12, truncate = false)

    spark.stop()
    println("Completed.")
  }

}
