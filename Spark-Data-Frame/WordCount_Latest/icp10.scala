## val df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("nullValue", "NA").option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss").option("mode", "failfast").load("C:\\Users\\prave\\OneDrive\\Documents\\GitHub\\Big-Data-Programing\\Spark-Data-Frame\\survey.csv")
   ## val dfs = df.select($"Gender", (when($"treatment" === "Yes", 1).otherwise(0)).alias("All-Yes"), (when($"treatment" === "No", 1).otherwise(0)).alias("All-Nos"))
   ## val df3 = dfs.groupBy("Gender")
val df = sqlContext.read.option("header", "true").option("inferSchema", "true").csv("C:\\Users\\prave\\OneDrive\\Documents\\GitHub\\Big-Data-Programing\\Spark-Data-Frame\\survey.csv")

def parseGender(g: String) = {
  g.toLowerCase match {
    case "male" | "m" | "male-ish" | "maile" | "mal" | "male (cis)" | "make" |
        "male " | "man" | "msle" | "mail" | "malr" | "cis man" | "cis male" =>
      "Male"
    case "cis female" | "f" | "female" | "woman" | "femake" | "female " |
        "cis-female/femme" | "female (cis)" | "femail" =>
      "Female"
    case _ => "Transgender"
  }
}

val genderUDF = udf( parsGender _ ) 
val df3 = df.select((genderUDF($"Gender")).alias("Gender"),$"Country")
val df4=df3.groupBy("Gender")
val df6=df.groupBy("Treatment")


## task-2:

val d2f=df.select($"Gender", $"Country", $"Age").groupBy("Country")

val df2 = df.select(
     |   $"Gender",
     |   (when($"treatment" === "Yes", 1).otherwise(0)).alias("All-Yes"),
     |   (when($"treatment" === "No", 1).otherwise(0)).alias("All-Nos")
     | )

val df3 = df2
  .groupBy("Gender")
  .agg(sum($"All-Yes"), sum($"All-Nos"))



spark.sql("select * from Survey ORDER BY Timestamp limit 13").createTempView("Test")
spark.sql("select * from Test order by Timestamp desc limit 1").show()






spark.sql("select Gender, Age, Country from survey_tb1 group by gender order by country")













val Male = df.select((dfm($"Gender")).alias("Male")
  
def parseFemaleGender(g: String) = {
  g.toLowerCase match {
    case "f" | "female" | "woman" | "female " |
       "F" =>
      "Female"
  }
}
val parseFealeGenderUDF = udf(parseFemaleGender _ ) 
val Female = df.select((parseFemaleGenderUDF($"Gender")).alias("Female")




def parseMaleGender(g: String) = {
  g.toLowerCase match {
    case "male" | "m" | 
        "Male " | "  =>
      "Male"
  }
}