
//Scala concepts :
//higher order function example :

Def multiplier (factor:int):int =>Int = {
 (X:int)=x* factor 
}
Val double =multipiler(2)
Println(double(5))

//Currying in scala :

Def addcurried (a:int)(b:int) = a+b 
addcurried(2)(3)

//closure in scala :

var factor = 3

val multiplier = (x: Int) => x * factor

println(multiplier(5))

//Monad in scala :
//types of MOnad :
1.Identity Monad

val x = 5
val result = x.flatMap(y => y + 1)

2. Option Monad 

val result = Some(10).flatMap(x => Some(x + 5)) 

3.Either Monad
Right(value) for success, Left(error) for failure.

val result: Either[String, Int] = Right(10).flatMap(x => Right(x * 2))

4.Future	Monad-- used for async compution

 val result = Future(10).flatMap(x => Future(x * 2))

5.List	Monad
6.Reader	Monad
7.Writer	Monad
8.State	Monad
9.IO	Monad

//foldLeft() function in scala

val numbers = List(1, 2, 3, 4, 5)

val sum = numbers.foldLeft(0)((acc, x) => acc + x)

println(sum)

--need initial value 

//foldRight()
it's oppsite of foldLeft fuction starting from right side with initial value.

//reduceLeft()
--same as foldLeft but no initial value 

val nums = List(1, 2, 3)
nums.reduceLeft(_ + _)

//scanLeft()
works as foldLeft but it reurns a result as a collection 
val nums = List(1, 2, 3)
nums.scanLeft(0)(_ + _)

output : List(0, 1, 3, 6)

// How to handles errorin Future 











//Find Users Who Logged In Consecutively for 3 Days
val df = Seq(
  ("user1", "2024-01-01"),
  ("user1", "2024-01-02"),
  ("user1", "2024-01-03"),
  ("user1", "2024-01-05"),
  ("user2", "2024-01-01"),
  ("user2", "2024-01-03"),
  ("user2", "2024-01-04")
).toDF("user", "login_date")
  .withColumn("login_date", to_date($"login_date"))

import org.apache.spark.sql.expressions.Window

val win = Window.partitionBy("user").orderBy("login_date")

val withRowNum = df.withColumn("row_num", row_number().over(win))
  .withColumn("grp", expr("date_sub(login_date, row_num)"))

val grouped = withRowNum.groupBy("user", "grp")
  .agg(count("*").alias("consecutive_days"))
  .filter($"consecutive_days" >= 3)

grouped.select("user", "consecutive_days").show()


//Pivot sales per product and region, sum quantity and revenue


val df = Seq(
  ("North", "A", 10, 100),
  ("North", "B", 5, 200),
  ("South", "A", 8, 160),
  ("South", "B", 7, 140)
).toDF("region", "product", "qty", "revenue")

val pivoted = df.groupBy("product").pivot("region").agg(sum("qty").alias("qty"),sum("revenue").alias("revenue"))
pivoted.show()

//You have a column with comma-separated IDs. Explode, join with master, then aggregate counts

val users = Seq(
  ("u1", "1,2"),
  ("u2", "2,3")
).toDF("user_id", "interest_ids")

val interests = Seq(
  (1, "Spark"), (2, "Scala"), (3, "Kafka")
).toDF("id", "interest")

val exploded = users
  .withColumn("id", explode(split($"interest_ids", ",")))
  .withColumn("id", $"id".cast("int"))
  .join(interests, "id")
  .groupBy("interest")
  .agg(countDistinct("user_id").alias("user_count"))

exploded.show()

//From a user activity log, get the latest activity per user.

val logs = Seq(
  ("u1", "2024-01-01", "login"),
  ("u1", "2024-01-03", "purchase"),
  ("u2", "2024-01-02", "logout")
).toDF("user", "event_date", "event")
  .withColumn("event_date", to_date($"event_date"))

val win = Window.partitionBy("user").orderBy(desc("event_date"))

val latest = logs.withColumn("rn", row_number().over(win))
  .filter($"rn" === 1)
  .drop("rn")

latest.show()

//For each user, calculate days since previous login.
val df = Seq(
  ("u1", "2024-01-01"),
  ("u1", "2024-01-05"),
  ("u1", "2024-01-10"),
  ("u2", "2024-02-01"),
  ("u2", "2024-02-03")
).toDF("user", "login_date")
  .withColumn("login_date", to_date($"login_date"))

import org.apache.spark.sql.expressions.Window

val win = Window.partitionBy("user").orderBy("login_date")

val withLag = df.withColumn("prev_login", lag("login_date", 1).over(win))
  .withColumn("days_since_last", datediff($"login_date", $"prev_login"))

withLag.show()


//Compare two versions of a DataFrame and find changed rows.

val oldDF = Seq((1, "John", 5000), (2, "Alice", 6000)).toDF("id", "name", "salary")
val newDF = Seq((1, "John", 5500), (2, "Alice", 6000)).toDF("id", "name", "salary")

val changes = oldDF.as("old")
  .join(newDF.as("new"), Seq("id"), "inner")
  .filter($"old.salary" =!= $"new.salary")

changes.select($"id", $"old.salary".as("old_salary"), $"new.salary".as("new_salary")).show()

//Group by department and list all employee names in a comma-separated string.
val df = Seq(
  ("HR", "John"),
  ("HR", "Alice"),
  ("IT", "Bob")
).toDF("dept", "name")

val grouped = df.groupBy("dept")
  .agg(collect_list("name").as("names"))
  .withColumn("name_csv", concat_ws(",", $"names"))

grouped.select("dept", "name_csv").show()

//Aggregate Sales by Product Category-- we have two csv files sales and products

val salesDF = spark.read.option("header", "true").csv("path/to/sales.csv")
  .withColumn("amount", $"amount".cast("double"))

val prodDF = spark.read.option("header", "true").csv("path/to/products.csv")

val joined = salesDF.join(prodDF, "product_id")
val result = joined.groupBy("category").agg(sum("amount").alias("total_sales"))

result.show()

//suppose we have a transaction data . We need to get the latest transaction per user 

val df = spark.read.option("header", "true").csv("path/to/transactions.csv")
  .withColumn("date", to_date($"date", "yyyy-MM-dd"))
  .withColumn("amount", $"amount".cast("double"))

import org.apache.spark.sql.expressions.Window

val win = Window.partitionBy("user_id").orderBy($"date".desc)

val latest = df.withColumn("rn", row_number().over(win))
  .filter($"rn" === 1)
  .drop("rn")

latest.show()

//we have sales dataset . we need to find out top 2 revenue of each product (schema - product,date.revenue)

val df = spark.read.option("header", "true").csv("path/to/sales_log.csv")
  .withColumn("date", to_date($"date"))
  .withColumn("revenue", $"revenue".cast("double"))

val win = Window.partitionBy("product").orderBy($"revenue".desc)

val top2 = df.withColumn("rank", row_number().over(win))
  .filter($"rank" <= 2)

top2.show()

//we have a customer data set . we need to detect validate no null in name and email must contain @ and .

val df = spark.read.option("header", "true").csv("path/to/customer_data.csv")

val invalid = df.filter($"name".isNull || !$"email".rlike("^[^@]+@[^@]+\\.[^@]+$"))

invalid.show()

//Slowly changing dimension SCD type 2 implementation using spark scala . we are getting 2 files employee_update and and employee_dim
//schema :
//employee_update: emp_id,location,updated_on
//employee_dim:emp_id,location,effective_from,effective_to,is_current

val updates = spark.read.option("header", "true").csv("path/to/employee_updates.csv")
  .withColumn("updated_on", to_date($"updated_on"))

val dim = spark.read.option("header", "true").csv("path/to/employee_dim.csv")
  .withColumn("effective_from", to_date($"effective_from"))
  .withColumn("effective_to", to_date($"effective_to"))

val joined = updates.as("u").join(dim.filter($"is_current" === "true").as("d"),
  $"u.emp_id" === $"d.emp_id" && $"u.location" =!= $"d.location")

val closedOld = joined.select(
  $"d.emp_id",
  $"d.location",
  $"d.effective_from",
  $"u.updated_on".alias("effective_to"),
  lit("false").alias("is_current")
)

val newRecords = joined.select(
  $"u.emp_id",
  $"u.location",
  $"u.updated_on".alias("effective_from"),
  lit("9999-12-31").cast("date").alias("effective_to"),
  lit("true").alias("is_current")
)

val result = dim.union(closedOld).union(newRecords)
result.show()








