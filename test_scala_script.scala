import org.apache.spark.sql._

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

sc.setLogLevel("ERROR")

val rdd1 = sc.parallelize(Seq(
                        ("math",    55),
                        ("math",    56),
                        ("english", 57),
                        ("english", 58),
                        ("science", 59),
                        ("science", 54)))
val df=rdd1.toDF("Subject","rank")
df.registerTempTable("mydf")

val str="""
          select
                  *
                  from mydf
        """
sqlContext.sql(str).show
