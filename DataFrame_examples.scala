 val df = sc.parallelize(Seq((201601,"a"),(201602, "b"),(201603, "c"))).toDF("col1", "col2")
 val f=df.select($"col1").limit(1).collect
 val nu:Int=f(0).getInt(0) 
