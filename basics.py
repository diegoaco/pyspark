#Create a dataframe:
data = [("James","M",60000),("Michael","M",70000),
        ("Robert",None,400000),("Maria","F",500000),
        ("Jen","Q",None), ("Mark", "M", 50000), ("Kate", "F", 120000)]
columns = ["name","gender","salary"]
df = spark.createDataFrame(data = data, schema = columns)

#Display the dataframe:
df.show()

#Add a new column 'gender2', based on the 'gender' column:
df1 = df.withColumn("gender2", when(df.gender == "M", "Male").when(df.gender == "F", "Female").when(df.gender.isNull(), "").otherwise("Other"))

#Add a column using .lit, setting all values to 0.2:
df2 = df1.withColumn("bonus_per", lit(0.2))

#Add two columns at once, 'bonus' and 'total':
df3 = df2.withColumn("bonus", df2.salary*df2.bonus_per).withColumn("total", df2.salary + df2.salary*df2.bonus_per)

#Add another column, 'total_after_tax':
df4 = df3.withColumn("total_after_tax", when(df3.total >= 100000, df3.total*0.75).otherwise(df3.total*0.8))

#%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

#Create a second dataframe:
data = [("0023", "James","Peterson"),("0054", "Michael","Fernandez"),
        ("0091", "Robert", "Johnson"),("0070", "Maria","Fuentes"),
        ("0026", "Jen","Quimby"), ("0077", "Mark", "Straus"), ("0014", "Kate", "Towers")]
columns = ["id","name","surname"]
df_new = spark.createDataFrame(data = data, schema = columns)

#Do an inner join between the two dataframes:
df_joint = df_new.alias("b").join(df4.alias("a"), df_new.name == df4.name, "inner").drop(df_new.name)
