#Create a dataframe:
data = [("James","M",60000),("Michael","M",70000),
        ("Robert",None,400000),("Maria","F",500000),
        ("Jen","Q",None), ("Mark", "M", 50000), ("Kate", "F", 120000)]
columns = ["name","gender","salary"]
df = spark.createDataFrame(data = data, schema = columns)
