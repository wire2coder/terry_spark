import pyspark

sc = pyspark.SparkContext('local[*]')
sc.setLogLevel('WARN')

txt = sc.textFile('file:////usr/share/doc/python/copyright')
print("terry was here!!!")
print(txt.count())

python_lines = txt.filter(lambda line: 'python' in line.lower())
print(python_lines.count())
