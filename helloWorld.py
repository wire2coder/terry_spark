import pyspark

sc = pyspark.SparkContext('local[*]')
sc.setLogLevel('WARN') # this line is a life saver!

txt = sc.textFile('file:////usr/share/doc/python/copyright')
print("terry was here!!!")
print(txt.count())

python_lines = txt.filter(lambda line: 'python' in line.lower())
print(python_lines.count())

# saving the results into a file
with open('helloWolrd_results1.txt', 'w') as file_obj:
  file_obj.write('Number of lines: %s \n' % txt.count())
  file_obj.write('Number of lines with python: %s \n' % python_lines.count() )

  