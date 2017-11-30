# tcifbigdata
TCIF Big Data stand-alone applicatie

De code in deze respository is gebaseerd op de Hadoop cursus van Surf/Sara.

Je kunt de code rechtstreeeks draaien in bijvoorbeeld IntelliJ; of met Maven een JAR maken die met behulp van Hadoop gedraaid kan worden.

Voorbeeld van een run door eerst een JAR te maken en dan Hadoop aan te roepen:
```
RoelantardesMBP:hadoop roelant$ ls
hadoop-2.7.2  tcifbigdata
RoelantardesMBP:hadoop roelant$ cd tcifbigdata/code/wordcount/
RoelantardesMBP:wordcount roelant$ mvn package
RoelantardesMBP:wordcount roelant$ cd ../../..
RoelantardesMBP:hadoop roelant$ cd hadoop-2.7.2/
RoelantardesMBP:hadoop-2.7.2 roelant$ echo "one two three red red red blue yellow" > input/input.txt
RoelantardesMBP:hadoop-2.7.2 roelant$ bin/hadoop jar ../tcifbigdata/code/wordcount/target/wordcount-0.0.1-SNAPSHOT.jar nl/hu/hadoop/wordcount/WordCount input output
RoelantardesMBP:hadoop-2.7.2 roelant$ cat output/part-r-00000 
blue	1
one	1
red	3
three	1
two	1
yellow	1
RoelantardesMBP:hadoop-2.7.2 roelant$ 
```

