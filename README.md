# ceng495-hw3
This is the third homework of Ceng495, Cloud Computing course in METU Computer Engineering department.

## Converting csv to tsv
A little python script, that takes a .csv file and converts it to .tsv file 
### Usage
```
$ python3 csvToTsv.py
$ Enter the name of your csv file.
$ Script creates a tsv file with the same name.
$ songs_normalize.csv ---> songs_normalize.tsv 
```
## Compilation
```
$ hadoop com.sun.tools.javac.Main Hw3.java
$ jar cf Hw3.jar Hw3*.class
```

## Running
__Instead of .csv, pass tsv file as input.__
```
hadoop jar Hw3.jar Hw3 total <input.tsv> output_total
hadoop jar Hw3.jar Hw3 average <input.tsv> output_average
hadoop jar Hw3.jar Hw3 popular <input.tsv> output_popular
hadoop jar Hw3.jar Hw3 explicitlypopular <input.tsv> output_explicitlypopular
hadoop jar Hw3.jar Hw3 dancebyyear <input.tsv> output_dancebyyear
```

