import csv

file_name = input("Please enter the name of csv file name: ")

with open(file_name,'r') as csvin, open(file_name[:-4] + ".tsv", 'w') as tsvout:
    csvin = csv.reader(csvin)
    tsvout = csv.writer(tsvout, delimiter='\t')

    for row in csvin:
        tsvout.writerow(row)