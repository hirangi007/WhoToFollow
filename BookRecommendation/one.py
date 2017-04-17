import csv

with open('ratings.csv') as f:
    reader = csv.reader(f,delimiter=';')
    rows = list(reader)
    

    
with open('ratings4.csv','wb') as f:
    spamwriter = csv.writer(f, delimiter=',', quoting=csv.QUOTE_MINIMAL)
    cnt = 1;
    for row in rows:
        if (row[0] != "ISBN") and (cnt < 1000):
            spamwriter.writerow([row[0], str(row[1]), row[2]])
            cnt = cnt + 1