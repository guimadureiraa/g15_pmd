import json
import csv

def dump_list_to_json(rowlist, x):
    filename='noticias'+str(x)+'.json'
    with open(filename, 'w') as out_json_file:
    # Save each obj to their respective filepath
    # with pretty formatting thanks to `indent=4`
        json.dump(rowlist, out_json_file, indent=4)


fileNews = open('C://Users/gui20/Downloads/spark/news.json', encoding="utf8")
Noticias = json.load(fileNews)

k = 0
rowlist = []
for json_obj in Noticias:
    rowlist.append(json_obj)
    if len(rowlist) == 10000:
        dump_list_to_json(rowlist, k)
        rowlist = []
        k += 1
if len(rowlist) > 0:
    dump_list_to_json(rowlist, k)
