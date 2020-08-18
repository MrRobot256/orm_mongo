import csv
import re
from pymongo import MongoClient
import pymongo
from datetime import datetime


def read_data(csv_file, db):
    with open(csv_file, encoding='utf8') as csvfile:
        reader = csv.DictReader(csvfile)
        for object in reader:
            object['Цена'] = int(object['Цена'])
            object['Дата'] = datetime.strptime(object['Дата'] + '.20', "%d.%m.%y")
            db.insert_one(object)


def find_cheapest(db):
    asc_collection = db.find().sort('Цена', pymongo.ASCENDING)
    return asc_collection


def find_by_name(name, db):
    for letter in name:
        if not letter.isalpha():
            name = name.replace(letter, '\\' + letter)
    pattern = re.compile(name, re.IGNORECASE)
    return db.find({'Исполнитель': pattern}).sort('Цена', pymongo.ASCENDING)


def find_by_date(start_date, end_date, db):
    # Для корректной работы дата должна быть представлена в формате d.m.y
    concerts_list = []
    start = datetime.strptime(start_date, "%d.%m.%y")
    end = datetime.strptime(end_date, "%d.%m.%y")
    for object in db.find():
        if start <= object['Дата'] <= end:
            concerts_list.append(object)
    return concerts_list


if __name__ == '__main__':
    client = MongoClient()

    homework_db = client['homework']
    concerts_collection = homework_db['concerts']

    print(find_by_date('01.03.20', '30.05.20', concerts_collection))

exit(0)
