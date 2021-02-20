import csv

from collections import defaultdict
from faker import Factory


class Anonymise(object):
    @staticmethod
    def anonymise_rows(rows):
        """
        :param rows:
        :return:
        """
        fake = Factory.create('en_AU')

        first_name = defaultdict(fake.first_name)
        last_name = defaultdict(fake.last_name)
        address = defaultdict(fake.address)

        for row in rows:
            row["first_name"] = first_name[row["first_name"]]
            row["last_name"] = last_name[row["last_name"]]
            row["address"] = address[row["address"]]

            yield row

    @staticmethod
    def anonymise(source, target):
        """
        The source argument is a path to a CSV file containing data to anonymize,
        while target is a path to write the anonymised CSV data to.
        :param source:
        :param target:
        :return:
        """
        f = open(source, 'r')
        o = open(target, 'w')
        # Use the DictReader to easily extract fields
        reader = csv.DictReader(f)
        r = ['first_name', 'last_name', 'address', 'date_of_birth']
        writer = csv.DictWriter(o, reader.fieldnames)

        # Read and anonymize data, writing to target file.
        for row in Anonymise.anonymise_rows(reader):
            writer.writerow(row)