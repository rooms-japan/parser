# coding: utf-8
import re
import sys
from abc import ABC, abstractmethod
import urllib.request
from multiprocessing import Pool
from bs4 import BeautifulSoup
from slugify import slugify
import psycopg2

class AbstractParser(ABC):
    @abstractmethod
    def __init__(self, _url):
        self.props = []
        self.url = _url

    @abstractmethod
    def parse(self):
        raise NotImplementedError("parse() must be overriden.")

    def print(self, out):
        with open(out, "w") as f:
            prop_keys = self.props[0].keys()
            f.write("\t".join(prop_keys) + "\n")
            for key in self.props:
                f.write("\t".join(map(str, [key[x] for x in prop_keys])) + "\n")

    @staticmethod
    def convert_price(string):
        """
            Converts a string price in yens into an integer.
            e.g. given the string "20,000¥", returns 20000
        """
        return int(re.sub('¥|,', '', string))

    def get_appt_info(self, apartment):
        raise NotImplementedError("get_appt_info() must be overriden.")

    def get_page_number(self):
        raise NotImplementedError("get_page_number() must be overriden.")

class Agharta(AbstractParser):
    def __init__(self, _url):
        super().__init__(_url)
        self.table_cols = ['floor', 'max_floor', 'size', 'rent',\
                           'maintenance_fee', 'deposit', 'key_money',\
                           'layout', 'year_built', 'nearest_station',\
                           'location', 'url']

    def get_table_cols(self):
        """
            Returns the names of the columns to be inserted in the database.
        """
        return self.table_cols

    def parse(self):
        """
            Parses the url, extracts all listings and parallelize listing parsing.

            Returns the list of parsed listings
        """
        page_number = self.get_page_number()
        prop_list = []

        # Extract all pages
        for i in range(1, page_number + 1):
            sys.stderr.write("...Handling page " + str(i) + " / " + str(page_number) + "\n")

            url = self.url + "&page=" + str(i)
            with urllib.request.urlopen(url) as f:
                soup = BeautifulSoup(f.read(), "html.parser")

                # Extract properties on page without taking the "Featured" listing if it exists
                properties = [p for p in \
                              soup.find_all("div", class_="property-listing")\
                              if not p.find("div", class_="listing-featured")]
                titles = [a.find("div", class_="listing-title") for a in properties]

                # Get links to detailed apartment page
                urls = [x.find("a", href=True)['href'] for x in titles]
                agents = 5
                chunksize = 3
                with Pool(processes=agents) as pool:
                    res = pool.map(Agharta.get_appt_info, urls, chunksize)
                prop_list.extend(res)
        return prop_list

    @staticmethod
    def get_appt_info(url):
        """
            Given the url of a detailed listing, extract the relevant information.

            Returns a dictionary.
        """
        prop = {}
        prop_db = {'floor': 0, 'max_floor': 0}
        with urllib.request.urlopen("http://www.realestate.co.jp" + url) as appt:
            appt_page = BeautifulSoup(appt.read(), "html.parser")

            dl_data = appt_page.find_all("dd")
            dt_data = appt_page.find_all("dt")
            for dlitem, dtitem in zip(dl_data, dt_data):
                # Do all "manual" parsing here
                prop[slugify(dtitem.string)] = dlitem.getText().strip()
            for p in prop:
                if p == 'floor':
                    floors = re.findall(r'\d+', prop[p])
                    if len(floors) == 1:
                        floors.append(floors[0])
                    (prop_db['floor'], prop_db['max_floor']) = map(int, floors)
                if p == 'size':
                    prop_db[p] = float(re.findall(r'\d+\.\d+|\d+', prop[p])[0])
                if p == 'nearest-station':
                    prop_db['nearest_station'] = int(str(min(re.findall(r'\d+', prop[p]))))
                if 'year-built' in p:
                    prop_db['year_built'] = int(prop[p])

            prop_db['location'] = prop['location']
            prop_db['layout'] = prop['layout']
            prop_db['rent'] = Agharta.convert_price(prop['rent'])
            prop_db['maintenance_fee'] = Agharta.convert_price(prop['maintenance-fee'])
            prop_db['deposit'] = Agharta.convert_price(prop['deposit'])
            prop_db['key_money'] = Agharta.convert_price(prop['key-money'])
            prop_db['url'] = "http://www.realestate.co.jp" + url

        return prop_db

    def get_page_number(self):
        """
            Return the number of pages to parse, assuming 15 listings per page
        """
        with urllib.request.urlopen(self.url) as f:
            html_doc = f.read()

            soup = BeautifulSoup(html_doc, "html.parser")

            page_number_text = [x.get_text() for x in\
                                soup.find("ul", class_="paginator").find_all("li")\
                                if u'of' in x.get_text()]
            assert len(page_number_text) > 0

            return int(int(page_number_text[0].split(u'of')[1]) / 15) + 1


if __name__ == '__main__':
    # Connect to db
    try:
        conn = psycopg2.connect(dbname="rooms-japan",\
                                host="localhost",\
                                user="tiphaine",\
                                password="tiphsolange")
        cur = conn.cursor()
    except psycopg2.OperationalError as e:
        print("Could not connect to database: {0}").format(e)
        sys.exit(1)

    ag = Agharta("https://www.realestate.co.jp/rent/listing?prefecture=JP-13&page=1")
    table_cols = ag.get_table_cols()

    properties_listing = ag.parse()
    print("Parsing finished. Inserting in database...")
    for prop in properties_listing:
        cur.execute("""
        INSERT INTO dwellings
        (id, """ + ','.join(table_cols) +""")
        VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """, [ prop[i] for i in table_cols])
    conn.commit()
    print("Finished.  Happy exploration :)")
