# coding: utf-8
import re
import sys
import urllib.request
from multiprocessing import Pool
from bs4 import BeautifulSoup
from slugify import slugify
from abc import ABC, abstractmethod
import psycopg2

class AbstractParser(ABC):
    @abstractmethod
    def __init__(self, _url):
        self.props = []
        self.url = _url

        # Connect to db
        try:
            self.conn = psycopg2.connect(dbname="rooms-japan", host="localhost", user="tiphaine", password="tiphsolange")
            self.cur = self.conn.cursor()
        except psycopg2.OperationalError as e:
            print("Could not connect to database: {0}").format(e)
            sys.exit(1)


    def get_ward(self, string):
        romaji_wards = [
            "Chiyoda",
            "Chūō",
            "Minato",
            "Shinjuku",
            "Bunkyō",
            "Taitō",
            "Sumida",
            "Kōtō",
            "Shinagawa",
            "Meguro",
            "Ōta",
            "Setagaya",
            "Shibuya",
            "Nakano",
            "Suginami",
            "Toshima",
            "Kita",
            "Arakawa",
            "Itabashi",
            "Nerima",
            "Adachi",
            "Katsushika",
            "Edogawa"
        ]
        kanji_wards = [
            "千代田区",
            "中央区",
            "港区",
            "新宿区",
            "文京区",
            "台東区",
            "墨田区",
            "江東区",
            "品川区",
            "目黒区",
            "大田区",
            "世田谷区",
            "渋谷区",
            "中野区",
            "杉並区",
            "豊島区",
            "北区",
            "荒川区",
            "板橋区",
            "練馬区",
            "足立区",
            "葛飾区",
            "江戸川区"
        ]
        res = [kanji_wards.index(x) for x in kanji_wards if x in string]
        if len(res) == 0:
            return "n/a"
        assert(len(res) <= 1)
        i = res[0]
        return romaji_wards[i]

    @abstractmethod
    def parse(self): 
        raise NotImplementedError("parse() must be overriden.")

    def print(self, out):
        with open(out, "w") as f:
            prop_keys = self.props[0].keys()
            f.write("\t".join(prop_keys) + "\n")
            for key in self.props:
                f.write("\t".join(map(str, [key[x] for x in prop_keys])) + "\n")

    def get_key(self, _key):
        # TODO: If key not in dict ?
        return [i[_key] for i in self.props] 

    @staticmethod
    def convert_price(string):
        return int(re.sub('¥|,', '', string))

    def convert_surface(self, surf):
        return float(surf.replace(u'm\xb2', ''))

    def get_appt_info(self, apartment):
        raise NotImplementedError("get_appt_info() must be overriden.")

    def get_page_number(self):
        raise NotImplementedError("get_page_number() must be overriden.")

    def load(self, infile):
        header = True
        with open(infile) as f:
            for line in f:
                if header:
                    # Read keys
                    keys = line.split()
                    header = False
                else:
                    entry = {}
                    contents = [x.strip() for x in line.split()]
                    for i in range(0, len(keys)):
                        if keys[i] == "ward":
                            entry[keys[i]] = str(contents[i])
                        else:
                            entry[keys[i]] = float(contents[i])
                    self.props.append(entry)

class Agharta(AbstractParser):
    def __init__(self, _url):
        super().__init__(_url)
        self.table_cols = ['floor', 'max_floor', 'size', 'rent', 'maintenance_fee', 'deposit', 'key_money', 'layout', 'year_built', 'nearest_station', 'location', 'url']

    def parse(self):
        page_number = self.get_page_number()
        finres = []
        # page_number = 1

        # Extract all pages
        for i in range(1, page_number + 1):
            sys.stderr.write("...Handling page " + str(i) + " / " + str(page_number) + "\n")

            url = self.url + "&page=" + str(i)
            with urllib.request.urlopen(url) as f:
                # f = open("test.html")
                html_doc = f.read()

                soup = BeautifulSoup(html_doc, "html.parser")

                properties = soup.find_all("div", class_="property-listing")
                titles = [ a.find("div", class_="listing-title") for a in properties ]
                # Get links        
                urls = [ x.find("a", href=True)['href'] for x in titles ]
                agents = 5
                chunksize = 3
                data = [i for i in range(0,15)]
                with Pool(processes=agents) as pool: 
                    res = pool.map(Agharta.insert_appt, urls, chunksize)
                finres.extend(res)
                print(len(finres))
        for p in finres:
            print(p)
            self.cur.execute("""
            INSERT INTO dwellings
            (id, """ + ','.join(self.table_cols) +""")
            VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, [ p[i] for i in self.table_cols])
        self.conn.commit()

    @staticmethod
    def insert_appt(url_apartment):
        print(url_apartment)
        with urllib.request.urlopen("http://www.realestate.co.jp" + url_apartment) as appt:
            p = Agharta.get_appt_info(appt, url_apartment)
            p['url'] = "http://www.realestate.co.jp" + url_apartment
            return p

    @staticmethod
    def get_appt_info(appt, url):
        prop = {}
        prop_db = {'floor':0, 'max_floor':0}
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
        
        return prop_db

    def get_page_number(self):
        # return 3
        with urllib.request.urlopen(self.url) as f:
            # html_doc = open("test.html").read()
            html_doc = f.read()

            soup = BeautifulSoup(html_doc, "html.parser")

            page_number_text = [x.get_text() for x in soup.find("ul", class_="paginator").find_all("li") if u'of' in x.get_text()]
            assert(len(page_number_text) > 0)

            return int(int(page_number_text[0].split(u'of')[1]) / 15) + 1


if __name__ == '__main__':
    print("Tests")
    ag = Agharta("https://www.realestate.co.jp/agharta/en/rent?page=1")
    ag.parse()
