# coding: utf-8
import ipdb
import re
import sys
import urllib.request
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod

class AbstractParser(ABC):
    @abstractmethod
    def __init__(self, _url):
        self.props = []
        self.url = _url


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
            ipdb.set_trace()
            f.write("\t".join(prop_keys) + "\n")
            for key in self.props:
                f.write("\t".join(map(str, [key[x] for x in prop_keys])) + "\n")

    def get_key(self, _key):
        # TODO: If key not in dict ?
        return [i[_key] for i in self.props] 

    def convert_price(self, string):
        rent = string.split(u'\u4e07')
        man = int(rent[0])
        yen = rent[1].split(u'\u5186')[0]
        if yen == "":
            yen = 0
        else:
            yen = int(yen)
        if (man*10000 + yen) > 150000:
            ipdb.set_trace()
        return (man*10000 + yen)

    def convert_surface(self, surf):
        return float(surf.replace(u'm\xb2', ''))

    def get_appt_info(self, apartment):
        raise NotImplementedError("get_appt_info() must be overriden.")

    def get_page_number(self):
        raise NotImplementedError("get_page_number() must be overriden.")

    def load(self, infile):
        header = True
        with open(infile) as f:
            for line in f.readlines():
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

    def parse(self):
        page_number = self.get_page_number()
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
        
                for apartment in properties:
                    self.props.append(self.get_appt_info(apartment))
            # sys.stderr.write(str(len(props)) + "\n")

    def get_appt_info(self, apartment):
        prop = {}
        title = apartment.find("div", class_="listing-title")
        prop["ward"] = self.get_ward(title.get_text())

        right_col = apartment.find("div", class_="listing-right-col")
        price = right_col.find_all("div", class_="listing-item")[1]
        infos = right_col.find_all("div", class_="listing-item")

        for info in infos:
            if u'\u8cc3\u6599' in info.get_text():
                # Rent price
                prop["rent"] = self.convert_price(info.get_text().split("\n")[1].replace("\t", ""))
            elif u'\u9762\u7a4d' in info.get_text():
                # Surface
                prop["surface"] = self.convert_surface(info.get_text().split("\n")[1].replace("\t", ""))
        more_infos = apartment.find("div", class_="listing-info").find_all("div", class_="listing-item")
        for info in more_infos:
            if u'\u7ba1\u7406\u8cbb' in info.get_text():
                # Administration Fee
                t = info.get_text().split("\n")[-1]
                rep = {u'/':u'', u',':u'', u'\u6708':u'',u'\xa5':u'', u'\t':'', u' ':u''}
                rep = dict((re.escape(k), v) for k, v in rep.items())
                pattern = re.compile("|".join(rep.keys()))
                t = pattern.sub(lambda m: rep[re.escape(m.group(0))], t)
                prop["admin_fee"] = int(t)
            elif u'\u5206' in info.get_text():
                # Walk Time
                walk_text = info.get_text().strip().split("\n")[-1]
                rep = {u'\uff08':u'', u'\uff09':u'', u'\u5f92':u'',u'\u5206':u'', u'\u6b69':u'', u'\u30d0\u30b9\u3067':''} # Dernier : bus, retirer ?
                rep = dict((re.escape(k), v) for k, v in rep.items())
                pattern = re.compile("|".join(rep.keys()))
                walk_time = pattern.sub(lambda m: rep[re.escape(m.group(0))], walk_text)
                prop["walk_time"] = int(walk_time)
        return prop

    def get_page_number(self):
        # return 3
        with urllib.request.urlopen(self.url) as f:
            # html_doc = open("test.html").read()
            html_doc = f.read()

            soup = BeautifulSoup(html_doc, "html.parser")

            page_number_text = [x.get_text() for x in soup.find("ul", class_="paginator").find_all("li") if u'\u4ef6' in x.get_text()]
            assert(len(page_number_text) > 0)
            return int(int(page_number_text[0].split(u'\u4ef6')[0]) / 15) + 1

