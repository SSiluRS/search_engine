import sqlite3
import datetime
import requests
import bs4

import re


def getTextOnly(soup):
    text = ""
    for elem in soup.find_all(text=True):
        text += re.sub(r"[^a-zA-ZА-Яа-я ]", "", elem) + " "
    return text


def separateWords(text: str):
    text = set(text.split())
    return text


class Crawler:

    def __init__(self, db_file_name):
        print("Конструктор")
        self.connection = sqlite3.connect(db_file_name)
        pass

    def __del__(self):
        print("Деструктор")
        self.connection.commit()
        self.connection.close()
        pass

    def initDB(self):
        print("Создать пустые таблицы с необходимой структурой")

        curs = self.connection.cursor()

        # 1. Таблица wordlist -----------------------------------------------------
        sqlDropWordlist = """DROP TABLE   IF EXISTS    wordlist;  """
        print(sqlDropWordlist)
        curs.execute(sqlDropWordlist)

        sqlCreateWordlist = """
            CREATE TABLE   IF NOT EXISTS   wordlist (
                rowid  INTEGER   PRIMARY KEY   AUTOINCREMENT, -- первичный ключ
                word TEXT   NOT NULL, -- слово
                isFiltred INTEGER     -- флаг фильтрации
            );
        """
        print(sqlCreateWordlist)
        curs.execute(sqlCreateWordlist)

        # 2. Таблица urllist -------------------------------------------------------
        sqlDropURLlist = """DROP TABLE   IF EXISTS    urllist;  """
        print(sqlDropURLlist)
        curs.execute(sqlDropURLlist)

        sqlCreateURLlist = """
            CREATE TABLE   IF NOT EXISTS   urllist (
                rowid  INTEGER   PRIMARY KEY   AUTOINCREMENT, -- первичный ключ
                url TEXT -- адрес
            );
        """
        print(sqlCreateURLlist)
        curs.execute(sqlCreateURLlist)

        # 3. Таблица wordlocation ----------------------------------------------------
        sqlDropURLlist = """DROP TABLE   IF EXISTS    wordlocation;  """
        print(sqlDropURLlist)
        curs.execute(sqlDropURLlist)

        sqlCreateURLlist = """
            CREATE TABLE   IF NOT EXISTS   wordlocation (
                rowid  INTEGER   PRIMARY KEY   AUTOINCREMENT,
                "word_id"	INTEGER,
                "URL_id"	INTEGER,
                "location"	INTEGER,
                FOREIGN KEY("word_id") REFERENCES "wordList"("rowId"),
                FOREIGN KEY("URL_id") REFERENCES "urllist"("rowId")
                );
        """
        print(sqlCreateURLlist)
        curs.execute(sqlCreateURLlist)
        # 4. Таблица linkbeetwenurl --------------------------------------------------
        sqlDropURLlist = """DROP TABLE   IF EXISTS    linkbeetwenurl;  """
        print(sqlDropURLlist)
        curs.execute(sqlDropURLlist)

        sqlCreateURLlist = """
            CREATE TABLE   IF NOT EXISTS   linkbeetwenurl (
            rowid  INTEGER   PRIMARY KEY   AUTOINCREMENT,
            "fromURL_id"	INTEGER,
            "toURL_id"	INTEGER,
            FOREIGN KEY("fromURL_id") REFERENCES "urllist"("rowId")
            );
        """
        print(sqlCreateURLlist)
        curs.execute(sqlCreateURLlist)
        # 5. Таблица linkwords -------------------------------------------------------
        sqlDropURLlist = """DROP TABLE   IF EXISTS    linkwords;  """
        print(sqlDropURLlist)
        curs.execute(sqlDropURLlist)

        sqlCreateURLlist = """
            CREATE TABLE   IF NOT EXISTS   linkwords (
                rowid  INTEGER   PRIMARY KEY   AUTOINCREMENT,
                "word_id"	INTEGER,
                "link_id"	INTEGER,
                FOREIGN KEY("word_id") REFERENCES "wordList"("rowId"),
                FOREIGN KEY("link_id") REFERENCES "urllist"("rowId")
                );
        """
        print(sqlCreateURLlist)
        curs.execute(sqlCreateURLlist)

        self.connection.commit()

    def crawl(self, url_list: list, max_depth=4):

        counter = 0  # счетчик обработанных страниц
        for currDepth in range(0, max_depth):
            print("===========Глубина обхода ", currDepth, "=====================================")
            nextUrlSet = set()

            # Вар.2. обход НЕСКОЛЬКИХ url на текущей глубине
            for num in range(0, 40):

                print(f"NUM = {num}")
                if num > len(url_list) - 1:
                    break
                if counter % 10 == 0:
                    print(f"Count = {counter}")
                    self.sizeTable("linkbeetwenurl")
                    self.sizeTable("wordlocation")
                    self.sizeTable("wordlist")
                    self.sizeTable("urllist")
                    self.sizeTable("linkwords")

                if counter == 100:
                    return
                url = url_list[num]

                counter += 1
                curentTime = datetime.datetime.now().time()

                try:
                    if url.startswith("https://www.facebook") or url.startswith("https://twitter.com/"):
                        counter -= 1
                        continue
                    print("{}/{} {} Попытка открыть {} ...".format(counter, len(url_list), curentTime, url))

                    html_doc = requests.get(url).text

                    soup = bs4.BeautifulSoup(html_doc, "html.parser")
                    if soup.title.text == "403 Forbidden":
                        raise Exception(soup.title.text)
                    print(" ", soup.title.text)
                except Exception as e:
                    print(e)
                    counter -= 1
                    continue

                listUnwantedItems = ['script', 'style']
                for script in soup.find_all(listUnwantedItems):
                    script.decompose()

                self.addIndex(soup, url)

                linksOnCurrentPage = soup.find_all('a')

                for tagA in linksOnCurrentPage:

                    if 'href' in tagA.attrs:
                        nextUrl = tagA.attrs['href']
                        if len(nextUrl) > 1 and nextUrl.startswith("/"):
                            nextUrl = url + nextUrl[1:]
                        if nextUrl.startswith("http"):
                            nextUrlSet.add(nextUrl)
                            text = re.sub(r'[^a-zA-Zа-яА-Я ]', '', tagA.get_text())
                            if text != "":
                                self.addLinkRef(url, nextUrl, separateWords(text))
                    else:
                        continue

            self.connection.commit()
            url_list = list(nextUrlSet)

    def isIndexedURL(self, url: str):

        curs = self.connection.cursor()

        sql = "select * from urllist WHERE URL='{}'".format(url)
        curs.execute(sql)

        res = curs.fetchall()
        if not len(res):
            return False
        sql = "select * from wordlocation where URL_id='{}'".format(res[0][0])
        curs.execute(sql)
        res = curs.fetchall()
        if len(res) == 0:
            return False
        return True

    def addIndex(self, soup, url: str):
        print("      addIndex")
        if self.isIndexedURL(url):
            return

        words = separateWords(getTextOnly(soup))

        url_id = self.getEntryId("urllist", "URL", url)

        for i, currentWord in enumerate(words):
            word_id = self.getEntryId("wordlist", "word", currentWord)
            sql = f"insert into wordlocation (word_id, URL_id, location) VALUES ({word_id},{url_id},{i})"
            self.connection.cursor().execute(sql)

    def printTable(self, table_name):
        cur = self.connection.cursor()
        sql = f"select * from '{table_name}'"
        result = cur.execute(sql)
        print(f"Table {table_name}:")
        print(result.fetchall())

    def sizeTable(self, table_name):
        cur = self.connection.cursor()
        sql = f"select count(*) from '{table_name}'"
        result = cur.execute(sql)
        print(f"{result.fetchone()[0]}")

    def addLinkRef(self, url_from, url_to, words):
        cur = self.connection.cursor()
        sql = f"select rowid from urllist where url='{url_from}'"
        url_from_id = cur.execute(sql).fetchall()[0][0]
        url_to_id = self.getEntryId("urllist", 'URL', url_to)
        sql = f"insert into linkbeetwenurl (fromURL_id, toURL_id) VALUES ({url_from_id}, {url_to_id})"
        linkId = cur.execute(sql).lastrowid

        for word in words:
            sql = f"select rowid from wordlist where word='{word}'"
            result = cur.execute(sql).fetchall()
            if len(result):
                wordId = result[0][0]
                sql = f"insert into linkwords (word_id, link_id) VALUES ({wordId},{linkId})"
                cur.execute(sql)
            else:
                url_from = str(url_from)
                err = f"нет слова {word} по ссылке {url_from}"
                print(err)

    def getEntryId(self, table_name: str, field_name: str, value: str):
        cur = self.connection.cursor()
        sql = f"select rowid from {table_name} where {field_name}='{value}'"
        cur.execute(sql)
        result = cur.fetchall()
        if not len(result):
            sql = f"insert into '{table_name}' ({field_name}) values ('{value}')"
            result = cur.execute(sql)
            return result.lastrowid
        else:
            return result[0][0]


def main():
    myCrawler = Crawler("search_engine.db")
    myCrawler.initDB()

    ulrList = list()
    ulrList.append("https://www.roscosmos.ru/")
    ulrList.append("https://lenta.ru/")
    ulrList.append("https://habr.com/")

    myCrawler.crawl(ulrList)


main()
