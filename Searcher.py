import sqlite3
import re
import requests
import bs4
import random
class Seacher:

    def dbcommit(self):
        """ Зафиксировать изменения в БД """
        self.con.commit()

    def __init__(self, dbFileName):
        """  0. Конструктор """
        # открыть "соединение" получить объект "сonnection" для работы с БД
        self.con = sqlite3.connect(dbFileName)

    def __del__(self):
        """ 0. Деструктор  """
        # закрыть соединение с БД
        self.con.close()
    
    def getWordsIds(self, queryString):
        """
        Получение идентификаторов для каждого слова в queryString
        :param queryString: поисковый запрос пользователя
        :return: список wordlist.rowid искомых слов
        """

        # Привести поисковый запрос к верхнему регистру
        queryString = queryString.lower()

        # Разделить на отдельные искомые слова
        queryWordsList = queryString.split(" ")

        # список для хранения результата
        rowidList = list()

        # Для каждого искомого слова
        for word in queryWordsList:
            # Сформировать sql-запрос для получения rowid слова, указано ограничение на кол-во возвращаемых результатов (LIMIT 1)
            sql = f"SELECT rowid FROM wordlist WHERE word ='{word}' LIMIT 1; "

            # Выполнить sql-запрос. В качестве результата ожидаем строки содержащие целочисленный идентификатор rowid
            result_row = self.con.execute(sql ).fetchone()

            # Если слово было найдено и rowid получен
            if result_row != None:
                # Искомое rowid является элементом строки ответа от БД (особенность получаемого результата)
                word_rowid = result_row[0]

                # поместить rowid в список результата
                rowidList.append(word_rowid)
                print("  ", word, word_rowid)
            else:
                # в случае, если слово не найдено приостановить работу (генерация исключения)
                raise Exception("Одно из слов поискового запроса не найдено:" + word)

        # вернуть список идентификаторов
        return rowidList

    def getMatchRows(self, queryString):
        """
        Поиск комбинаций из всех искомых слов в проиндексированных url-адресах
        :param queryString: поисковый запрос пользователя
        :return: 1) список вхождений формата (urlId, loc_q1, loc_q2, ...) loc_qN позиция на странице Nго слова из поискового запроса  "q1 q2 ..."
        """

        # Разбить поисковый запрос на слова по пробелам
        queryString = queryString.lower()
        wordsList = queryString.split(' ')

        # получить идентификаторы искомых слов
        wordsidList = self.getWordsIds(queryString)

        #Созать переменную для полного SQL-запроса
        sqlFullQuery = """"""

        # Созать объекты-списки для дополнений SQL-запроса
        sqlpart_Name = list() # имена столбцов
        sqlpart_Join = list() # INNER JOIN
        sqlpart_Condition = list() # условия WHERE

        
        #Конструктор SQL-запроса (заполнение обязательной и дополнительных частей)
        #обход в цикле каждого искомого слова и добавлене в SQL-запрос соответствующих частей
        for wordIndex in range(0,len(wordsList)):

            # Получить идентификатор слова
            wordID = wordsidList[wordIndex]

            if wordIndex ==0:
                # обязательная часть для первого слова
                sqlpart_Name.append("""w0.URL_id    urlid  --идентификатор url-адреса""")
                sqlpart_Name.append("""   , w0.location w0_loc --положение первого искомого слова""")

                sqlpart_Condition.append("""WHERE w0.word_id={}     -- совпадение w0 с первым словом """.format(wordID))
            else:
                # Дополнительная часть для 2,3,.. искомых слов

                if len(wordsList)>=2:
                    # Проверка, если текущее слово - второе и более

                    # Добавить в имена столбцов
                    sqlpart_Name.append(""" , w{}.location w{}_loc --положение следующего искомого слова""".format(wordIndex,wordIndex))

                    #Добавить в sql INNER JOIN
                    sqlpart_Join.append("""INNER JOIN wordlocation w{}  -- назначим псевдоним w{} для второй из соединяемых таблиц
                        on w0.URL_id=w{}.URL_id -- условие объединения""".format(wordIndex, wordIndex, wordIndex))
                    # Добавить в sql ограничивающее условие
                    sqlpart_Condition.append("""  AND w{}.word_id={} -- совпадение w{}... с cоответсвующим словом """.format(wordIndex, wordID, wordIndex ))
                    pass
            pass


        # Объеднение запроса из отдельных частей

        #Команда SELECT
        sqlFullQuery += "SELECT "

        #Все имена столбцов для вывода
        for sqlpart in sqlpart_Name:
            sqlFullQuery+="\n"
            sqlFullQuery += sqlpart

        # обязательная часть таблица-источник
        sqlFullQuery += "\n"
        sqlFullQuery += "FROM wordlocation w0 "

        #часть для объединения таблицы INNER JOIN
        for sqlpart in sqlpart_Join:
            sqlFullQuery += "\n"
            sqlFullQuery += sqlpart

        #обязательная часть и дополнения для блока WHERE
        for sqlpart in sqlpart_Condition:
            sqlFullQuery += "\n"
            sqlFullQuery += sqlpart

        # Выполнить SQL-запроса и извлеч ответ от БД
        print(sqlFullQuery)
        cur = self.con.execute(sqlFullQuery)
        rows = [row for row in cur]

        return rows, wordsidList
    
    def normalizeScores(self, scores, smallIsBetter=0):
     
        resultDict = dict() # словарь с результатом

        vsmall = 0.00001  # создать переменную vsmall - малая величина, вместо деления на 0
        minscore = min(scores.values())  # получить минимум
        maxscore = max(scores.values())  # получить максимум

        # перебор каждой пары ключ значение
        for (key, val) in scores.items():

            if smallIsBetter:
                # Режим МЕНЬШЕ вх. значение => ЛУЧШЕ
                # ранг нормализованный = мин. / (тек.значение  или малую величину)
                resultDict[key] = float(minscore) / max(vsmall, val)
            else:
                # Режим БОЛЬШЕ  вх. значение => ЛУЧШЕ вычислить макс и разделить каждое на макс
                # вычисление ранга как доли от макс.
                # ранг нормализованный = тек. значения / макс.
                resultDict[key] = float(val) / maxscore

        return resultDict

        # Ранжирование. Содержимомое. 1. Частота слов.
    def frequencyScore(self, rowsLoc):
        """
        Расчет количества комбинаций искомых слов
        Пример встречается на странице  q1 - 10 раз,  q2 - 3 раза, Общий ранг страницы = 10*3 = 30 "комбинаций"
        :param rowsLoc: Список вхождений: urlId, loc_q1, loc_q2, .. слов из поискового запроса "q1 q2 ..." (на основе результата getmatchrows ())
        :return: словарь {UrlId1: общее кол-во комбинаций, UrlId2: общее кол-во комбинаций, }
        """
        countsDict = dict()
        for row in rowsLoc:
            urlId = row[0]
            if urlId in countsDict.keys():
                countsDict[urlId]+=1;
            else:
                countsDict[urlId]=1;
        # Создать countsDict - словарь с количеством упоминаний/комбинаций искомых слов -
        # {id URL страницы где встретилась комбинация искомых слов: общее количество комбинаций на странице }

        # поместить в словарь все ключи urlid с начальным значением счетчика "0"

        # Увеличивает счетчик для URLid +1 за каждую встреченную комбинацию искомых слов

        # передать словарь счетчиков в функцию нормализации, режим "чем больше, тем лучше")
        return self.normalizeScores(countsDict, smallIsBetter=0)

    def geturlname(self, id):
        """
        Получает из БД текстовое поле url-адреса по указанному urlid
        :param id: целочисленный urlid
        :return: строка с соответствующим url
        """
        sql = f"select url from urllist where rowid='{id}'"
        result = self.con.execute(sql).fetchone()
        if result != None:
            return result[0]
        # сформировать SQL-запрос вида SELECT url FROM urllist WHERE rowid=
        # выполнить запрос в БД
        # извлечь результат - строковый url и вернуть его
        else:
            raise Exception(f"Не найдена ссылка с id={id}")


    def getSortedList(self, queryString):
        """
        На поисковый запрос формирует список URL, вычисляет ранги, выводит в отсортированном порядке
        :param queryString:  поисковый запрос
        :return:
        """
        rowsLoc, wordsidList = self.getMatchRows(queryString)
        # получить rowsLoc и wordids от getMatchRows(queryString)
        # rowsLoc - Список вхождений: urlId, loc_q1, loc_q2, .. слов из поискового запроса "q1 q2 ..."
        # wordids - Список wordids.rowid слов поискового запроса

        
        m1Scores = self.frequencyScore(rowsLoc)
        # Получить m1Scores - словарь {id URL страниц где встретились искомые слова: вычисленный нормализованный РАНГ}
        # как результат вычисления одной из метрик

        #Создать список для последующей сортировки рангов и url-адресов
        rankedScoresList = list()
        for url, score in m1Scores.items():
            pair = (score, url)
            rankedScoresList.append( pair )

        # Сортировка из словаря по убыванию
        rankedScoresList.sort(reverse=True)

        # Вывод первых N Результатов
        #print("score, urlid, geturlname")
        #for (score, urlid) in rankedScoresList[0:10]:
        #    print ( "{:.2f} {:>5}  {}".format ( score, urlid, self.geturlname(urlid)))
        return rankedScoresList,m1Scores

    def calculatePageRank(self, iterations=5):
        # Подготовка БД ------------------------------------------
        # стираем текущее содержимое таблицы PageRank
        self.con.execute('DROP TABLE IF EXISTS pagerank')
        self.con.execute("""CREATE TABLE  IF NOT EXISTS  pagerank(
                                rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                                urlid INTEGER,
                                score REAL
                            );""")


        # Для некоторых столбцов в таблицах БД укажем команду создания объекта "INDEX" для ускорения поиска в БД
        self.con.execute("DROP INDEX   IF EXISTS wordidx;")
        self.con.execute("DROP INDEX   IF EXISTS urlidx;")
        self.con.execute("DROP INDEX   IF EXISTS wordurlidx;")
        self.con.execute("DROP INDEX   IF EXISTS urltoidx;")
        self.con.execute("DROP INDEX   IF EXISTS urlfromidx;")
        self.con.execute('CREATE INDEX IF NOT EXISTS wordidx       ON wordlist(word)')
        self.con.execute('CREATE INDEX IF NOT EXISTS urlidx        ON urllist(url)')
        self.con.execute('CREATE INDEX IF NOT EXISTS wordurlidx    ON wordlocation(word_id)')
        self.con.execute('CREATE INDEX IF NOT EXISTS urltoidx      ON linkbeetwenurl(toURL_id)')
        self.con.execute('CREATE INDEX IF NOT EXISTS urlfromidx    ON linkbeetwenurl(fromURL_id)')
        self.con.execute("DROP INDEX   IF EXISTS rankurlididx;")
        self.con.execute('CREATE INDEX IF NOT EXISTS rankurlididx  ON pagerank(urlid)')
        self.con.execute("REINDEX wordidx;")
        self.con.execute("REINDEX urlidx;")
        self.con.execute("REINDEX wordurlidx;")
        self.con.execute("REINDEX urltoidx;")
        self.con.execute("REINDEX urlfromidx;")
        self.con.execute("REINDEX rankurlididx;")

        # в начальный момент ранг для каждого URL равен 1
        self.con.execute('INSERT INTO pagerank (urlid, score) SELECT rowid, 1.0 FROM urllist')
        self.dbcommit()

        d = 0.85
        # Цикл Вычисление PageRank в несколько итераций  
        for i in range(iterations):
            print("Итерация %d" % (i))


            # Цикл для обхода каждого  urlid адреса в urllist БД
            sql = "select rowid from urllist"
            url_list = self.con.execute(sql).fetchall()
            for [urlA] in url_list:
                # В цикле обходим все страницы, ссылающиеся на данную urlid
                # SELECT DISTINCT fromid FROM linkbeetwenurl  -- DISTINCT выбрать уникальные значения fromid
                sql = f"SELECT DISTINCT fromURL_id FROM linkbeetwenurl where toURL_id={urlA}"
                urlsLinksToA = self.con.execute(sql).fetchall()
                summ = 0
                for [urlT] in urlsLinksToA:
                    # Находим ранг ссылающейся страницы linkingpr. выполнить SQL-зарпрос
                    prT = self.con.execute(f"select score from pagerank where urlid={urlT}").fetchone()[0]

                    # Находим общее число ссылок на ссылающейся странице linkingcount. выполнить SQL-зарпрос
                    cT = self.con.execute(f"select count(*) from linkbeetwenurl where fromURL_id={urlT}").fetchone()[0]
        #SELECT count (*) 
        #FROM linkbeetwenurl
        #WHERE  fromid = 502               
                    summ += prT/cT
                    # Прибавить к pr вычесленный результат для текущего узла
                
                prA = (1-d) + d * summ
                # выполнить SQL-зарпрос для обновления значения  score в таблице pagerank БД
        #self.con.execute('UPDATE pagerank SET score=%f WHERE urlid=%d' % (pr, urlid))
                self.con.execute(f'UPDATE pagerank SET score={prA} WHERE urlid={urlA}')
            self.dbcommit()
        
        return self.pagerankScore()

    def pagerankScore(self):
        # получить значения pagerank
        scores = self.con.execute(f'select score, urlid from pagerank').fetchall()
        scoreDict = dict()
        for [score, urlid] in scores:
            scoreDict[urlid] = score
        normalizedscores = self.normalizeScores(scoreDict)
        # нормализовать отностительно максимума
        rankedScoresList = list()
        for url, score in normalizedscores.items():
            pair = (score, url)
            rankedScoresList.append( pair )

        # Сортировка из словаря по убыванию
        rankedScoresList.sort(reverse=True)

        # Вывод первых N Результатов
        #print("score, urlid, geturlname")
        #for (score, urlid) in rankedScoresList[0:10]:
        #    print ( "{:.2f} {:>5}  {}".format ( score, urlid, self.geturlname(urlid)))
        return rankedScoresList,scoreDict

    def createMarkedHtmlFile(self, markedHTMLFilename, testText, testQueryList, colors, url):
        
        #Приобразование текста к нижнему регистру
        testText = testText.lower()
        for i in range(0, len(testQueryList)):
            testQueryList[i] = testQueryList[i].lower()
        # Получения текста страницы с знаками переноса строк и препинания. Прием с использованием регулярных выражений
        wordList = re.compile("[\\w]+|[\\n.,!?:—]").findall(testText)

        #Получить html-код с маркировкой искомых слов
        htmlCode = self.getMarkedHTML(wordList, testQueryList, colors, url)
        #print(htmlCode)

        #сохранить html-код в файл с указанным именем
        file = open(markedHTMLFilename, 'w', encoding="utf-8")
        file.write(htmlCode)
        file.close()


    def getMarkedHTML(self, wordList, queryList, colors, url):
        """Генерировть html-код с макркировкой указанных слов цветом
        wordList - список отдельных слов исходного текста
        queryList - список отдельных искомых слов,
        """
        html = f"""
        <!DOCTYPE html>
        <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>Document</title>
                </head>
            <body>
            <div><b>{url}</b></div>
            <div>
        """
        
        for word in wordList:
            if word in queryList:
                html += f"""
                        <span style=background-color:{colors[word]}> {word} </span>
                        """
            else:
                if word == '\n':
                    html += f"""{word}
                    </div>
                    <div>
                """
                else:                    
                    html += f"""{word}  """

        html += """
            </div>
            </body>
        </html>         
        """
        #... подробнее в файле примере
        return html


# ------------------------------------------
def main():

    mySeacher = Seacher("search_engine.db")

    mySearchQuery = "спецпроекты новости ссср"
    sortedList,scoreDict1 = mySeacher.getSortedList(mySearchQuery)
    rankedScoresList,scoreDict2 = mySeacher.calculatePageRank()

    allRank = dict()
    for url_id in scoreDict1.keys():
        val1 = scoreDict1[url_id]
        val2 = scoreDict2[url_id]
        allRank[url_id] = (val1, val2, (val1+val2)/2)
        
    allRankSort = list()
    for url, score in allRank.items():
        pair = (score, url)
        allRankSort.append( pair )

    # Сортировка из словаря по убыванию
    allRankSort = sorted(allRankSort, key=lambda score: score[0][2], reverse=True) 

    #print("score, urlid, geturlname")
    #for (score, urlid) in avgRankSort[0:10]:
    #    print ( "{:.2f} {:>5}  {}".format ( score, urlid, mySeacher.geturlname(urlid)))

    print("urlid, m1, m2, m3, urlname")
    for (score, urlid) in allRankSort[0:10]:
        print ( "{:>5} {:.2f} {:.2f} {:.2f} {}".format (urlid, score[0], score[1], score[2], mySeacher.geturlname(urlid)))

    cnt = 0
    colors = dict()
    for query in mySearchQuery.lower().split(' '):
        color = f"rgb({random.randint(0,255)},{random.randint(0,255)},{random.randint(0,255)})"
        colors[query] = color

    for [_, urlid] in allRankSort:
        if cnt == 3:
            break
        
        markedHTMLFilename = f"getMarkedHTML{cnt}.html"
        url = mySeacher.geturlname(urlid)
        if "rss" in url:
            continue
        html_doc = requests.get(url).text

        soup = bs4.BeautifulSoup(html_doc, "html.parser")
        listUnwantedItems = ['script', 'style', 'svg', 'head', 'title', 'meta', '[document]' ]
        for script in soup.find_all(listUnwantedItems):
            script.decompose()
        text = ""
        for elem in soup.find_all(text=True):
            text += elem + " "
        mySeacher.createMarkedHtmlFile(markedHTMLFilename, text, mySearchQuery.lower().split(), colors, url)
        cnt+=1

# ------------------------------------------

main()

