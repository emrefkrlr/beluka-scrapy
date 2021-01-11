from bs4 import BeautifulSoup
import cfscrape
import mysql.connector
import requests
from mysql.connector import errorcode
from urllib.parse import unquote
import time
import logging

logging.basicConfig(filename='app.log', filemode='a+', format='%(name)s - %(levelname)s - %(message)s')


def main():
    # scrape parent sitemap
    #urls = get_parent_sitemap_scrape_urls()

    # set parent sitemap on db
    #set_parent_sitemap(urls)

    # get parent sitemap
    #parent_urls = get_parent_sitemap_urls()

    #set child sitemape urls on db
    #for parent_url in parent_urls:
        #results = get_child_sitemap_words_in_url(parent_url)
        #save_child_sitemap_url = set_child_sitemap_in_url(results)
        #print('saved parent id:', parent_url[0])

    try:
        # SAVED TRASNLATE DATA
        get_child_sitemap_urls = get_child_sitemap_in_url_ids_on_db(95479, 100000)
        delay_time = 1300
        steps = 0
        for page in get_child_sitemap_urls:
            steps += 1
            child_sitemap_in_url_id = page[0]
            sayfa = url_decode(page[1])
            get_translate_data = get_page_all_word(page[1])
            for data in get_translate_data:
                saved_data = set_translate_data_on_db(data, child_sitemap_in_url_id)
            print('saved detail...\npage:  ', sayfa, '\nchild_sitemap_id: ', child_sitemap_in_url_id)
            if steps > delay_time:
                print('delay....', steps)
                time.sleep(120)
                delay_time += 2000
    except Exception as e:
        logging.error('{} -  id: {}'.format(e, child_sitemap_in_url_id))



    # test_url = "http://www.beluka.de/woerterbuch/deutschtuerkisch/Avrupa%C2%B4da+u%C3%A7ak+markas%C4%B1"
    # for i in test(test_url):
    #     print(i)



def create_connection_db():
    try:
        connection = mysql.connector.connect(
            user='root',
            password='root',
            database='beluka',
            host='127.0.0.1',
            port='8889'
        )
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    finally:
        if connection is not None:
            return connection

def get_parent_sitemap_scrape_urls():
    session = requests.session()
    session.headers = ...
    url = "https://beluka.de/sitemap/google_beluka_sitemap.xml"
    scraper = cfscrape.create_scraper(sess=session, delay=15)
    html_content = scraper.get(url).content
    soup = BeautifulSoup(html_content, 'html.parser')
    get_list = soup.find_all("loc")
    clear_urls = []

    for i in get_list:
        get_url = i.text
        target_url = get_url.find('sitemap_page')
        if target_url > 0:
            clear_urls.append(get_url)

    return clear_urls

def set_parent_sitemap(url):
    connection = create_connection_db()
    for i in url:
        data_base = connection.cursor()
        sql = """INSERT INTO parent_sitemap (url) VALUES (%s);"""
        data_base.execute(sql, (i,))
        connection.commit()

def get_parent_sitemap_urls():
    connection = create_connection_db()
    data_base = connection.cursor()
    sql = "SELECT * FROM parent_sitemap"
    data_base.execute(sql)
    return data_base.fetchall()


def get_child_sitemap_words_in_url(parent_urls):
    session = requests.session()
    session.headers = ...
    scraper = cfscrape.create_scraper(sess=session, delay=15)
    clear_urls = []
    parent_sitemap_id = parent_urls[0]
    url = parent_urls[1]
    html_content = scraper.get(url).content
    soup = BeautifulSoup(html_content, 'html.parser', encoding='utf8')
    get_list = soup.find_all("loc")
    for i in get_list:
        get_url = i.text
        clear_urls.append((parent_sitemap_id, get_url))

    return clear_urls

def set_child_sitemap_in_url(urls):
    connection = create_connection_db()
    for i in urls:
        data_base = connection.cursor()
        sql = """INSERT INTO child_sitemap_in_url (parent_sitemap_id, url) VALUES (%s,%s);"""
        data_base.execute(sql, (i[0], i[1]))
        connection.commit()
        connection.close()

def get_page_all_word(page):
    session = requests.session()
    session.headers = ...
    scraper = cfscrape.create_scraper(sess=session, delay=15)
    url = url_decode(page)
    html_content = scraper.get(url).content
    soup = BeautifulSoup(html_content, 'html.parser')
    trasnlate_content = []
    # get all table
    get_list = soup.find_all("tbody")

    for table in get_list:
        # inspect table - get row
        rows = table.select("tr")

        # get columns by row
        for row in rows:
            columns = row.select("td")
            german = clear_content(columns[0].text)
            turkish = clear_content(columns[1].text)
            trasnlate_content.append((german, turkish))

    return trasnlate_content


def get_child_sitemap_in_url_ids_on_db(start, end):
    connection = create_connection_db()
    data_base = connection.cursor()
    sql = "SELECT id,url FROM `child_sitemap_in_url` where id BETWEEN {} AND {} ORDER BY `id` ASC".format(start, end)
    data_base.execute(sql)
    result = data_base.fetchall()
    connection.close()
    return result

def set_translate_data_on_db(list_data, child_sitemap_in_url_id):
    connection = create_connection_db()
    data_base = connection.cursor()
    sql = """INSERT INTO translate_data2 (child_sitemap_in_url_id, searched_word, value) VALUES (%s,%s, %s);"""
    data_base.execute(sql, (child_sitemap_in_url_id, list_data[0], list_data[1]))
    connection.commit()
    connection.close()

def clear_content(text):
    remove_character = ["\t", "\n", "med."]
    for rcharacter in remove_character:
        text = text.replace(rcharacter, " ")
    return text.strip()

def url_decode(url):
    decode_url = unquote(url)
    Tr2Eng = str.maketrans("çğıöşüâÜßÇĞŞİÖûäÄéîèÎ´", "cgiosuauBCGSIOuaAeieI´")
    enWord = decode_url.translate(Tr2Eng)
    url = enWord.replace("+", "%20")
    return url

def test(url):
    session = requests.session()
    session.headers = ...
    scraper = cfscrape.create_scraper(sess=session, delay=15)
    url = url_decode(url)
    print(url)
    html_content = scraper.get(url).content
    soup = BeautifulSoup(html_content, 'html.parser')
    trasnlate_content = []
    # get all table
    get_list = soup.find_all("tbody")

    for table in get_list:
        # inspect table - get row
        rows = table.select("tr")

        # get columns by row
        for row in rows:
            columns = row.select("td")
            german = clear_content(columns[0].text)
            turkish = clear_content(columns[1].text)
            trasnlate_content.append((german, turkish))

    return trasnlate_content


if __name__ == "__main__":
    main()
