import datetime

__author__ = 'alexandre s. cavalcante'
__version__= '1.0'

from webCrawler import Crawler
import pickle,logging, os

class CrawlerDic(object):

    def __init__(self):
        pass

    """
        This script crawls de news session from http://www.adorocinema.com/ . The website contains 1058 pages, ordered from
        newest to older news.
    """

    def crawlListAdoroCin(self, sleepTime):

        filmFile = open('./dicionarios/filmList.txt', 'a+', encoding='utf-8')

        # create logger
        logger = logging.getLogger("logging2")
        logger.setLevel(logging.DEBUG)

        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # create formatter
        formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s")

        # add formatter to ch
        ch.setFormatter(formatter)

        # add ch to logger
        logger.addHandler(ch)

        # crawler the news session
        crawler = Crawler()
        urlbase = 'http://www.adorocinema.com/'
        filmControl = []

        try:
            # load pickle
            filmControl = pickle.load(open('filmList.p', 'rb'))

        except FileNotFoundError:
            pickle.dump([], open('filmList.p', 'wb'))
            logging.debug('Pickle was empty - new pickle has been created')


        filmList = crawler.executeXpath('http://www.adorocinema.com/filmes/numero-cinemas/', '//h2[@class="tt_18 d_inline"]/a/text()', sleepTime)

        prefixURL = urlbase+ "/filmes/numero-cinemas/?page="

        pageStart = 2

        while(pageStart < 6  ):

            filmList.extend(crawler.executeXpath(prefixURL + str(pageStart), '//h2[@class="tt_18 d_inline"]/a/text()', sleepTime))
            pageStart += 1
            logging.info('page d`article: ' + prefixURL + str(pageStart) + ' crawled')

        filmControl.extend(filmList)

        print(' List crawled ' + str(datetime.datetime.now()))

        for film in filmList:
            if(film in filmControl):
                print(film)
                filmFile.write(film.rstrip().lstrip() + '\n')
                filmControl.append(film)
        pickle.dump(filmControl, open('filmList.p', 'wb'))

    def crawlOneSectionPage(self, sleepTime, url, xpath):

        filmFile = open('./dicionarios/movie_list.txt', 'a+', encoding='utf-8')

        # create logger
        logger = logging.getLogger("logging2")
        logger.setLevel(logging.DEBUG)

        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        # create formatter
        formatter = logging.Formatter("%(asctime)s;%(levelname)s;%(message)s")

        # add formatter to ch
        ch.setFormatter(formatter)

        # add ch to logger
        logger.addHandler(ch)

        # crawler the news session
        crawler = Crawler()

        filmControl = []

        try:
            # load pickle
            filmControl = pickle.load(open('filmList.p', 'rb'))

        except FileNotFoundError:
            pickle.dump([], open('filmList.p', 'wb'))
            logging.debug('Pickle was empty - new pickle has been created')

        filmList = crawler.executeXpath(url, xpath, sleepTime)

        print(' List crawled ' + str(datetime.datetime.now()))

        filmFile.write('#### DATE: ' + str(datetime.datetime.now()) + '#### SOURCE: ' + url+ '\n')

        for film in filmList:
            if(not film in filmControl):
                print(film)
                filmFile.write(film.rstrip().lstrip() + '\n')
                filmControl.append(film)
        print('Total entries inserted: ' + str(len(filmControl)))
        pickle.dump(filmControl, open('filmList.p', 'wb'))
        filmFile.close()