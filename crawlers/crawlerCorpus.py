__author__ = 'alexandre s. cavalcante'
__version__ = '4.1'

from webCrawler import Crawler
from bs4 import BeautifulSoup
import pickle, logging, os, re


class CrawlerSite1(object):
    def __init__(self):
        pass

    """
        This version fixes the bug caused by twitter tags and <br> joints sents.
    """

    def crawlAllPages(self, sleepTime, pageStart, pageEnd=1000):
        """
        Crawls all pages from the adorocinema.com news feed.
        :param sleepTime: int variable to indicate the time to sleep after crawl a page
        :param pageStart: int variable to indicate the page we want to start crawl the feed
        :param pageEnd: int variable to indicate the page we want to stop crawl the feed
        :return: void
        """

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
        urlControl = []

        try:
            # load pickle
            urlControl = pickle.load(open('urlControl.p', 'rb'))

        except FileNotFoundError:
            pickle.dump([], open('urlControl.p', 'wb'))
            logging.debug('Pickle was empty - new pickle has been created')

        if (pageEnd != 1000):
            pageEnd = int(crawler.executeXpath(urlbase + '/noticias-materias-especiais/',
                                               ("//div[@class='pager pager pager margin_40t']/ul/li/span/text()"), 5)[
                              -1])

        prefixURL = urlbase + "/noticias-materias-especiais/?page="

        while (pageStart < pageEnd + 1):

            articlesLinks = crawler.executeXpath(prefixURL + str(pageStart),
                                                 '//a[@class="tt_14 bold no_underline"]/@href', sleepTime)
            pageStart += 1
            logging.info('page d`article: ' + prefixURL + str(pageStart) + ' crawled')

            for links in articlesLinks:

                if (links.startswith('/slideshow') or str(urlbase + links) in urlControl):
                    continue

                # download page
                text = str(
                    crawler.executeXpath(urlbase + links, 'string(//div[@class="editorial richText j_w"])', sleepTime))

                if (text == ''):
                    logging.info(' article : ' + urlbase + links + ' was empty - PAGE NOT CRAWLED')
                    continue

                # obtain file name using url
                fileName = links.split('/')[-2]

                # create and write file
                fileOut = open("./corpus/" + fileName + '.txt', 'a+', encoding='utf-8')
                fileOut.write('URL_REF: ' + urlbase + links + '\n\n')
                fileOut.write(text)
                urlControl.append(urlbase + links)
                prefixURL + str(pageStart)
                pickle.dump(urlControl, open('urlControl.p', 'wb'))

                logging.info(' article : ' + urlbase + links + ' crawled')
                print('URL : ' + urlbase + links + ' - crawled! ☣ ☢ ☠')
        pickle.dump(urlControl, open('urlControl.p', 'wb'))

    def crawl2SectionsPages1(self, sleepTime, urlbase, firstXpath, secondXpath, urlPrefix=''):

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

        urlControl = []

        try:
            # load pickle
            urlControl = pickle.load(open('urlControl.p', 'rb'))

        except FileNotFoundError:
            pickle.dump([], open('urlControl.p', 'wb'))
            logging.debug('Pickle was empty - new pickle has been created')

        # crawl first section
        articlesLinks = crawler.executeXpath(urlbase, firstXpath, sleepTime)

        logging.info('page d`article: ' + urlbase + ' crawled')

        # crawl second section with texts
        for links in articlesLinks:

            if (links in urlControl):
                continue

            if (not urlPrefix == ''):
                links = urlPrefix + links
                if (links in urlControl):
                    continue

            textSet = crawler.executeXpath(links, secondXpath, sleepTime)

            if (not len(textSet) > 0):
                continue

            # textSet may be a list or a string, treat differents cases
            if (type(textSet) is list):

                # if the are many lines in the list, concatenate in one string
                if (len(textSet) > 1):
                    text = "\n".join(textSet)
                else:
                    text = str(textSet[0])
            else:
                text = str(textSet)

            if (text == ''):
                logging.info(' article : ' + urlbase + links + ' was empty - PAGE NOT CRAWLED')
                continue

            if (links.startswith('http://www.adorocinema')):

                # obtain file name using url
                fileName = links.split('/')[-2].split('.html')[0] + '.txt'

            else:
                # obtain file name using url
                fileName = links.split('/')[-1].split('.html')[0] + '.txt'

            try:

                # create and write file
                fileOut = open("./corpus/" + fileName + '.txt', 'a+', encoding='utf-8')
                fileOut.write('URL_REF: ' + links + '\n\n')
                fileOut.write(text)
                urlControl.append(links)

            except IOError:
                logging.debug('problem to write the file for the URL ' + links)
                continue

            pickle.dump(urlControl, open('urlControl.p', 'wb'))

            logging.info(' article : ' + urlbase + links + ' crawled')
            print('URL : ' + urlbase + links + ' - crawled! ☣ ☢ ☠')

        print('Total links crawled: ' + str(len(urlControl)))
        pickle.dump(urlControl, open('urlControl.p', 'wb'))

    def crawlWithSoup(self, sleepTime, urlbase, firstXpath, classe, urlPrefix=''):

        if not os.path.exists("./corpus/" + classe + "/"):
            os.makedirs("./corpus/" + classe + "/")

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

        urlControl = []

        try:
            # load pickle
            urlControl = pickle.load(open('urlControl.p', 'rb'))

        except FileNotFoundError:
            pickle.dump([], open('urlControl.p', 'wb'))
            logging.debug('Pickle was empty - new pickle has been created')

        # crawl first section
        articlesLinks = crawler.executeXpath(urlbase, firstXpath, sleepTime)

        logging.info('page d`article: ' + urlbase + ' crawled')

        # crawl second section with texts
        for links in articlesLinks:

            if (links in urlControl):
                continue

            if (not urlPrefix == ''):
                links = urlPrefix + links
                if (links in urlControl):
                    continue

            # get page content
            text = ''
            pageContentTemp = crawler.getPage(links, 4)
	
            if pageContentTemp is None:
                continue
            else:
                pageContent = pageContentTemp.text

            # deleting the strong tag avoids the problem with sents like "Lançamentos DCConsiderado"
            pageContent = re.sub('</?strong>', '', pageContent)
            pageContent = re.sub('</?b>', '', pageContent)
            pageContent = re.sub('<br>', '*#*space#*', pageContent)
            pageContent = re.sub('<div class="scrollable mceNonEditable richPlayer">.*?</div>', '', pageContent)
            pageContent = re.sub('<twitterwidget>.*?</twitterwidget>', '', pageContent)
            pageContent = re.sub('<!--wadsCallStart-->.*?<!--wadsCallEnd-->', '', pageContent, flags= re.DOTALL)

            soup = BeautifulSoup(pageContent, 'html.parser')


            if (links.startswith('http://www.adorocinema.com/')):
                for t in soup.find_all('div', {"id": "article-content"}):
                    text = text + '\n' + t.text
            else:
                # extract comments and links from globo.com articles
                for s in soup('div', {"class": "feed theme theme-border-color-primary"}):
                    s.extract()

                for s in soup('div', {"id": "menu-addon-container"}):
                    s.extract()

                for s in soup('div', {"class": "content-meta-info"}):
                    s.extract()

                for s in soup('div', {"class": "wrapper-comentarios"}):
                    s.extract()

                for s in soup('div', {"class": "box-lista-noticias"}):
                    s.extract()

                for s in soup('div', {"class": "materia-cabecalho"}):
                    s.extract()

                for s in soup('div', {"class": "materia-assinatura-letra"}):
                    s.extract()

                for s in soup('header', {"class": "materia-cabecalho"}):
                    s.extract()


                for s in soup('div', {"class": "comentarios-conteudo"}):
                    s.extract()

                for s in soup('footer', {"id": "rodape"}):
                    s.extract()



                for t in soup.find_all('p'):
                    text = text + '\n' + t.text
            text = text.replace('*#*space#*', '\n')

            if (text == ''):
                logging.info(' article : ' + urlbase + links + ' was empty - PAGE NOT CRAWLED')
                continue

            if (links.startswith('http://www.adorocinema')):

                # obtain file name using url
                fileName = links.split('/')[-2].split('.html')[0] + '.txt'

            else:
                # obtain file name using url
                fileName = links.split('/')[-1].split('.html')[0] + '.txt'


            try:

                # create and write file
                fileOut = open("./corpus/" + classe + "/" + fileName + '.txt', 'a+', encoding='utf-8')
                fileOut.write('URL_REF: ' + links + '\n\n')
                fileOut.write(text)
                urlControl.append(links)

            except IOError:
                logging.debug('problem to write the file for the URL ' + links)
                continue

            pickle.dump(urlControl, open('urlControl.p', 'wb'))

            logging.info(' article : ' + urlbase + links + ' crawled')
            print('URL : ' + urlbase + links + ' - crawled! ☣ ☢ ☠')

        print('Total links crawled: ' + str(len(urlControl)))
        pickle.dump(urlControl, open('urlControl.p', 'wb'))
