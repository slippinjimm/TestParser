from celery import Celery, group, chain
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import itertools


app = Celery('tasks', broker='redis://:redis_pas@localhost:6379/0')
app.conf.result_backend = 'redis://:redis_pas@localhost:6379/0'
URL_TEMPLATE = "https://zakupki.gov.ru/epz/order/extendedsearch/results.html?fz44=on&pageNumber={page}"
HEADERS = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'}
PAGES_TO_PARSE = 2

class HTMLParserTask(app.Task):
    name = "html_parser"

    def run(self, page_number) -> list:
        url = URL_TEMPLATE.format(page=page_number)
        xml_links = []
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            links = soup.find_all(class_="w-space-nowrap ml-auto registry-entry__header-top__icon")
            for link in links:
                new_link = link.find(target="_blank")
                if new_link and new_link.get('href'):
                    xml_link = "https://zakupki.gov.ru" + new_link['href'].replace("view.html", "viewXml.html")
                    if xml_link.startswith(('http://', 'https://')):
                        xml_links.append(xml_link)
                    else:
                        print(f"Invalid URL found: {xml_link}")
        except Exception as e:
            print(f"Error parsing page {page_number}: {e}")
        return xml_links


class XMLParserTask(app.Task):
    name = "xml_parser"

    def run(self, xml_url) -> list:
        try:
            response = requests.get(xml_url, headers=HEADERS)
            response.raise_for_status()
            root = ET.fromstring(response.text)
            namespace = {'ns': 'http://zakupki.gov.ru/oos/EPtypes/1'}
            publish_dt = root.find('.//ns:commonInfo/ns:publishDTInEIS', namespace)
            return {
                'url': xml_url,
                'publish_dt': publish_dt.text if publish_dt is not None else None
            }
        except Exception as e:
            print(f"Error parsing XML {xml_url}: {e}")
            return None


html_parser = app.register_task(HTMLParserTask())
xml_parser = app.register_task(XMLParserTask())


# Основная функция
def main():
    page_tasks = group(html_parser.s(page) for page in range(1, PAGES_TO_PARSE + 1))
    xml_links = page_tasks.apply_async().get()

    # объеденение списка
    xml_links = list(itertools.chain.from_iterable(xml_links))
    xml_tasks = group(xml_parser.s(link) for link in xml_links)
    results = xml_tasks.apply_async().get()

    for res in results:
        if res:
            print(f"{res['url']} - {res['publish_dt']}")


if __name__ == '__main__':
    main()