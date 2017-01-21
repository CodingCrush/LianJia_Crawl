import asyncio
import cgi
import re
from datetime import datetime
from asyncio import Queue
import aiohttp
from lxml import etree
from collections import OrderedDict
import csv
import time


base_url = 'http://sh.lianjia.com/'
prefix_url = base_url[:-1]

file = 'lianjia1.csv'

max_page = 2300

sleep_interval = 0.1


def get_page_url(_max_page, start=1):
    page = start
    while page < _max_page:
        yield ''.join((base_url, 'zufang/d', str(page)))
        page += 1


def is_root_url(url):
    if re.match(base_url+'zufang/d', url, re.S):
        return True
    return False


def create_csv():
    with open(file, 'w', newline='') as f:
        f_csv = csv.writer(f)
        fields = ['href', 'title', 'compound', 'layout',
                  'gross_floor_area', 'distribute', 'road', 'floor',
                  'orientation', 'rent_per_month', 'added_at',
                  'total_views', 'subway_line', 'subway_station',
                  'subway_distance', 'number', 'address',
                  'latest_week_views', 'room_type', 'img_url',
                  'captured_at']
        f_csv.writerow(fields)

        """
        house_dict_detail
        {
            'href': 'http://sh.lianjia.com/zufang/shz3363299.html',
            'title': '钥匙在店，2室1厅，享受好房，人气房源',
            'compound': 华通公寓,
            'layout': 2室1厅,
            'gross_floor_area': 75,
            'distribute': 静安,
            'road': 江宁路,
            'floor': 高区/18层,
            'orientation': 朝南,
            'rent_per_month': 9000,
            'added_at': 2016.08.20,
            'total_views': 4,
            'subway_line': 7,
            'subway_station': 昌平路,
            'subway_distance': 486,

            # in second_level_url
            'number': shz3700315 ,
            'address': 瞿溪路1111弄
            'latest_week_views': 4
            'room_type': 卧室/14.69平米/厨房/3.29平米/卫生间/2.9平米/阳台/4.19平米
            'img_url': 'http://cdn1.dooioo.com/fetch/vp/fy/gi/2016'
                        '1231/8348b246-dcde-40e7-88d6-0a8413863b15'
                        '.jpg_200x150.jpg',
            'captured_at: 2017-01-18 16:58:33.964408
        }
        """


class Crawler:
    def __init__(self, roots,
                 max_tries=4, max_tasks=10, _loop=None):
        self.loop = _loop or asyncio.get_event_loop()
        self.roots = roots
        self.max_tries = max_tries
        self.max_tasks = max_tasks
        self.urls_queue = Queue(loop=self.loop)
        self.seen_urls = set()
        self.session = aiohttp.ClientSession(loop=self.loop)
        self.href_house = {}
        create_csv()
        for root in roots:
            self.urls_queue.put_nowait(root)

        self.started_at = datetime.now()
        self.end_at = None

    def close(self):
        self.session.close()

    def write_to_csv(self, house_dict):
        with open(file, 'a+', newline='') as f:
            f_csv = csv.writer(f)
            href = house_dict['href']
            f_csv.writerow(house_dict.values())
            del self.href_house[href]

    @staticmethod
    async def fetch_etree(response):
        if response.status == 200:
            content_type = response.headers.get('content-type')
            if content_type:
                content_type, _ = cgi.parse_header(content_type)
            if content_type in ('text/html', 'application/xml'):
                text = await response.text()
                doc = etree.HTML(text)
                return doc

    def _parse_root_per_house(self, house_box, count):
        house_dict = OrderedDict()
        child = '//li[{}]'.format(count)

        href = prefix_url + house_box.xpath(
            child + '/div[2]/h2/a/@href')[0]
        house_dict['href'] = href
        self.seen_urls.add(href)

        house_dict['title'] = house_box.xpath(
            child + '/div[2]/h2/a/text()')[0]

        house_dict['compound'] = house_box.xpath(
            child + '/div[2]/div[1]/div[1]/a/span/text()')[0].strip()

        house_dict['layout'] = house_box.xpath(
            child + '/div[2]/div[1]/div[1]/span[1]/text()')[0].strip()

        house_dict['gross_floor_area'] = house_box.xpath(
            child + '/div[2]/div[1]/div[1]/span[2]/text()')[0].strip()[:-1]

        house_dict['distribute'] = house_box.xpath(
            child + '/div[2]/div[1]/div[2]/div/a[1]/text()')[0]

        house_dict['road'] = house_box.xpath(
            child + '/div[2]/div[1]/div[2]/div/a[2]/text()')[0]

        house_dict['floor'] = house_box.xpath(
            child + '/div[2]/div[1]/div[2]/div/text()')[3].strip()

        try:
            house_dict['orientation'] = house_box.xpath(
                child + '/div[2]/div[1]/div[2]/div/text()')[4].strip()
        except IndexError:
            house_dict['orientation'] = None

        house_dict['rent_per_month'] = house_box.xpath(
            child + '/div[2]/div[2]/div[1]/span/text()')[0]

        house_dict['added_at'] = house_box.xpath(
            child + '/div[2]/div[2]/div[2]/text()')[0][:10]

        house_dict['total_views'] = house_box.xpath(
            child + '/div[2]/div[3]/div/div[1]/span/text()')[0]

        try:
            subway_detail = house_box.xpath(
                child + '/div[2]/div[1]/div[3]/div/div/span[2]/span/text()')[0]
            _matched = re.search('距离(.*?)号线(.*?)[站+](.*?)米', subway_detail, re.S)
            if _matched:
                house_dict['subway_line'] = _matched.group(1)
                house_dict['subway_station'] = _matched.group(2)
                house_dict['subway_distance'] = _matched.group(3)
        except IndexError:
            house_dict['subway_line'] = None
            house_dict['subway_station'] = None
            house_dict['subway_distance'] = None
        return house_dict

    def parse_root_etree(self, doc):
        houses_select = '//*[@id="house-lst"]/li'
        houses_list = doc.xpath(houses_select)
        count = 0
        for house_box in houses_list:
            count += 1
            house_dict = self._parse_root_per_house(house_box, count)
            second_level_url = house_dict['href']
            self.href_house[second_level_url] = house_dict
            self.urls_queue.put_nowait(second_level_url)

    def parse_second_etree(self, doc, href):
        assert href in self.href_house.keys()
        assert href in self.seen_urls
        house_dict = self.href_house[href]
        house_dict['number'] = href[len(base_url+'zufang/'):-5]

        if doc is not None:
            house_dict['address'] = doc.xpath(
                '//tr[4]/td[2]/p/@title')[0]
            try:
                house_dict['latest_week_views'] = doc.xpath(
                    '//*[@id="record"]/div[2]/div[2]/text()')[0]
            except IndexError:
                house_dict['latest_week_views'] = None
        else:
            house_dict['address'] = None
            house_dict['latest_week_views'] = None

        # TODO: fetch AJAX room_type data
        house_dict['room_type'] = None

        if doc is None:
            house_dict['img_url'] = None
        else:
            img_tmp = []
            count = 1
            for img_box in doc.xpath('//*[@id="album-view-wrap"]/ul/li'):
                img_src = img_box.xpath('//li[{}]/img/@src'.format(count))[0]
                count += 1
                img_tmp.append(img_src)
            img_tmp = ','.join(img_tmp)
            house_dict['img_url'] = img_tmp

        house_dict['captured_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.write_to_csv(house_dict)

    async def handle(self, url):
        tries = 0
        while tries < self.max_tries:
            try:
                response = await self.session.get(
                    url, allow_redirects=False)
                break
            except aiohttp.ClientError:
                pass
            tries += 1
        try:
            doc = await self.fetch_etree(response)
            if is_root_url(url):
                print('root:{}'.format(url))
                self.parse_root_etree(doc)
            else:
                print('second level:{}'.format(url))
                self.parse_second_etree(doc, url)
        finally:
            await response.release()

    async def work(self):
        try:
            while True:
                url = await self.urls_queue.get()
                await self.handle(url)
                time.sleep(sleep_interval)
                self.urls_queue.task_done()
        except asyncio.CancelledError:
            pass

    async def run(self):
        workers = [asyncio.Task(self.work(), loop=self.loop)
                   for _ in range(self.max_tasks)]
        self.started_at = datetime.now()
        await self.urls_queue.join()
        self.end_at = datetime.now()
        for w in workers:
            w.cancel()

if __name__ == '__main__':

    loop = asyncio.get_event_loop()
    crawler = Crawler(get_page_url(max_page), max_tasks=100)
    loop.run_until_complete(crawler.run())

    print('Finished {0} urls in {1} secs'.format(
        len(crawler.seen_urls),
        (crawler.end_at - crawler.started_at).total_seconds()))

    crawler.close()

    loop.close()
