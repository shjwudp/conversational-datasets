import argparse

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings



class JJWXCBoardSpider(scrapy.Spider):
    name = "https://bbs.jjwxc.net/ Board Spider"
    allowed_domains = ["bbs.jjwxc.net"]
    start_urls = [
        "https://bbs.jjwxc.net/board.php?board=2&type=&page=1",
    ]

    custom_settings = {
        "BOT_NAME": "Conversational-Datasets Spider",
        "FEED_EXPORT_ENCODING": "utf-8",
        "ROBOTSTXT_OBEY": True,
    }

    def __init__(self, start_urls=None) -> None:
        super().__init__()
        self.page = 1
        if start_urls:
            self.start_urls = start_urls

    def parse(self, response):
        for td in response.css("td.subjecttd"):
            text = td.css("*::text").getall()
            href = td.css("a::attr(href)").get()

            yield {
                "text": text,
                "url": href,
            }

        next_page_table = [t for t in response.css("table")][-3]
        next_page_td = [td for td in next_page_table.css("td")][-1]
        next_page_a = [a for a in next_page_td.css("a")][-2]
        next_page = next_page_a.css("::attr(href)").get()

        if next_page:
            yield response.follow(next_page, self.parse)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start_urls", nargs="+")
    parser.add_argument("--output", default="./jjwxc_board.jsonl")

    args = parser.parse_args()

    settings = get_project_settings()
    settings.update({
        "FEED_FORMAT": "jsonlines",
        "FEED_URI": args.output,
    })

    process = CrawlerProcess(settings)
    process.crawl(JJWXCBoardSpider, start_urls=args.start_urls)
    process.start()


if __name__ == "__main__":
    main()
