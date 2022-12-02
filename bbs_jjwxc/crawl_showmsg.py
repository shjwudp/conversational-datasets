import json
import argparse
from urllib.parse import urlsplit, parse_qs

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


def get_comment_id(url):
    query = urlsplit(url).query
    params = parse_qs(query)

    return params["id"][0]


class JJWXCMessageSpider(scrapy.Spider):
    name = "https://bbs.jjwxc.net/ Message Spider"
    allowed_domains = ["bbs.jjwxc.net"]

    custom_settings = {
        "BOT_NAME": "Conversational-Datasets Spider",
        "FEED_EXPORT_ENCODING": "utf-8",
        "ROBOTSTXT_OBEY": True,
    }

    def __init__(self, url_jsonl) -> None:
        super().__init__()

        def next_page():
            for line in open(url_jsonl):
                j = json.loads(line)

                yield j["url"]

        self.next_page = next_page()
        self.start_urls = [
            f"https://bbs.jjwxc.net/{next(self.next_page)}",
        ]

    def parse(self, response):
        comment_id = get_comment_id(response.request.url)
        msgsubject = response.css("div#msgsubject::text").getall()
        comment = response.css(f"div#topic::text").getall()
        comment_author = []
        for tr in response.css(f"tr.comment_{comment_id}"):
            comment_author += tr.css("td.authorname *::text").getall()

        replies = []
        for tr in response.css(".replyend"):
            reply_id = tr.xpath("@data-replyid").get()
            reply_author = []
            for tr in response.css(f"tr.reply_{reply_id}"):
                reply_author += tr.css("td.authorname *::text").getall()

            replies.append({
                "quote": response.css(f"div#quotebody_{reply_id}.quotebodyinner::text").getall(),
                "text": response.css(f"div#replybody_{reply_id}.replybodyinner::text").getall(),
                "author": reply_author,
            })

        yield {
            "msgsubject": msgsubject,
            "comment": {
                "author": comment_author,
                "text": comment,
            },
            "replies": replies,
        }

        next_page = next(self.next_page)
        if next_page:
            yield response.follow(next_page, self.parse)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", default="./jjwxc_message.jsonl")

    args = parser.parse_args()

    settings = get_project_settings()
    settings.update({
        "FEED_FORMAT": "jsonlines",
        "FEED_URI": args.output,
    })

    process = CrawlerProcess(settings)
    process.crawl(JJWXCMessageSpider, url_jsonl=args.input)
    process.start()


if __name__ == "__main__":
    main()
