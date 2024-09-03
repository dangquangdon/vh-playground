import valohai
from bs4 import BeautifulSoup
from scrapy import Spider, Request
from scrapy.crawler import CrawlerProcess
from scrapy.http.response.html import HtmlResponse


class JutsuSpider(Spider):
    name = "justsu_spider"
    start_urls = [
        "https://naruto.fandom.com/wiki/Special:BrowseData/Jutsu?limit=250&offset=0&_cat=Jutsu",
    ]

    def parse(self, response: HtmlResponse):
        yield from self.extract_justsu_lists(response)
        yield from self.go_to_next_page(response)

    def go_to_next_page(self, response: HtmlResponse):
        for next_page in response.css("a.mw-nextlink"):
            yield response.follow(next_page, self.parse)

    def extract_justsu_lists(self, response: HtmlResponse):
        column_list = response.css(".smw-columnlist-container")
        assert len(column_list) == 1, "Should have only 1 container div"
        container = column_list[0]

        for href in container.css("a::attr(href)").extract():
            jutsu_detail_url = f"https://naruto.fandom.com{href}"
            jutsu_detail = Request(jutsu_detail_url, callback=self.get_jutsu_detail)
            yield jutsu_detail

    def get_jutsu_detail(self, response: HtmlResponse):
        title = response.css("span.mw-page-title-main::text").extract()[0]
        title = title.strip()

        detail_container = response.css("div.mw-parser-output")[0].extract()
        soup = BeautifulSoup(detail_container)
        jutsu_type = self._get_jutsu_type_from_side_table(soup) or ""
        jutsu_description = self._get_jutsu_description(soup)
        return dict(
            jutsu_name=title,
            jutsu_type=jutsu_type,
            jutsu_description=jutsu_description,
        )

    def _get_jutsu_type_from_side_table(self, soup: BeautifulSoup) -> str | None:
        aside_element = soup.find("div").find("aside")
        if not aside_element:
            return None
        for cell in aside_element.find_all("div", {"class": "pi-data"}):
            if not cell.find("h3"):
                continue
            cell_name = cell.find("h3").text.strip()
            if cell_name == "Classification":
                return cell.find("div").text.strip()
        return None

    def _get_jutsu_description(self, soup: BeautifulSoup) -> str:
        # Remove aside
        soup.find("div").find("aside").decompose()
        verbose_description = soup.text.strip()
        return verbose_description.split("Trivia")[0].strip()


if __name__ == "__main__":
    output = valohai.outputs().path("output.jsonl")

    process = CrawlerProcess({
        'FEED_FORMAT': 'jsonl',
        'FEED_URI': output,
        'LOG_ENABLED': False,
    })
    process.crawl(JutsuSpider, output=output)
    process.start()