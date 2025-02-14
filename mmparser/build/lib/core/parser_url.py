"""mmparser"""

import logging
import re
from datetime import datetime
import string
from time import sleep, time
import threading
import concurrent.futures
import sys
import json
import signal
from pathlib import Path
from urllib.parse import urlparse, parse_qsl, parse_qs, unquote, urljoin
import uuid

from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeRemainingColumn
from rich.logging import RichHandler
from curl_cffi import requests

from .models import ParsedOffer, Connection
from .exceptions import ConfigError, ApiError
from . import db_utils, utils
from .telegram import TelegramClient, validate_tg_credentials


class Parser_url:
    def __init__(
        self,
        url: str,
        urls: list[str],
        categories_path: str = "",
        job_name: str = "",
        include: str = "",
        exclude: str = "",
        blacklist: str = "",
        all_cards: bool = False,
        no_cards: bool = False,
        cookie_file_path: str = "",
        account_alert: bool = False,
        address: str = "",
        proxy: str = "",
        allow_direct: bool = False,
        use_merchant_blacklist: bool = False,
        proxy_file_path: str = "",
        tg_config: list = [],
        price_min_value_alert: float = None,
        price_value_alert: float = None,
        price_bonus_value_alert: float = None,
        bonus_value_alert: float = None,
        bonus_percent_alert: float = None,
        alert_repeat_timeout: float = None,
        threads: int = None,
        delay: float = None,
        error_delay: float = None,
        log_level: str = "INFO",
    ):
        self.cookie_file_path = cookie_file_path
        self.proxy = proxy
        self.allow_direct = allow_direct
        self.proxy_file_path = proxy_file_path
        self.categories_path = categories_path
        self.tg_config = tg_config
        self.connection_success_delay = delay or 1.8
        self.connection_error_delay = error_delay or 10.0
        self.log_level = log_level

        self.start_time: datetime = None

        self.region_id = "50"
        self.session = None
        self.connections: list[Connection] = []
        self.parsed_proxies: set | None = None
        self.categories: dict = None
        self.cookie_dict: dict = None
        self.profile: dict = {}
        self.rich_progress = None
        self.job_name: str = ""

        self.logger: logging.Logger = self._create_logger(self.log_level)
        self.tg_client: TelegramClient = None
        self.tg_client_phone: TelegramClient = None

        self.url: str = url
        self.urls: list[str] = urls
        self.job_name: str = job_name
        self.include: str = include
        self.exclude: str = exclude
        self.blacklist_path: str = blacklist
        self.all_cards: bool = all_cards
        self.no_cards: bool = no_cards
        self.address: str = address
        self.proxy: str = proxy
        self.account_alert: bool = account_alert
        self.use_merchant_blacklist: bool = use_merchant_blacklist
        self.merchant_blacklist: list = utils.load_blacklist() if use_merchant_blacklist else []
        self.price_min_value_alert: float = price_min_value_alert or float("-inf")
        self.price_value_alert: float = price_value_alert or float("inf")
        self.price_bonus_value_alert: float = price_bonus_value_alert or float("inf")
        self.bonus_value_alert: float = bonus_value_alert or float("-inf")
        self.bonus_percent_alert: float = bonus_percent_alert or float("inf")
        self.alert_repeat_timeout: float = alert_repeat_timeout or 0
        self.perecup_price: float = None
        self.threads: int = threads

        self.blacklist: list = []
        self.parsed_url: dict = None
        self.scraped_tems_counter: int = 0
        self.rich_progress = None
        self.job_id: int = None

        self.address_id: str = None
        self.lock = threading.Lock()
        
        self.all_titles = []
        self.zakup_info = ""

        self._set_up()

    def _create_logger(self, log_level: str) -> logging.Logger:
        logging.basicConfig(
            level=log_level,
            format="%(message)s",
            datefmt="%H:%M:%S",
            handlers=[RichHandler(rich_tracebacks=True)],
        )
        return logging.getLogger("rich")

    def _proxies_set_up(self) -> None:
        """Настройка и валидация прокси"""
        if self.proxy:
            is_valid_proxy = utils.proxy_format_check(self.proxy)
            if not is_valid_proxy:
                raise ConfigError(f"Прокси {self.proxy} не верного формата!")
            self.connections = [Connection(self.proxy)]
        elif self.parsed_proxies:
            for proxy in self.parsed_proxies:
                is_valid_proxy = utils.proxy_format_check(proxy)
                if not is_valid_proxy:
                    raise ConfigError(f"Прокси {proxy} не верного формата!")
            self.connections = [Connection(proxy) for proxy in self.parsed_proxies]
        if self.connections and self.allow_direct:
            self.connections.append(Connection(None))
        elif not self.connections:
            self.connections = [Connection(None)]

    def _get_connection(self) -> str:
        """Получить самое позднее использованное `Соединение`"""
        while True:
            free_proxies = [proxy for proxy in self.connections if not proxy.busy]
            if free_proxies:
                oldest_proxy = min(free_proxies, key=lambda obj: obj.usable_at)
                current_time = time()
                if oldest_proxy.usable_at <= current_time:
                    return oldest_proxy
                sleep(oldest_proxy.usable_at - time())
                return self._get_connection()
            # No free proxies, wait and retry

    def _api_request(self, api_url: str, json_data: dict, tries: int = 10, delay: float = 0) -> dict:
        headers = {
            "cookie": "spid=1739524944560_206a03d45181b4ec04b4fb09d6d7c655_c32xxggt69laajxw; __zzatw-smm=MDA0dBA=Fz2+aQ==; _sa=SA1.fb7a2799-258a-4160-a5e0-bbb713c930fb.1739524945; _sas=SA1.fb7a2799-258a-4160-a5e0-bbb713c930fb.1739524945.1739524945; spjs=1739524947625_424d73cd_0134fe0a_717888e8ba5d44f93ec371269d7796f8_bdCTMz8GeqtWDLFwnMZy5+KuDm6bY/aW0o6PL3Gc5RGY0BrKli+zkzpgwSEB+W0uyvJnYqN0+iVnPBDCmhR5eVXOP705RMUlEWru6172ZxCMwWnZFS5nhwtim+tg+Bxd2aRj1oJpUpx4hpMCuZKs3nMb9iaOsKVEUQg6uA/348OWfw1oJUpkRRhTe23C2kIrLrEzZJFa+F5KVUcWjcNsLdBIZJfLYE4/FvjsXGghRUfTDYhp3JsxYVnALNxHyQd26jLFdZd8eYis1+eWA/tOyNVsMAGcQM+/MxuW8uiQRFQxuV/N2cB1pHKnS+sXm7rd/mTo6JxMDaJe8Zbix99Juj/GZSA+Jlr8AO9kNC3kLnExXmvfumejRHEvmzgXy9HSXocZv0NaxFVtowOB5Yj6Pn2mEvJ2Blt/IfhEFNuXfR5Tqsk5yuJykNUvS7uPlqEDnwcr6sPt0SF7BL7JYNhsfcmRJqbSa7+/CyyQQF2FaQr2niPHnERU1/M9Pe144VZHkog5S5RupHRIMUxatL9jPTjx1fWWaS6+ajJXZvyk6GnkbaJlHUFY3BJcidhcNX4FwT1KGm45NfFbkFxY/U1oQMWkJ2cfswUH2mC2ZzBY3GvHjzLzluWrCxWAADGbwVcWwpovTVnA9aRBh7lrFE5xId6FO0jUy/6vXwKy5hLt7Y1oBALgnsfIqlSPcmGvRHwnhw8L7o/hooID3Zk9Gs9BAx4m7b7CagemDvdhkCT8iVz5phO1MDTZfRGYxPVfSbbHK3PAEGS9BrX2mRydkMDVKd/msuo0RrHz/BJ+fvWMSRjNhGcylm5qek8oIkJ+SBqo2F1xMJBLM+UDDr8aW08Vp9vIaksnz3Oj39W5aRDOcC3TM2LNgljcjflWIoNZkboK1y5nl8umK9p0ChmvmuNXoad/yppeHfc1uTRYaJFblmaKRjNxdXgcP/mFoYFkDC4pUxnVpMiH/T+zPlKiH1WQYIRIOXq/18agpwHtj2IMdnc7864Oxon7WF1UUhLTyj/PyhITJukQ3hzBT7QXSuAM5XPuSLi+92Lncw7vP0Gc8BH9lRq69grjwyoQxCEBSR07utcXxxZUbf0BHDmiLmX5OMScL/+LUuIAUousTBhMj0mc5jfVzv7AKVUMs2iS6n+ml6x2BJrFc185h7trFcb7iXUGGowhmxeGXqqcTtaBSmTvyASrqA7N4myAukl0TEEv6gMi66I88l1MoJTkGefb9Nw2RaDNRPascQVwphMvdQABjzVI0=; device_id=35899286-eab5-11ef-9f27-fa163e551efb; sbermegamarket_token=572ed57d-6bd2-4e61-91c1-2a8c5a9f4d07; ecom_token=572ed57d-6bd2-4e61-91c1-2a8c5a9f4d07; _ym_uid=1739524948866859906; _ym_d=1739524948; _ym_visorc=b; _ym_isad=1; isOldUser=true; spsc=1739524949967_0523d70dcd6399b4a5c9303cf33ece42_bf4cd2fa3d30987fd0282ffebd8a9122; adspire_uid=AS.258800070.1739524950; _ga=GA1.1.1029686991.1739524950; _sv=SA1.cde8b57e-6cd3-4f86-b94c-49af5eed4df5.1714480000; __tld__=null; ssaid=378b7040-eab5-11ef-bc88-8968fbb7d467; ma_cid=4267358201739524951; adtech_uid=9b0c39e2-055a-4433-9a93-a60a5c995d75%3Amegamarket.ru; top100_id=t1.7729504.882577337.1739524951495; ma_id=1499508061701609951554; region_info=%7B%22displayName%22%3A%22%D0%9C%D0%9E%D0%A1%D0%9A%D0%92%D0%90%22%2C%22kladrId%22%3A%227700000000000%22%2C%22isDeliveryEnabled%22%3Atrue%2C%22geo%22%3A%7B%22lat%22%3A55.755814%2C%22lon%22%3A37.617635%7D%2C%22id%22%3A%2250%22%7D; cfidsw-smm=opCoTy6g2b4IzMmxVQQfv75AJgicCFs87jgV5K7TgPOyYKQYLHqN/XpEq3sJaDr8MxksQTSI6fbPme5Q/F489Gse2g88tO+MHAtLdWZHu6jlT89+BhYxWEjFUdi5QyyFW2RRp4lWMEfATXQx+5vAnnHxAHHKBN2QsDYTVuau; ma_ss_d19e96e4-c103-40bf-a791-3dcb7460a86f=0733784061739524951.1.1739524998.7; cfidsw-smm=Sjsi6mmmKmMHCuI7eqo+mLohsBFLJikK/8nU5B9Ip7t8HnTZA4vksjL9xjknATKWHkbPBthy7xB+Cj9En02XaZvQUEKCdkkJ8zUTPVDOKIzXhW69z/6zO4nK2Yu18B1bJeD849J3YHiWZ7m0u0nvhUIeNxHWzauhr9O1f/5b; _ga_W49D2LL5S1=GS1.1.1739524950.1.1.1739525004.6.0.0; t3_sid_7729504=s1.1027760779.1739524943708.1739525004058.1.10"
        }
        json_data["auth"] = {
            "locationId":"50",
            "appPlatform":"WEB",
            "appVersion":0,
            "experiments":{},
            "os":"UNKNOWN_OS"
        }
        for i in range(0, tries):
            proxy: Connection = self._get_connection()
            proxy.busy = True
            self.logger.debug("Прокси : %s", proxy.proxy_string)
            try:
                response = requests.post(api_url, headers=headers, json=json_data, proxy=proxy.proxy_string, verify=False, impersonate="chrome120")
                response_data: dict = response.json()
            except Exception:
                response = None
            if response and response.status_code == 200 and not response_data.get("error"):
                proxy.usable_at = time() + delay
                proxy.busy = False
                return response_data
            if response and response.status_code == 200 and response_data.get("code") == 7:
                self.logger.debug("Соединение %s: слишком частые запросы", proxy.proxy_string)
                proxy.usable_at = time() + self.connection_error_delay
            else:
                sleep(1 * i)
            proxy.busy = False

        raise ApiError("Ошибка получения данных api")

    def _get_profile(self) -> None:
        """Получить и сохранить информацию профиля ММ"""
        response_json = self._api_request("https://megamarket.ru/api/mobile/v1/securityService/profile/get", json_data={})
        self.profile = response_json["profile"]

    def _set_up(self) -> None:
        """Парсинг в валидация конфигурации"""
        if self.tg_config:
            for client in self.tg_config:
                if not validate_tg_credentials(client):
                    raise ConfigError(f"Конфиг {client} не прошел проверку!")
            self.tg_client = TelegramClient(self.tg_config[0], self.logger)
            self.tg_client_phone = TelegramClient(self.tg_config[1], self.logger)
        self.parsed_proxies = self.proxy_file_path and utils.parse_proxy_file(self.proxy_file_path)
        self.categories = self.categories_path and utils.parse_categories_file(self.categories_path)
        self._proxies_set_up()
        self.cookie_dict = self.cookie_file_path and utils.parse_cookie_file(self.cookie_file_path)

        # Make Ctrl-C work when deamon threads are running
        signal.signal(signal.SIGINT, signal.SIG_DFL)

        regex_check = self.include and utils.validate_regex(self.include)
        if regex_check is False:
            raise ConfigError(f'Неверное выражение "{self.include}"!')
        regex_check = self.exclude and utils.validate_regex(self.exclude)
        if regex_check is False:
            raise ConfigError(f'Неверное выражение "{self.exclude}"!')
        if self.blacklist_path:
            self._read_blacklist_file()
        self.threads = self.threads or len(self.connections)
        if not Path(db_utils.FILENAME).exists():
            db_utils.create_db()

    def parse(self) -> None:
        """Метод запуска парсинга"""
        utils.check_for_new_version()
        if self.address:
            self._get_address_from_string(self.address)
        while True:
            db_utils.delete_old_entries()
            for single_url in self.urls:
                self.url = single_url
                self.start_time = datetime.now()
                self.logger.info("Целевой URL: %s", self.url)
                self.logger.info("Потоков: %s", self.threads)
                self._single_url()
                # filename = f"{uuid.uuid4().hex}.json"
                # with open(filename, "w", encoding="utf-8") as file:
                #     json.dump(sorted(set(self.all_titles)), file, indent=4, ensure_ascii=False)
                # self.all_titles = []

    def _single_url(self):
        self.parse_input_url()
        if self.parsed_url and not self.job_name:
            search_text = self.parsed_url.get("searchText", {})
            collection_title = (self.parsed_url.get("collection", {}) or {}).get("title")
            merchant = (self.parsed_url.get("merchant", {}) or {}).get("slug")
            unknown = "Не_определено"
            self.job_name = search_text or collection_title or merchant or unknown
            self.job_name = utils.slugify(self.job_name)
        if self.parsed_url["type"] == "TYPE_PRODUCT_CARD":
            self._parse_card()
            self.logger.info("%s %s", self.job_name, self.start_time.strftime("%d-%m-%Y %H:%M:%S"))
        else:
            self.logger.info("%s %s", self.job_name, self.start_time.strftime("%d-%m-%Y %H:%M:%S"))
            self._parse_multi_page()
            self.logger.info("Спаршено %s товаров", self.scraped_tems_counter)

    def _read_blacklist_file(self):
        blacklist_file_contents: str = open(self.blacklist_path, "r", encoding="utf-8").read()
        self.blacklist = [line for line in blacklist_file_contents.split("\n") if line]

    def _export_to_db(self, parsed_offer: ParsedOffer) -> None:
        """Экспорт одного предложения в базу данных"""
        with self.lock:
            db_utils.add_to_db(
                self.job_id,
                self.job_name,
                parsed_offer.goods_id,
                parsed_offer.merchant_id,
                parsed_offer.url,
                parsed_offer.title,
                parsed_offer.price,
                parsed_offer.price_bonus,
                parsed_offer.bonus_amount,
                parsed_offer.bonus_percent,
                parsed_offer.available_quantity,
                parsed_offer.delivery_date,
                parsed_offer.merchant_name,
                parsed_offer.merchant_rating,
                parsed_offer.notified,
            )

    def parse_input_url(self, tries: int = 10) -> dict:
        """Парсинг url мм с использованием api самого мм"""
        json_data = {"url": self.url}
        response_json = self._api_request(
            "https://megamarket.ru/api/mobile/v1/urlService/url/parse",
            json_data,
            tries=tries,
        )
        parsed_url = response_json["params"]
        parsed_url = self._filters_convert(parsed_url)
        parsed_url["type"] = response_json["type"]
        sorting = int(dict(parse_qsl(unquote(urlparse(self.url).fragment.lstrip("?")))).get("sort", 0))
        search_query_from_url = parse_qs(urlparse(self.url).query).get("q", "") or ""
        search_query_from_url = search_query_from_url[0] if search_query_from_url else None
        parsed_url["searchText"] = parsed_url["searchText"] or search_query_from_url
        parsed_url["sorting"] = sorting
        self.parsed_url = parsed_url
        return parsed_url

    def _get_profile_default_address(self) -> None:
        """Получить данные адреса аккаунта мм"""
        json_data = {}
        response_json = self._api_request("https://megamarket.ru/api/mobile/v1/profileService/address/list", json_data)
        address = [address for address in response_json["profileAddresses"] if address["isDefault"] is True]
        if address:
            self.address_id = address[0]["addressId"]
            self.region_id = address[0]["regionId"]
            self.logger.info(f"Регион: {address[0]['region']}")
            self.logger.info(f"Адрес: {address[0]['full']}")

    def _get_address_info(self, cookie_dict: dict) -> None:
        """Получить данные адреса принадлежащие аккаунту из cookies"""
        address = cookie_dict.get("address_info")
        region = cookie_dict.get("region_info")
        if region:
            region = json.loads(unquote(region))
            self.region_id = self.region_id or region["id"]
            self.logger.info("Регион: %s", region["displayName"])
        if address:
            address = json.loads(unquote(address))
            self.address_id = self.address_id or address["addressId"]
            self.logger.info("Адрес: %s", address["full"])

    def _get_address_from_string(self, address: str) -> None:
        """Установить адрес доставки по строке адреса"""
        json_data = {"count": 10, "isSkipRegionFilter": True, "query": address}
        response_json = self._api_request(
            "https://megamarket.ru/api/mobile/v1/addressSuggestService/address/suggest",
            json_data,
        )
        address = response_json.get("items")
        if address:
            self.address_id = address[0]["addressId"]
            self.region_id = address[0]["regionId"]
            self.logger.info("Регион: %s", address[0]["region"])
            self.logger.info("Адрес: %s", address[0]["full"])
        else:
            sys.exit(f"По запросу {address} адрес не найден!")

    def _get_merchant_inn(self, merchant_id: str) -> str:
        """Получить ИНН по ID продавца"""
        json_data = {"merchantId": merchant_id}
        response_json = self._api_request("https://megamarket.ru/api/mobile/v1/partnerService/merchant/legalInfo/get", json_data)
        return response_json["merchant"]["legalInfo"]["inn"]

    def _parse_item(self, item: dict):
        """Парсинг дефолтного предложения товара"""
        if item["favoriteOffer"]["merchantName"] in self.blacklist:
            self.logger.debug("Пропуск %s", item["favoriteOffer"]["merchantName"])
            return

        if self.use_merchant_blacklist:
            merchant_inn = self._get_merchant_inn(item["favoriteOffer"]["merchantId"])
            if merchant_inn in self.merchant_blacklist:
                self.logger.debug("Пропуск %s", item["favoriteOffer"]["merchantName"])
                return

        self.scraped_tems_counter += 1

        delivery_date_iso: str = item["favoriteOffer"]["deliveryPossibilities"][0].get("displayDeliveryDate", "")
        delivery_date = delivery_date_iso.split("T")[0]

        parsed_offer = ParsedOffer(
            title=item["goods"]["title"],
            price=item["favoriteOffer"]["finalPrice"],
            delivery_date=delivery_date,
            price_bonus=item["favoriteOffer"]["finalPrice"] - item["favoriteOffer"]["bonusAmount"],
            goods_id=item["goods"]["goodsId"].split("_")[0],
            bonus_amount=item["favoriteOffer"]["bonusAmount"],
            url=item["goods"]["webUrl"],
            available_quantity=item["favoriteOffer"]["availableQuantity"],
            merchant_id=item["favoriteOffer"]["merchantId"],
            merchant_name=item["favoriteOffer"]["merchantName"],
            image_url=item["goods"]["titleImage"],
        )

        parsed_offer.notified = self._notify_if_notify_check(parsed_offer)
        self._export_to_db(parsed_offer)

    def _filters_convert(self, parsed_url: dict) -> dict:
        """Конвертация фильтров каталога или поиска"""
        for url_filter in parsed_url["selectedListingFilters"]:
            if url_filter["type"] == "EXACT_VALUE":
                url_filter["type"] = 0
            if url_filter["type"] == "LEFT_BOUND":
                url_filter["type"] = 1
            if url_filter["type"] == "RIGHT_BOUND":
                url_filter["type"] = 2
        return parsed_url

    def _parse_offer(self, item: dict, offer: dict) -> None:
        """Парсинг предложения товара"""
        if offer["merchantName"] in self.blacklist:
            self.logger.debug("Пропуск %s", offer["merchantName"])
            return

        if self.use_merchant_blacklist:
            merchant_inn = self._get_merchant_inn(offer["merchantId"])
            if merchant_inn in self.merchant_blacklist:
                self.logger.debug("Пропуск %s", offer["merchantName"])
                return

        delivery_date_iso: str = offer["deliveryPossibilities"][0]["date"]
        delivery_date = delivery_date_iso.split("T")[0]

        # добавление merchantId в конец url
        offer_url: str = item["webUrl"]
        if offer_url.endswith("/"):
            offer_url = offer_url[:-1]
        offer_url = f"{offer_url}_{offer['merchantId']}"

        parsed_offer = ParsedOffer(
            delivery_date=delivery_date,
            price_bonus=offer["finalPrice"] - offer["bonusAmountFinalPrice"],
            goods_id=item["goodsId"].split("_")[0],
            title=item["title"],
            price=offer["finalPrice"],
            bonus_amount=offer["bonusAmountFinalPrice"],
            url=offer_url,
            available_quantity=offer["availableQuantity"],
            merchant_id=offer["merchantId"],
            merchant_name=offer["merchantName"],
            merchant_rating=offer["merchantSummaryRating"],
            image_url=None,
        )

        self.scraped_tems_counter += 1
        parsed_offer.notified = self._notify_if_notify_check(parsed_offer)
        self._export_to_db(parsed_offer)

    def _notify_if_notify_check(self, parsed_offer: ParsedOffer):
        """Отправить уведомление в tg если предложение подходит по параметрам"""
        time_diff = 0
        last_notified = None
        last_notified = db_utils.get_last_notified(parsed_offer.goods_id, parsed_offer.merchant_id, parsed_offer.price, parsed_offer.bonus_amount)
        last_notified = datetime.strptime(last_notified, "%Y-%m-%d %H:%M:%S") if last_notified else None
        if last_notified:
            now = datetime.now()
            time_diff = now - last_notified
              
        if self.perecup_price and (not last_notified or (last_notified and (time_diff.total_seconds() > self.alert_repeat_timeout * 3600 or not time_diff))):
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                message = self._format_tg_message(parsed_offer)
                executor.submit(self.tg_client_phone.notify, message, parsed_offer.image_url)
                return True
        else:
            if (
                parsed_offer.bonus_percent >= self.bonus_percent_alert 
                and parsed_offer.bonus_amount >= self.bonus_value_alert 
                and parsed_offer.price <= self.price_value_alert 
                and parsed_offer.price_bonus <= self.price_bonus_value_alert 
                and parsed_offer.price >= self.price_min_value_alert
                and (not last_notified or (last_notified and (time_diff.total_seconds() > self.alert_repeat_timeout * 3600 or not time_diff)))
                and self.tg_client
            ):
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    message = self._format_tg_message(parsed_offer)
                    executor.submit(self.tg_client.notify, message, parsed_offer.image_url)
                    self.perecup_price = None
                    return True
        return False

    def _format_tg_message(self, parsed_offer: ParsedOffer) -> str:
        """Форматировать данные для отправки в telegram"""
        if self.perecup_price:
            return (
            f'🛍 <b>Товар:</b> <a href="{parsed_offer.url}">{parsed_offer.title}</a>\n'
            f"💰 <b>Цена:</b> {parsed_offer.price}₽\n"
            f"💸 <b>Цена-Бонусы:</b> {parsed_offer.price_bonus}\n"
            f"🟢 <b>Бонусы:</b> {parsed_offer.bonus_amount}\n"
            f"🔢 <b>Процент Бонусов:</b> {parsed_offer.bonus_percent}\n"
            f"✅ <b>Доступно:</b> {parsed_offer.available_quantity or '?'}\n"
            f"📦 <b>Доставка:</b> {parsed_offer.delivery_date}\n"
            f"🛒 <b>Продавец:</b> {parsed_offer.merchant_name} {parsed_offer.merchant_rating}{'⭐' if parsed_offer.merchant_rating else ''}\n"
            f"💰 <b>Цена перекупа:</b> {self.perecup_price}₽\n"
            f"💰 <b>Выгода:</b> {self.perecup_price - parsed_offer.price + parsed_offer.bonus_amount}₽\n"
            f"🟢 <b>Статус закупки:</b> {self.zakup_info}"
        )
        else:
            return (
                f'🛍 <b>Товар:</b> <a href="{parsed_offer.url}">{parsed_offer.title}</a>\n'
                f"💰 <b>Цена:</b> {parsed_offer.price}₽\n"
                f"💸 <b>Цена-Бонусы:</b> {parsed_offer.price_bonus}\n"
                f"🟢 <b>Бонусы:</b> {parsed_offer.bonus_amount}\n"
                f"🔢 <b>Процент Бонусов:</b> {parsed_offer.bonus_percent}\n"
                f"✅ <b>Доступно:</b> {parsed_offer.available_quantity or '?'}\n"
                f"📦 <b>Доставка:</b> {parsed_offer.delivery_date}\n"
                f"🛒 <b>Продавец:</b> {parsed_offer.merchant_name} {parsed_offer.merchant_rating}{'⭐' if parsed_offer.merchant_rating else ''}\n"
            )

    def _get_offers(self, goods_id: str, delay: int = 0) -> list[dict]:
        """Получить список предложений товара"""
        json_data = {
            "addressId": self.address_id,
            "collectionId": None,
            "goodsId": goods_id,
            "listingParams": {
                "priorDueDate": "UNKNOWN_OFFER_DUE_DATE",
                "selectedFilters": [],
            },
            "merchantId": "0",
            "requestVersion": 11,
            "shopInfo": {},
        }
        response_json = self._api_request("https://megamarket.ru/api/mobile/v1/catalogService/productOffers/get", json_data, delay=delay)
        return response_json["offers"]

    def _get_page(self, offset: int) -> dict:
        """Получить страницу каталога или поиска"""
        json_data = {
            "requestVersion": 10,
            "limit": 44,
            "offset": offset,
            "isMultiCategorySearch": self.parsed_url.get("isMultiCategorySearch", False),
            "searchByOriginalQuery": False,
            "selectedSuggestParams": [],
            "expandedFiltersIds": [],
            "sorting": self.parsed_url["sorting"],
            "ageMore18": None,
            "addressId": self.address_id,
            "showNotAvailable": True,
            "selectedFilters": self.parsed_url.get("selectedListingFilters", []),
        }
        if self.parsed_url.get("type", "") == "TYPE_MENU_NODE":
            self.parsed_url["collection"] = self.parsed_url["collection"] or self.parsed_url["menuNode"]["collection"]
        json_data["collectionId"] = self.parsed_url["collection"]["collectionId"] if self.parsed_url["collection"] else None
        json_data["searchText"] = self.parsed_url["searchText"] if self.parsed_url["searchText"] else None
        json_data["selectedAssumedCollectionId"] = self.parsed_url["collection"]["collectionId"] if self.parsed_url["collection"] else None
        json_data["merchant"] = {"id": self.parsed_url["merchant"]["id"]} if self.parsed_url["merchant"] else None

        response_json = self._api_request(
            "https://megamarket.ru/api/mobile/v1/catalogService/catalog/search",
            json_data,
            delay=self.connection_success_delay,
        )

        if response_json.get("error"):
            raise ApiError()
        if response_json.get("success") is True:
            return response_json

    def _parse_page(self, response_json: dict) -> bool:
        """Парсинг страницы каталога или поиска"""
        items_per_page = int(response_json.get("limit"))
        if items_per_page == 0:
            # костыль для косяка мм
            return False
        page_progress = self.rich_progress.add_task(f"[orange]Страница {int(int(response_json.get('offset')) / items_per_page) + 1}")
        self.rich_progress.update(page_progress, total=len(response_json["items"]))
        x = 0
        for item in response_json["items"]:
            x += 1
            bonus_percent = item["favoriteOffer"]["bonusPercent"]
            item_title = item["goods"]["title"]
            price = item["favoriteOffer"]["price"]
            if self._exclude_check(item_title) or (item["isAvailable"] is not True) or (not self._include_check(item_title)):
                # пропускаем, если товар не доступен или исключен
                self.rich_progress.update(page_progress, advance=1)
                continue
            is_listing = self.parsed_url["type"] == "TYPE_LISTING"
            attributes = item["goods"]["attributes"]
            brand = item["goods"]["brand"]
            self.perecup_price = None
            self.zakup_info = ""
            if brand in "Apple":
                self.perecup_price = self._match_product_apple(item_title, attributes)
            elif item_title.startswith("Игровая приставка"):
                self.perecup_price = self._match_product_konsol(item_title, attributes)
            elif item_title.startswith("Игровая портативная консоль"):
                self.perecup_price = self._match_product_konsol(item_title, attributes)
            elif item_title.startswith("Шлем Sony"):
                self.perecup_price = self._match_product_shlem(item_title)
            elif item_title.startswith("Фен Dyson"):
                self.perecup_price = self._match_product_dyson(item_title)
            elif item_title.startswith("Смартфон"):
                self.perecup_price = self._match_product_smartphone(item_title, attributes)
            elif item_title.startswith("Видеокарта"):
                self.perecup_price = self._match_product_video_card(item_title, attributes)
            elif item_title.startswith("Умная колонка") or item_title.startswith("Колонка умная"):
                self.perecup_price = self._match_product_colonka(item_title)
            # match = re.search(r"Яндекс", item_title)
            # if match:
            #     filename = f"'Z'.{uuid.uuid4().hex}.json"
            #     with open(filename, "w", encoding="utf-8") as file:
            #         json.dump(item, file, indent=4, ensure_ascii=False)
            # print(item_title, self.perecup_price)
            # self.all_titles.append(item_title)
            if self.perecup_price is None:
                if bonus_percent >= self.bonus_percent_alert:
                    if self.all_cards or (not self.no_cards and (item["hasOtherOffers"] or item["offerCount"] > 1 or is_listing)):
                        self.logger.info("Парсим предложения %s", item_title)
                        offers = self._get_offers(item["goods"]["goodsId"], delay=self.connection_success_delay)
                        for offer in offers:
                            self._parse_offer(item["goods"], offer)
                    else:
                        self._parse_item(item)
            elif price < self.perecup_price:
                if self.all_cards or (not self.no_cards and (item["hasOtherOffers"] or item["offerCount"] > 1 or is_listing)):
                    self.logger.info("Парсим предложения %s ", item_title)
                    offers = self._get_offers(item["goods"]["goodsId"], delay=self.connection_success_delay)
                    for offer in offers:
                        self._parse_offer(item["goods"], offer)
                else:
                    self._parse_item(item)
            self.rich_progress.update(page_progress, advance=1)
        self.rich_progress.remove_task(page_progress)
        parse_next_page = response_json["items"] and response_json["items"][-1]["isAvailable"]
        return parse_next_page
    
    def _getOperative(self, attributes):
        """Ищет в атрибутах значение оперативной памяти и возвращает его"""
        for attribute in attributes:
            if attribute["title"].startswith("Оперативная память"):
                return attribute["value"]
        return None

    def _exclude_check(self, title: str) -> bool:
        if self.exclude:
            return self.exclude.match(title)
        return False

    def _include_check(self, title: str) -> bool:
        if self.include:
            return self.include.match(title)
        return True

    def _create_progress_bar(self) -> None:
        """Создание и запуск полосы прогресса"""
        self.rich_progress = Progress(
            "{task.description}",
            SpinnerColumn(),
            BarColumn(),
            TextColumn("[progress.percentage]{task.completed}/{task.total}"),
            TimeRemainingColumn(elapsed_when_finished=True, compact=True),
        )
        self.rich_progress.start()

    def _get_card_info(self, goods_id: str) -> dict:
        """Получить карточку товара"""
        json_data = {"goodsId": goods_id, "merchantId": "0"}
        response_json = self._api_request("https://megamarket.ru/api/mobile/v1/catalogService/productCardMainInfo/get", json_data)
        return response_json["goods"]

    def _parse_card(self) -> None:
        """Парсинг карточки товара"""
        item = self._get_card_info(self.parsed_url["goods"]["goodsId"])
        offers = self._get_offers(self.parsed_url["goods"]["goodsId"])
        self.job_name = utils.slugify(item["title"])
        self.job_id = db_utils.new_job(self.job_name)
        for offer in offers:
            self._parse_offer(item, offer)

    def _process_page(self, offset: int, main_job) -> bool:
        """Получение и парсинг страницы каталога или поиска"""
        response_json = self._get_page(offset)
        parse_next_page = self._parse_page(response_json)
        self.rich_progress.update(main_job, advance=1)
        sleep(1)
        return parse_next_page

    def _parse_multi_page(self) -> None:
        """Запуск и менеджмент парсинга каталога или поиска"""
        start_offset = 0
        response_json = self._get_page(start_offset)
        if len(response_json["items"]) == 0 and response_json["processor"]["type"] in ("MENU_NODE", "COLLECTION"):
            self.logger.debug("Редирект в каталог")
            self.url = urljoin("https://megamarket.ru", response_json["processor"]["url"])
            return self.parse()
        items_per_page = int(response_json.get("limit"))
        item_count_total = int(response_json["total"])

        pages_to_parse = list(range(start_offset, item_count_total, items_per_page))
        self._create_progress_bar()
        main_job = self.rich_progress.add_task("[green]Общий прогресс", total=len(pages_to_parse))
        max_threads = min(len(pages_to_parse), self.threads)
        while pages_to_parse:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
                futures = {executor.submit(self._process_page, page, main_job): page for page in pages_to_parse}
                for future in concurrent.futures.as_completed(futures):
                    try:
                        parse_next_page = future.result()
                    except Exception:
                        continue
                    page = futures[future]
                    if page in pages_to_parse:
                        pages_to_parse.remove(page)
                    if not parse_next_page:
                        self.logger.info("Дальше товары не в наличии, их не парсим")
                        for fut in futures:
                            future_page = futures[fut]
                            if future_page > page:
                                if future_page in pages_to_parse:
                                    pages_to_parse.remove(future_page)
                                self.rich_progress.update(main_job, total=len(pages_to_parse))
                                fut.cancel()
        self.rich_progress.stop()



# ------------------------------APPLE------------------------------

    def _match_product_apple(self, input_string, attributes):
            input_string = input_string.lower()
            if input_string.startswith("смартфон"):
                return self._match_product_phone_apple(input_string, self._match_category_phone_apple(input_string, attributes))
            elif input_string.startswith("ноутбук"):
                return self._match_product_notebook_apple(input_string, self._match_category_notebook_apple(input_string), self._processor_apple(input_string, attributes))
            elif input_string.startswith("планшет"):
                return self._match_product_planshet_apple(input_string, self._get_memory_planshet_apple(attributes), self._get_year_planshet_apple(input_string, attributes), self._get_size_planshet_apple(attributes))
            elif "наушники" in input_string:
                return self._match_product_yho_apple(input_string)
                
            return None
    
    # --------------------PHONE--------------------
    
    def _match_product_phone_apple(self, input_string: str, memory: str):
        if not memory:
            return None
        for category, products in self.categories.items():
            if input_string.startswith(category.lower()):
                for product in products:
                    if product["description"].lower() in input_string and product["memory"] in memory:
                        if product["sim"].lower() in input_string:
                            if not product["priceSim"]:
                                return None
                            self.zakup_info = product["result"]
                            return product["priceSim"]
                        if not product["price"]:
                            return None
                        self.zakup_info = product["result"]
                        return product["price"]
        return None
    
    def _match_category_phone_apple(self, input_string, attributes):
        one = ["128", "128gb", "128гб"]
        two = ["256", "256gb", "256гб"]
        three = ["512", "512gb", "512гб"]
        four = ["1024", "1024gb", "1024гб"]
        
        if any(x in input_string for x in one):
            return "128"
        elif any(x in input_string for x in two):
            return "256"
        elif any(x in input_string for x in three):
            return "512"
        elif any(x in input_string for x in four):
            return "1024"
                
        if not attributes:
            return None
        result = self._process_smartphone_apple(attributes)
        if result:
            return result
        if any(x in input_string for x in one):
            return "128"
        elif any(x in input_string for x in two):
            return "256"
        elif any(x in input_string for x in three):
            return "512"
        elif any(x in input_string for x in four):
            return "1024"
        return None
    
    def _process_smartphone_apple(self, attributes):
        return self._get_memory_smartphone_apple(attributes)
    
    def _get_memory_smartphone_apple(self, attributes):
        """Ищет в атрибутах значение оперативной памяти и возвращает его"""
        for attribute in attributes:
            if attribute["title"].startswith("Встроенная память"):
                return attribute["value"]
        return None
    
    # --------------------NOTEBOOK--------------------

    def _match_product_notebook_apple(self, input_string: str, memory: str, processor: str):
            if not memory or not processor:
                return None
            for category, products in self.categories.items():
                if input_string.startswith(category.lower()):
                    for product in products:
                        if product["code"].lower() in input_string:
                            self.zakup_info = product["result"]
                            return product["price"]
                        if product["description"].lower() in input_string and product["memory"] in memory and product["proc"] in processor:
                            self.zakup_info = product["result"]
                            return product["price"]
            return None
        
    def _match_category_notebook_apple(self, input_string):
        input_string = input_string.lower().replace(" ", "")

        memory_patterns = {
            "48": [r"48/256", r"/48/256", r"48/512", r"/48/512", r"48/1024", r"48/1000", r"48/1tb", r"48/1тб", r"/48/1024", r"/48/1000", r"/48/1tb", r"/48/1тб", r"48/2048", r"48/2000", r"48/2tb", r"48/2тб", r"/48/2048", r"/48/2000", r"/48/2tb", r"/48/2тб", r"48gb", r"/48gb", r"48гб", r"/48гб"],
            "32": [r"32/256", r"/32/256", r"32/512", r"/32/512", r"32/1024", r"32/1000", r"32/1tb", r"32/1тб", r"/32/1024", r"/32/1000", r"/32/1tb", r"/32/1тб", r"32/2048", r"32/2000", r"32/2tb", r"32/2тб", r"/32/2048", r"/32/2000", r"/32/2tb", r"/32/2тб", r"32gb", r"/32gb", r"32гб", r"/32гб"],
            "24": [r"24/256", r"/24/256", r"24/512", r"/24/512", r"24/1024", r"24/1000", r"24/1tb", r"24/1тб", r"/24/1024", r"/24/1000", r"/24/1tb", r"/24/1тб", r"24/2048", r"24/2000", r"24/2tb", r"24/2тб", r"/24/2048", r"/24/2000", r"/24/2tb", r"/24/2тб", r"24gb", r"/24gb", r"24гб", r"/24гб"],
            "16": [r"16/256", r"/16/256", r"16/512", r"/16/512", r"16/1024", r"16/1000", r"16/1tb", r"16/1тб", r"/16/1024", r"/16/1000", r"/16/1tb", r"/16/1тб", r"16/2048", r"16/2000", r"16/2tb", r"16/2тб", r"/16/2048", r"/16/2000", r"/16/2tb", r"/16/2тб", r"16gb", r"/16gb", r"16гб", r"/16гб"],
            "8": [r"8/256", r"/8/256", r"8/512", r"/8/512", r"8/1024", r"8/1000", r"8/1tb", r"8/1тб", r"/8/1024", r"/8/1000", r"/8/1tb", r"/8/1тб", r"8/2048", r"8/2000", r"8/2tb", r"8/2тб", r"/8/2048", r"/8/2000", r"/8/2tb", r"/8/2тб", r"8gb", r"/8gb", r"8гб", r"/8гб"]
        }

        hard_drive_patterns = {
            "2048": [r"2048", r"2000", r"2000gb", r"2000 gb", r"2048gb", r"2048 gb", r"2048гб", r"2048 гб", r"2000 гб", r"2000гб", r"2tb", r"2тб", r"2 tb", r"2 тб"],
            "1024": [r"1024", r"1000", r"1000gb", r"1000 gb", r"1024gb", r"1024 gb", r"1024гб", r"1024 гб", r"1000 гб", r"1000гб", r"1tb", r"1тб", r"1 tb", r"1 тб"],
            "512": [r"512gb", r"512гб", r"512"],
            "256": [r"256gb", r"256гб", r"256"]
        }

        memory = None
        memory_hard = None

        for mem, patterns in memory_patterns.items():
            if any(re.search(pattern, input_string) for pattern in patterns):
                memory = mem
                break

        for hdd, patterns in hard_drive_patterns.items():
            if any(re.search(pattern, input_string) for pattern in patterns):
                memory_hard = hdd
                break

        if memory and memory_hard:
            return f"{memory}/{memory_hard}"
        return None
    
    def _processor_apple(self, input_string, attributes):
        if not attributes:
            return None
        processor_patterns = {"M1", "M2", "M3", "M4"}
        for x in processor_patterns:
            if x in input_string:
                return x
        processor = self._getProcessor(attributes)
        for x in processor_patterns:
            if x in processor:
                return x
        return None
    
    def _getProcessor(self, attributes):
        for attribute in attributes:
            if attribute["title"].startswith("Модель процессора"):
                return attribute["value"]
        return None
    
        # --------------------PLANSHET--------------------

    def _match_product_planshet_apple(self, input_string: str, memory: str, year: str, size: str):
        if not memory or not year or not size:
            return None
        
        for category, products in self.categories.items():
            if input_string.startswith(category.lower()):
                for product in products:
                    if product["description"].lower() in input_string and product["memory"] in memory and product["year"] in year and product["diagonal"] in size:
                        if product["lte"].lower() in input_string:
                            self.zakup_info = product["result"]
                            return product["priceLte"]
                        self.zakup_info = product["result"]
                        return product["price"]
        return None
    
    def _get_memory(self, attributes):
        if not attributes:
            return None
        for attribute in attributes:
            if attribute["title"].startswith("Оперативная память"):
                return attribute["value"]
        return None
        
    def _get_memory_planshet_apple(self, attributes):
        if not attributes:
            return None
        for attribute in attributes:
            if attribute["title"].startswith("Встроенная память"):
                return attribute["value"]
        return None
    
    def _get_size_planshet_apple(self, attributes):
        if not attributes:
            return None
        for attribute in attributes:
            if attribute["title"].startswith("Диагональ экрана"):
                return attribute["value"]
        return None
    
    def _get_year_planshet_apple(self, input_string, attributes):
        if not attributes:
            return None
        for attribute in attributes:
            if attribute["title"].startswith("Год релиза"):
                return attribute["value"]
        
        year_patterns = {"2020", "2021", "2022", "2024"}
        for x in year_patterns:
            if x in input_string:
                return x
        
        return None
    
    # --------------------KONSOL--------------------

    def _match_product_konsol(self, input_string, attributes):
        input_string = input_string.lower()
        if not attributes:
            return None
        
        memory = self._get_memory_konsol(input_string, attributes)
        if not memory:
            return None
        
        for category, products in self.categories.items():
            if input_string.startswith(category.lower()):
                for product in products:
                    if product["description"].lower() in input_string:
                        for x in product["code"]:
                            if x in input_string:
                                self.zakup_info = product["result"]
                                return product["price"]
                            if product["memory"] in memory:
                                self.zakup_info = product["result"]
                                return product["price"]
        return None
    
    def _get_memory_konsol(self, input_string, attributes):
        for attribute in attributes:
            if attribute["title"].startswith("Объем встроенной памяти"):
                return attribute["value"]
        memory_patterns = {
        "512": [r"500", r"512", r"500gb", r"512gb", r"500гб", r"512гб", r"500 gb", r"512 gb", r"500 гб", r"512 гб"],
        "825": [r"825", r"825gb", r"825 gb", r"825гб", r"825 гб"],
        "1 Тб": [r"1000", r"1024", r"1000gb", r"1024gb", r"1000 gb", r"1024 gb", r"1000гб", r"1024гб", r"1000 гб", r"1024 гб", r"1tb", r"1 tb", r"1тб", r"1 тб"],
        "2 Тб": [r"2000", r"2048", r"2000gb", r"2048gb", r"2000 gb", r"2048 gb", r"2000гб", r"2048гб", r"2000 гб", r"2048 гб", r"2tb", r"2 tb", r"2тб", r"2 тб"]
        }
        
        for mem, patterns in memory_patterns.items():
            if any(re.search(pattern, input_string) for pattern in patterns):
                return mem
        return None
    
    # --------------------VR--------------------
    
    def _match_product_shlem(self, input_string: str):
        input_string = input_string.lower()
        for category, products in self.categories.items():
            if input_string.startswith(category.lower()):
                for product in products:
                    if product["description"].lower() in input_string:
                            return product["price"]
        return None
    
    # --------------------DYSON--------------------
    
    def _match_product_dyson(self, input_string: str):
        input_string = input_string.lower()
        for category, products in self.categories.items():
            if input_string.startswith(category.lower()):
                for product in products:
                    if product["code"].lower() in input_string and product["description"].lower() in input_string:
                        self.zakup_info = product["result"]
                        return product["price"]
        return None
    
    # --------------------СМАРТФОНЫ--------------------
    
    def _match_product_smartphone(self, input_string, attributes):
        input_string = input_string.lower()
        memory = self._getMemory(input_string, attributes)
        if not memory:
            return None
        
        for category, products in self.categories.items():
            if input_string.startswith(category.lower()):
                for product in products:
                    if product["description"].lower() in input_string and product["memory"] in memory:
                        self.zakup_info = product["result"]
                        if product["sim"].lower() in input_string:
                            return product["priceSim"]
                        return product["price"]
        return None
        
        
    def _getMemory(self, input_string, attributes):
        memory = None
        memory_hard = None
        if attributes:
            memory = self._get_memory(attributes)
            memory_hard = self._get_memory_planshet_apple(attributes)
            
        memory_patterns = {
            "4": [r"/4/", r" 4/", r"4gb", r"4 gb", r"4гб", r"4 гб"],
            "6": [r"/6/", r" 6/", r"6gb", r"6 gb", r"6гб", r"6 гб"],
            "8": [r"/8/", r" 8/", r"8gb", r"8 gb", r"8гб", r"8 гб"],
            "12": [r"/12/", r" 12/", r"12gb", r"12 gb", r"12гб", r"12 гб"],
            "16": [r"/16/", r" 16/", r"16gb", r"16 gb", r"16гб", r"16 гб"],
            "24": [r"/24/", r" 24/", r"24gb", r"24 gb", r"24гб", r"24 гб"],
            "32": [r"/32/", r" 32/", r"32gb", r"32 gb", r"32гб", r"32 гб"],
            "64": [r"/64", r"/64gb", r"/64 gb", r"/64гб", r"/64 гб", r" 64 gb", r"64gb", r"64гб", r"64 гб"],
            "128": [r"128", r"/128", r"/128gb", r"/128 gb", r"/128гб", r"/128 гб", r"128 gb", r"128gb", r"128гб", r"128 гб"],
            "256": [r"256", r"/256", r"/256gb", r"/256 gb", r"/256гб", r"/256 гб", r"256 gb", r"256gb", r"256гб", r"256 гб"],
            "512": [r"512", r"500", r"/512", r"/500", r"/512gb", r"/500gb", r"/512 gb", r"500 gb", r"/512гб", r"/500гб", r"/512 гб", r"/500 гб", r"512 gb", r"500 gb", r"512gb", r"500gb", r"512гб", r"500гб", r"512 гб", r"500 гб"],
            "1024": [r"1024", r"1000", r"/1024", r"/1000", r"/1024gb", r"/1000gb", r"/1024 gb", r"1000 gb", r"/1024гб", r"/1000гб", r"/1024 гб", r"/1000 гб", r"1024 gb", r"1000 gb", r"1024gb", r"1000gb", r"1024гб", r"1000гб", r"1024 гб", r"1000 гб"],
            "2048": [r"2048", r"2000", r"/2048", r"/2000", r"/2048gb", r"/2000gb", r"/2048 gb", r"2000 gb", r"/2048гб", r"/2000гб", r"/2048 гб", r"/2000 гб", r"2048 gb", r"2000 gb", r"2048gb", r"2000gb", r"2048гб", r"2000гб", r"2048 гб", r"2000 гб"]
        }
        
        if memory:
            for mem, patterns in memory_patterns.items():
                if any(re.search(pattern, memory) for pattern in patterns):
                    memory = mem
        else:
            for mem, patterns in memory_patterns.items():
                if any(re.search(pattern, input_string) for pattern in patterns):
                    memory = mem
        if memory_hard:
            for mem, patterns in memory_patterns.items():
                if any(re.search(pattern, memory) for pattern in patterns):
                    memory_hard = mem
        else:
            for mem, patterns in memory_patterns.items():
                if any(re.search(pattern, input_string) for pattern in patterns):
                    memory_hard = mem
        if not memory or not memory_hard:
            return None
        return f"{memory}/{memory_hard}"
    
    # --------------------ВИДЕОКАРТЫ--------------------
    
    def _match_product_video_card(self, input_string, attributes):
        input_string = input_string.lower()
        
        for category, products in self.categories.items():
            if input_string.startswith(category.lower()):
                for product in products:
                    if product["description"].lower() in input_string and product["name"].lower() in input_string:
                        self.zakup_info = product["result"]
                        return product["price"]
        return None
    
    # --------------------НАУШНИКИ APPLE--------------------
    
    def _match_product_yho_apple(self, input_string):
        for category, products in self.categories.items():
            if category.lower() in input_string:
                for product in products:
                    if product["description"].lower() in input_string:
                        self.zakup_info = product["result"]
                        if product["name"].lower() in input_string:
                            return product["priceYear"]
                        return product["price"]
        return None
    
    # --------------------УМНАЯ КОЛОНКА--------------------
    
    def _match_product_colonka(self, input_string):
        input_string = input_string.lower()
        for category, products in self.categories.items():
            if category.lower() in input_string:
                for product in products:
                    if product["description"].lower() in input_string and product["name"].lower() in input_string:
                        self.zakup_info = product["result"]
                        return product["price"]
        return None