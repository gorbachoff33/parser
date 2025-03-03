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
from confluent_kafka import Producer

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
        perekup: bool = True
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
        self.connection: Connection = Connection(None)
        self.parsed_proxies: set | None = None
        self.categories: dict = None
        self.cookie_dict: dict = None
        self.profile: dict = {}
        self.rich_progress = None
        self.job_name: str = ""

        self.logger: logging.Logger = self._create_logger(self.log_level)
        self.tg_client_error: TelegramClient = None

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
        self.perecup: bool = perekup
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
        self.naming_product_for_tg_chat = ""

        self.price = None
        self.bonus_amount = None
        
        self.producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'mygroup',
            'auto.offset.reset': 'earliest'
        }
        )

        
        self.category_methods = {
            "Apple": self._match_product_apple,
            "Игровая приставка": self._match_product_konsol,
            "Игровая портативная консоль": self._match_product_konsol,
            "Шлем Sony" : self._match_product_shlem,
            "Фен Dyson" : self._match_product_dyson,
            "Смартфон" : self._match_product_smartphone,
            "Видеокарта" : self._match_product_video_card,
            "Умная колонка" : self._match_product_colonka,
            "перфоратор" : self._match_product_perf,
            "высокого давления karcher" : self._match_product_karcher,
            "пылесос karcher" : self._match_product_pilesos_karcher,
            "телевизор sber" : self._match_product_sber,
            "геймпад" : self._match_product_gamepad
        }
        
        self.chat_name = {
            "Смартфон": "Смартфон",
            "Видеокарта": "Компьютер",
            "Материнская плата": "Компьютер",
            "Ноутбук": "Ноутбук",
            "Монитор": "Монитор"
        }

        self._set_up()
        self.session = self._new_session()

    def _new_session(self) -> requests.Session:
        """Создание новой сессии"""
        session = requests.Session(impersonate="chrome")
        session.cookies.update(self.cookie_dict)
        session.cookies["adult_disclaimer_confirmed"] = "1"
        return session

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
        self.connection.add_connection([Connection(None)])
        if self.proxy:
            is_valid_proxy = utils.proxy_format_check(self.proxy)
            if not is_valid_proxy:
                raise ConfigError(f"Прокси {self.proxy} не верного формата!")
            self.connection.add_connection([Connection(self.proxy)])
        elif self.parsed_proxies:
            for proxy in self.parsed_proxies:
                is_valid_proxy = utils.proxy_format_check(proxy)
                if not is_valid_proxy:
                    raise ConfigError(f"Прокси {proxy} не верного формата!")
            self.connection.add_connection([Connection(proxy) for proxy in self.parsed_proxies])


    def _api_request(self, api_url: str, json_data: dict, delay: float = 0) -> dict:
        json_data["addressId"] = ""
        json_data["auth"] = {
            "locationId":"50",
            "appPlatform":"WEB",
            "appVersion":0,
            "os":"UNKNOWN_OS"
        }
        for i in range(0, 2):
            proxy: Connection = self.connection._get_connection()
            proxy.busy = True
            self.logger.debug("Прокси : %s", proxy.proxy_string)
            try:
                # response = requests.post(api_url, headers=headers, json=json_data, proxy=proxy.proxy_string, verify=False, impersonate="chrome120")
                response = self.session.post(api_url, json=json_data, proxy=proxy.proxy_string, verify=False)
                response_data: dict = response.json()
            except Exception:
                response = None
            if response and response.status_code == 200 and not response_data.get("error"):
                proxy.usable_at = time() + delay
                proxy.busy = False
                return response_data
            if response and response.status_code == 200 and response_data.get("code") == 7:
                self.logger.debug("Соединение %s: слишком частые запросы", proxy.proxy_string)
                error = response_data.get("error")
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    message = (
                        f"🔴<b>Ошибка:</b> Ошибка получения данных api: {error}\n"
                        f"🔷 <b>Заблокирован ip:</b> {proxy.proxy_string}")
                    executor.submit(self.tg_client_error.notify, message, None)
                proxy.usable_at = time() + 3660
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
            self.tg_client_error = TelegramClient(self.tg_config[0], self.logger)
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
        if not Path(db_utils.FILENAME).exists():
            db_utils.create_db()

    def parse(self) -> None:
        """Метод запуска парсинга"""
        if self.address:
            self._get_address_from_string(self.address)
        with concurrent.futures.ThreadPoolExecutor() as executor:
                    message = f"🟢 <b>Статус:</b> Запуск успешный - server каша от 50%"
                    executor.submit(self.tg_client_error.notify, message, None)
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
            self.logger.info("%s", self.start_time.strftime("%d-%m-%Y %H:%M:%S"))
        else:
            self.logger.info("%s", self.start_time.strftime("%d-%m-%Y %H:%M:%S"))
            self._parse_multi_page()
            self.logger.info("Спаршено %s товаров", self.scraped_tems_counter)

    def _read_blacklist_file(self):
        blacklist_file_contents: str = open(self.blacklist_path, "r", encoding="utf-8").read()
        self.blacklist = [line for line in blacklist_file_contents.split("\n") if line]

    def _export_to_db(self, parsed_offer: ParsedOffer) -> None:
        """Экспорт одного предложения в базу данных"""
        with self.lock:
            db_utils.add_to_db(
                parsed_offer.goods_id,
                parsed_offer.merchant_id,
                parsed_offer.url,
                parsed_offer.title,
                self.price,
                parsed_offer.price_bonus,
                self.bonus_amount,
                parsed_offer.bonus_percent,
                parsed_offer.available_quantity,
                parsed_offer.delivery_date,
                parsed_offer.merchant_name,
                parsed_offer.merchant_rating,
                parsed_offer.notified,
            )

    def parse_input_url(self) -> dict:
        """Парсинг url мм с использованием api самого мм"""
        json_data = {"url": self.url}
        response_json = self._api_request(
            "https://megamarket.ru/api/mobile/v1/urlService/url/parse",
            json_data
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
        time_diff = 0
        last_notified = self._get_last_notifaed(parsed_offer.goods_id, parsed_offer.merchant_id, parsed_offer.price, parsed_offer.bonus_amount)
        if last_notified:
            now = datetime.now()
            time_diff = now - last_notified
        if not last_notified or (last_notified and (time_diff.total_seconds() > self.alert_repeat_timeout * 3600 or not time_diff)):
            if not last_notified or (parsed_offer.price_bonus < self.perecup_price):
                parsed_offer.notified = self._notify_if_notify_check(parsed_offer)
                self._export_to_db(parsed_offer)

    def _notify_if_notify_check(self, parsed_offer: ParsedOffer):
        """Отправить уведомление в Kafka, если предложение подходит по параметрам"""
        topic = "MM.PARSER.V1"
        message = json.dumps(self._format_tg_message(parsed_offer))
                
        if self.perecup_price:
            headers = [("telegram_room", "perekup")]
            self.producer.produce(topic, value=message, headers=headers)
            self.producer.flush()
            return True
        else:
            # print(parsed_offer.title, "bonus_percent =", parsed_offer.bonus_percent, "bonus_percent_alert =", self.bonus_percent_alert, "itog =", parsed_offer.bonus_percent >= self.bonus_percent_alert, 
            #       "bonus_amount =", parsed_offer.bonus_amount, "bonus_value_alert =", self.bonus_value_alert, "itog =", parsed_offer.bonus_amount >= self.bonus_value_alert,
            #       "price =", parsed_offer.price, "price_value_alert =", self.price_value_alert, "itog =", parsed_offer.price <= self.price_value_alert,
            #       "price_bonus =", parsed_offer.price_bonus, "price_bonus_value_alert =", self.price_bonus_value_alert, "itog =", parsed_offer.price_bonus <= self.price_bonus_value_alert,
            #       "price =", parsed_offer.price, "price_min_value_alert =", self.price_min_value_alert, "itog =", parsed_offer.price >= self.price_min_value_alert)
            if (
                parsed_offer.bonus_percent >= self.bonus_percent_alert
                and parsed_offer.bonus_amount >= self.bonus_value_alert
                and parsed_offer.price <= self.price_value_alert
                and parsed_offer.price_bonus <= self.price_bonus_value_alert
                and parsed_offer.price >= self.price_min_value_alert
            ):
                print(parsed_offer.title, "SUUUUUUUUCCCCCCCEEEEEEESSSSSS")
                if "Смартфон" in self.naming_product_for_tg_chat:
                    headers = [("telegram_room", "phone")]
                elif "Компьютер" in self.naming_product_for_tg_chat:
                    headers = [("telegram_room", "computer")]
                elif "Ноутбук" in self.naming_product_for_tg_chat:
                    headers = [("telegram_room", "notebook")]
                elif "Монитор" in self.naming_product_for_tg_chat:
                    headers = [("telegram_room", "monitor")]
                else:
                    headers = [("telegram_room", "client")]

                self.producer.produce(topic, value=message, headers=headers)
                self.producer.flush()
                self.perecup_price = None
                return True
            else:
                self.logger.info(f"Условия не удовлетворены для предложения: {parsed_offer}")
                
        return False

    def _format_tg_message(self, parsed_offer: ParsedOffer) -> str:
        """Форматировать данные для отправки в telegram"""
        if self.perecup_price:
            return {
                "url": parsed_offer.url,
                "title": parsed_offer.title,
                "price": parsed_offer.price,
                "price-bonus": parsed_offer.price_bonus,
                "bonus": parsed_offer.bonus_amount,
                "percent": parsed_offer.bonus_percent,
                "size": parsed_offer.available_quantity or '?',
                "dostavka": parsed_offer.delivery_date,
                "prodavec-name": parsed_offer.merchant_name,
                "prodavec-rating": parsed_offer.merchant_rating or '',
                "perecup": self.perecup_price,
                "vigoda": self.perecup_price - parsed_offer.price + parsed_offer.bonus_amount,
                "status": self.zakup_info,
                "server": "mainserver"
            }
        else:
            return {
                "url": parsed_offer.url,
                "title": parsed_offer.title,
                "price": parsed_offer.price,
                "price-bonus": parsed_offer.price_bonus,
                "bonus": parsed_offer.bonus_amount,
                "percent": parsed_offer.bonus_percent,
                "size": parsed_offer.available_quantity or '?',
                "dostavka": parsed_offer.delivery_date,
                "prodavec-name": parsed_offer.merchant_name,
                "prodavec-rating": parsed_offer.merchant_rating or '',
                "server": "mainserver"
            }

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
        for item in response_json["items"]:
            bonus_percent = item["favoriteOffer"]["bonusPercent"]
            item_title = item["goods"]["title"]
            self.price = item["favoriteOffer"]["price"]
            self.bonus_amount = item["favoriteOffer"]["bonusAmount"]
            if self._exclude_check(item_title) or (item["isAvailable"] is not True) or (not self._include_check(item_title)):
                # пропускаем, если товар не доступен или исключен
                self.rich_progress.update(page_progress, advance=1)
                continue
            is_listing = self.parsed_url["type"] == "TYPE_LISTING"
            attributes = item["goods"]["attributes"]
            self.perecup_price = None
            self.zakup_info = ""
            self.naming_product_for_tg_chat = ""
            if self.perecup:
                for category, method in self.category_methods.items():
                    if category.lower() in item_title.lower():
                        method(item_title, attributes)
                        break
            # match = re.search(r"Яндекс", item_title)
            # if match:
            # filename = f"'Z'.{uuid.uuid4().hex}.json"
            # with open(filename, "w", encoding="utf-8") as file:
            #     json.dump(item, file, indent=4, ensure_ascii=False)
            # print(item_title, self.perecup_price)
            # self.all_titles.append(item_title)
            time_diff = 0
            last_notified = self._get_last_notifaed(item["goods"]["goodsId"].split("_")[0], item["favoriteOffer"]["merchantId"],  self.price, self.bonus_amount)
            if last_notified:
                now = datetime.now()
                time_diff = now - last_notified
            if not last_notified or (last_notified and (time_diff.total_seconds() > self.alert_repeat_timeout * 3600 or not time_diff)):
                if self.perecup_price is None:
                    for key, name in self.chat_name.items():
                        if item_title.startswith(key):
                            self.naming_product_for_tg_chat = name
                            break
                    if bonus_percent >= self.bonus_percent_alert:
                        if self.all_cards or (not self.no_cards and (item["hasOtherOffers"] or item["offerCount"] > 1 or is_listing)):
                            self.logger.info("Парсим предложения %s", item_title)
                            print(bonus_percent)
                            offers = self._get_offers(item["goods"]["goodsId"], delay=self.connection_success_delay)
                            for offer in offers:
                                self.price = offer["finalPrice"]
                                self.bonus_amount = offer["bonusAmountFinalPrice"]
                                self._parse_offer(item["goods"], offer)
                        else:
                            self._parse_item(item)
                elif self.price < self.perecup_price:
                    # filename = f"'Z'.{uuid.uuid4().hex}.json"
                    # with open(filename, "w", encoding="utf-8") as file:
                    #     json.dump(item, file, indent=4, ensure_ascii=False)
                    if self.all_cards or (not self.no_cards and (item["hasOtherOffers"] or item["offerCount"] > 1 or is_listing)):
                        self.logger.info("Парсим предложения %s ", item_title)
                        offers = self._get_offers(item["goods"]["goodsId"], delay=self.connection_success_delay)
                        for offer in offers:
                            self.price = offer["finalPrice"]
                            self.bonus_amount = offer["bonusAmountFinalPrice"]
                            self._parse_offer(item["goods"], offer)
                    else:
                        self._parse_item(item)
            self.rich_progress.update(page_progress, advance=1)
        self.rich_progress.remove_task(page_progress)
        parse_next_page = response_json["items"] and response_json["items"][-1]["isAvailable"]
        return parse_next_page
    
    def _get_last_notifaed(self, goodsId, merchantId, price, bonus_amount):
        last_notified = db_utils.get_last_notified(goodsId, merchantId, price, bonus_amount)
        return datetime.strptime(last_notified, "%Y-%m-%d %H:%M:%S") if last_notified else None
    
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
            self.price = offer["finalPrice"]
            self.bonus_amount = offer["bonusAmountFinalPrice"]
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
                        # self.logger.info("Дальше товары не в наличии, их не парсим")
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
                return self._match_product_yho_apple(input_string, attributes)
                
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
    
    def _match_product_shlem(self, input_string, attributes):
        input_string = input_string.lower()
        for category, products in self.categories.items():
            if input_string.startswith(category.lower()):
                for product in products:
                    if product["description"].lower() in input_string:
                            return product["price"]
        return None
    
    # --------------------DYSON--------------------
    
    def _match_product_dyson(self, input_string, attributes):
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
    
    def _match_product_yho_apple(self, input_string, attributes):
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
    
    def _match_product_colonka(self, input_string, attributes):
        input_string = input_string.lower()
        for category, products in self.categories.items():
            if category.lower() in input_string:
                for product in products:
                    if product["description"].lower() in input_string and product["name"].lower() in input_string:
                        self.zakup_info = product["result"]
                        return product["price"]
        return None
    
    # --------------------ПЕРФОРАТОР--------------------
    
    def _match_product_perf(self, input_string, attributes):
        input_string = input_string.lower()
        for category, products in self.categories.items():
            if category.lower() in input_string:
                for product in products:
                    if product["description"].lower() in input_string and product["name"].lower() in input_string:
                        self.zakup_info = product["result"]
                        return product["price"]
        return None
    
    # --------------------KARKHER--------------------
    
    def _match_product_karcher(self, input_string, attributes):
        input_string = input_string.lower()
        for category, products in self.categories.items():
            if category.lower() in input_string:
                for product in products:
                    if product["description"].lower() in input_string and product["name"].lower() in input_string:
                        self.zakup_info = product["result"]
                        return product["price"]
        return None
    
    def _match_product_pilesos_karcher(self, input_string, attributes):
        input_string = input_string.lower()
        for category, products in self.categories.items():
            if category.lower() in input_string:
                for product in products:
                    if product["description"].lower() in input_string and product["name"].lower() in input_string:
                        self.zakup_info = product["result"]
                        return product["price"]
        return None
    
    # --------------------SBER--------------------
    
    def _match_product_sber(self, input_string, attributes):
        input_string = input_string.lower()
        for category, products in self.categories.items():
            if category.lower() in input_string:
                for product in products:
                    if product["description"].lower() in input_string:
                        self.zakup_info = product["result"]
                        return product["price"]
        return None
    
    def _match_product_gamepad(self, input_string, attributes):
        input_string = input_string.lower()
        for category, products in self.categories.items():
            if category.lower() in input_string:
                for product in products:
                    if product["description"].lower() in input_string:
                        self.zakup_info = product["result"]
                        if product["name"].lower() in input_string:
                            return product["priceEdge"]
                        return product["price"]
        return None