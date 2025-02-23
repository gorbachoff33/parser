from dataclasses import dataclass, field
from typing import Optional
from time import sleep, time
import threading


@dataclass
class ParsedOffer:
    title: str
    url: str
    image_url: str
    price: float
    price_bonus: int
    bonus_amount: int
    available_quantity: int
    goods_id: str
    delivery_date: str
    merchant_id: str
    merchant_name: str
    merchant_rating: Optional[bool] = field(default=None)
    notified: Optional[bool] = field(default=None)

    @property
    def bonus_percent(self) -> int:
        """Рассчитать процент бонусов от цены"""
        if self.price == 0:
            return 0
        return int((self.bonus_amount / self.price) * 100)


class Connection:
    mutex = threading.Lock()
    shared_resource = 0

    def __init__(self, proxy: str | None):
        self.proxy_string: str | None = proxy
        self.usable_at: int = 0
        self.busy = False
        self.count: int = 0
        self.connections: list[Connection] = []

    def add_connection(self, proxies: 'Connection'):
        self.connections.extend(
            Connection(proxy.proxy_string) for proxy in proxies
        )

    def _get_connection(self) -> str:
        """Получить самое позднее использованное `Соединение`"""
        self.mutex.acquire()
        while True:
            free_proxies = [proxy for proxy in self.connections if not proxy.busy]
            if free_proxies:
                oldest_proxy = min(free_proxies, key=lambda obj: obj.usable_at)
                current_time = time()
                if oldest_proxy.usable_at <= current_time:
                    self.mutex.release()
                    return oldest_proxy
