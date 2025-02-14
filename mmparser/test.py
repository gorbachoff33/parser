import json
import uuid
from curl_cffi import requests

url = 'https://megamarket.ru/api/mobile/v1/catalogService/catalog/search'

headers = {
"accept": "application/json",
"accept-encoding": "gzip, deflate, br, zstd",
"accept-language": "en-US,en;q=0.9,ru;q=0.8",
"cache-control": "no-cache",
"content-length": "1285",
"content-type": "application/json",
"cookie": "spid=1737808186492_5d0f19208675d36ea6aea3ee421be6f2_r3vjagfm8xw40lcp; device_id=1202384b-db18-11ef-a934-469e2808707f; _ym_uid=173780819149127953; _ym_d=1737808191; _sa=SA1.54d3035c-603d-4a0d-bc74-a2e17170df74.1737808192; isOldUser=true; adspire_uid=AS.1531129391.1737808193; _sv=SA1.cde8b57e-6cd3-4f86-b94c-49af5eed4df5.1714480000; adtech_uid=ed91666a-ba64-42b3-baa8-fbdb45fb6f94%3Amegamarket.ru; top100_id=t1.7729504.1876589183.1737808195992; ma_cid=6936891441737808196; ma_id=1499508061701609951554; __zzatw-smm=MDA0dBA=Fz2+aQ==; ssaid=09964350-e09b-11ef-b5a0-751f21e3fb65; sbermegamarket_token=ef273939-4a26-4f96-b26a-01020d5050be; ecom_token=ef273939-4a26-4f96-b26a-01020d5050be; rrpvid=489627406146677; flocktory-uuid=a44c80f2-ef4e-4b8b-aa80-10ad5e591a50-7; rcuid=63dc01729562c1e4a1a404a7; _gcl_au=1.1.754159093.1738509401; uxs_uid=b603c440-e178-11ef-a7f3-557b38f69949; _gid=GA1.2.707644954.1739358116; _ym_isad=1; _ga_QETLGKM757=GS1.1.1739432536.1.1.1739432573.23.0.0; spst=1739446846712_63a70859cedd871b662e40010593daa3_25acb1919f2fbe6b528d4e83a474ead3; address_info=%7B%22addressId%22%3A%222gis_4504235282675152%22%2C%22full%22%3A%22%D0%9C%D0%BE%D1%81%D0%BA%D0%B2%D0%B0%2C%20%D1%83%D0%BB%D0%B8%D1%86%D0%B0%20%D0%92%D0%B0%D0%B2%D0%B8%D0%BB%D0%BE%D0%B2%D0%B0%2C%2019%22%2C%22geo%22%3A%7B%22lon%22%3A37.57968%2C%22lat%22%3A55.69961%7D%7D; __tld__=null; _ga_VD1LWDPWYX=GS1.2.1739447727.10.1.1739451256.0.0.0; _ga=GA1.1.156925095.1737808194; spjs=1739478986895_31a620a4_0134fe0a_d7f95e4accf5dfb05a98b82a6d8d744f_35UFgaj87BbPciLPBjvnBV3V4NwClWQhSfgemudncqMBrYh0DdEhLqMaXpKfDPyYUYX11DqObJXARoFvCqqrlhxw411WGm4gSDxNiQRz04J7r+oLPDAAiOAsT/YOQ9P/MPSBBTmtT7fxVa+g+Zy/QblUNJjDq5pGL3ItX1Ug0ASuu1pvgwZgiSA9fBSK5/evQA2aUYm8fB8xZXAi+67oj0j1ZQpw7E6jWjPTj7dFBKAo/CruhPGx0gp67UE5YOboE88fp560VpzlgfCAeS7eu6P2R1LkuW4g2BQ2+xfLu6c5HJw5IvbmkMlfjDgAIrOfTvlbhg2xYR7EewdXXSjaDKaz85Kp3MxBmZH3i7cZv8OuJfVtI5Ex8+pZCo2BMEZgx/qLAdikVuj02Zwye33ricbRgETv/M2c9vMFKVGtvEftJder8s6uQc9Jqfx047CxCHy9WFoTQ57gei90/iPTz1DktBBJnS0KklYH88t4+LRt4bF5Bb7KlGxHrFjiFsXxC/2v+QOUMsnRT9/lLkLyj6eKS0cPn4wJ8VTG02pfb6sNELA8Ajpvtbk3d+ozcRCbau4aLPdDE9aaeL0xvdpihIXQiUWMPJngavdH0YgdfTkA5KRQFrXblGwyrN3EuOq0e++/SaJ25hDJbKzagDFjnEb5qlWt8eEdxHwn1ws9ygrgRSFgC73tJc9xoB2GimmRTKQkaQOix0XtLPj80DzkImY7C4NZ5UTqwyvcAnsJ2Wwz1oF1WB0eOhYytakQeGZZog5eovtHl+PY2657cr3lMEL8zdAPEWhelNs/E5vWp6p14KEB3blIjxdiA0GpW2X3z/9yH9W1YdCOICva93Ro8sr27QigBOWexvr61o41451G+Px/G65OOmIgszbPai24TlP1KVBYuWftcIYcBBbGFzwIWCzH52GGLk7J1QukBdgTO63D6oKCimDzMjb++b+bxGJDFlF5b4AoQMMfga4P0h09q8wkIVQVPnt6PPTx58mgjDywX3Nl68Iuw6dJPax5U2eFlH7qSKFYMSeo0B2dV+yV1gonAfBQ6Hh+itdnZ6Nk6IgEbfRxfrNqW5KfvIyNIaCFcY+u+06WZql/lJpqhl3hsU0UqHy16/0/+4K9DhroradxFINSls5yIFUMEeARb1/nogGXQFlQ/SzYgbtqN455iDK4YYk1wExVW7EWKdJ2vZIZP/RbX/g7GFONEYEddfiZ1B+zX3/NhjBxqbzquO8tqedy+zoVxBUF0bo+Xx5J235QDVEg==; spsc=1739478984839_203a230a5d699888f08477e53e8d4634_3a64d3c2a47309e8de9b5822d4e6e249; sprfr=invalidated; _ym_visorc=b; region_info=%7B%22displayName%22%3A%22%D0%9C%D0%BE%D1%81%D0%BA%D0%BE%D0%B2%D1%81%D0%BA%D0%B0%D1%8F%20%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C%22%2C%22kladrId%22%3A%225000000000000%22%2C%22isDeliveryEnabled%22%3Atrue%2C%22geo%22%3A%7B%22lat%22%3A55.755814%2C%22lon%22%3A37.617635%7D%2C%22id%22%3A%2250%22%7D; cfidsw-smm=s39PC8yO4lWlk1tIfKC0bx7PjSys+zNq0ZBuaumLU81xGzgMdl7S7XsTOTLFQ+MVOncxwI5dWFeQyb09qOFTif7YqIgpkiBBednT+QPcM5rmlqh2/ksFdDjOmq/Zl+CYOlAIPuXvzv0mmnr62e8ymz6NFdnHNSCgYf7Nonzh; _sas=SA1.cde8b57e-6cd3-4f86-b94c-49af5eed4df5.1714480000.1739478997; ma_ss_d19e96e4-c103-40bf-a791-3dcb7460a86f=7534162501739478992.38.1739479007.3; cfidsw-smm=ndThRWuJzLN8mRzzOgQdBwuqmWQCWJASwW/PpsJY0Y9VZ3j/IKULyy0yyvPrNa8DrLoE8GVAHclgPPCGMqBI9n+9gvG6URLufppoF4kBo2LlUkTbpQc4q+fyU03O4OnyEBb/nHG4T3/ShD5i2K7V9HGvj1vv8GQZGfZtydFf; _ga_W49D2LL5S1=GS1.1.1739477704.40.1.1739479018.33.0.0; t3_sid_7729504=s1.189785273.1739477703662.1739479018325.60.10",
"origin": "https://megamarket.ru",
"pragma": "no-cache",
"priority": "u=1, i",
"referer": "https://megamarket.ru/catalog/smartfony-apple/",
"sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Google Chrome\";v=\"132\"",
"sec-ch-ua-mobile": "?0",
"sec-ch-ua-platform": "\"macOS\"",
"sec-fetch-dest": "empty",
"sec-fetch-mode": "cors",
"sec-fetch-site": "same-origin",
"user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
"x-cfids": "s39PC8yO4lWlk1tIfKC0bx7PjSys+zNq0ZBuaumLU81xGzgMdl7S7XsTOTLFQ+MVOncxwI5dWFeQyb09qOFTif7YqIgpkiBBednT+QPcM5rmlqh2/ksFdDjOmq/Zl+CYOlAIPuXvzv0mmnr62e8ymz6NFdnHNSCgYf7Nonzh",
"x-creeper": "5uXN580v-PuW0xMhF-fVOhWOrs2hzOkigMKIkJTWgGbvnFdfY0eGKgK9x9W47Q9vHMhqTJvZoF2l-6A6-rrvWuST4c1HvGllclSuc9oy9HytHxZHwGufyzgHuuASJY663PIRqI1F_7WDRkd6cW9h6fwGzY1zylP56O15IObyJaT6Pv5-8snoBO_II3MtMTmqUzclJNtJR593agXf5u2ml5Rm0ALpcQWuAFSgtW9qqO2gYcq9U1VkOV1LjQJ16cFtHE5MktUKdD5YBS7tGY2H6wWwY8o=",
"x-requested-with": "XMLHttpRequest"
}

body = {"requestVersion":12,"merchant":{},"limit":24,"offset":0,"isMultiCategorySearch":False,"searchByOriginalQuery":False,"selectedSuggestParams":[],"expandedFiltersIds":[],"sorting":0,"ageMore18":None,"addressId":"2gis_4504235282675152","showNotAvailable":True,"selectedAssumedCollectionId":"16218","selectedFilters":[],"collectionId":"16218","auth":{"locationId":"50","appPlatform":"WEB","appVersion":0,"experiments":{"8":"1","55":"2","58":"2","62":"1","64":"2","68":"2","69":"2","79":"3","84":"1","96":"2","99":"1","107":"2","109":"2","119":"2","120":"2","121":"2","122":"2","130":"1","132":"2","144":"3","147":"3","154":"2","163":"2","173":"1","178":"1","182":"1","184":"3","186":"2","190":"1","192":"2","194":"3","200":"2","205":"2","209":"1","218":"1","229":"1","243":"1","249":"3","255":"1","644":"3","645":"3","646":"2","723":"2","775":"1","777":"2","778":"2","790":"1","794":"3","797":"2","805":"2","807":"2","816":"2","818":"3","826":"2","828":"2","829":"2","837":"2","842":"2","844":"2","846":"1","848":"2","851":"2","884":"2","885":"3","889":"1","890":"2","894":"1","897":"1","899":"3","903":"1","910":"2","945":"1","956":"2","958":"2","960":"1","961":"2","962":"3","1054":"2","1055":"2","1095":"1","5779":"2","20121":"2","70070":"1","85160":"2","91562":"3"},"os":"UNKNOWN_OS"}}

url_a = "https://megamarket.ru/api/mobile/v1/urlService/url/parse"
json_data = {"url": "https://megamarket.ru/catalog/smartfony-apple/"}
json_data["auth"] = {
            "locationId": 50,
            "appPlatform": "WEB",
            "appVersion": 1710405202,
            "experiments": {},
            "os": "UNKNOWN_OS",
        }
res = requests.post(url, headers=headers, json=body, verify=False, impersonate="chrome120")
print(res.json())
filename = f"'Z'.{uuid.uuid4().hex}.json"
with open(filename, "w", encoding="utf-8") as file:
  json.dump(res.json(), file, indent=4, ensure_ascii=False)

# from selenium import webdriver
# from selenium.webdriver.chrome.service import Service
# from selenium.webdriver.chrome.options import Options

# # Указываем путь к драйверу (он уже в /usr/local/bin)
# service = Service("/usr/local/bin/chromedriver")

# # Опции для браузера
# options = Options()
# options.add_argument("--headless")  # Без графического интерфейса

# # Запускаем браузер
# driver = webdriver.Chrome(service=service, options=options)
# driver.get("https://www.google.com")

# # Проверяем, всё ли работает
# print(driver.title)

# # Закрываем браузер
# driver.quit()
