import requests
from bs4 import BeautifulSoup
import uuid
import datetime
import sqlite3
import time
import re

start_time = time.time()

connection = sqlite3.connect(r"C:\Users\Пользователь\Desktop\ML 5.1\dvnovosti.db")
cursor = connection.cursor()


def extract_comments_count(soup):
    # Извлекаем количество комментариев из страницы
    try:
        # Ищем элемент с комментариями (в вашем примере в теге strong)
        comments_text = soup.find('strong', string=re.compile(r'Комментарии:'))
        if comments_text:
            comment_match = re.search(r'Комментарии:\s*(\d+)', comments_text.text)
            if comment_match:
                return int(comment_match.group(1))

        return 0

    except Exception as e:
        print(f"Ошибка при извлечении количества комментариев: {e}")
        return 0


# Обрабатываем статьи
def articles(url):
    try:
        resp = requests.get(url)
        resp.raise_for_status()
    except requests.exceptions.ConnectionError:
        return 'Соединение не возможно'
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при загрузке страницы {url}: {e}")
        return f"Ошибка: {e}"

    html = resp.text
    soup = BeautifulSoup(html, 'html.parser')

    guid = str(uuid.uuid4())

    # Извлекаем заголовок из h1
    title = ""
    h1_element = soup.find('h1')
    if h1_element:
        title = h1_element.text.strip()
    elif soup.title:
        title = soup.title.string.strip()

    # Собираем весь текст статьи из div с классом detail_text
    description_parts = []
    article_div = soup.find('div', class_='detail_text')
    if article_div:
        for t in article_div.find_all('p'):
            text = t.get_text().strip()
            if text:  # добавляем только непустые параграфы
                description_parts.append(text)
    else:
        # Если не нашли нужный div, собираем все параграфы
        for t in soup.find_all('p'):
            text = t.get_text().strip()
            if text:
                description_parts.append(text)

    # Объединяем текст статьи
    cleaned_description = '\n\n'.join(description_parts)

    # Извлекаем дату публикации из div с alert-secondary
    published_at = None
    try:
        date_div = soup.find('div', class_='alert-secondary')
        if date_div:
            date_span = date_div.find('span')
            if date_span:
                date_text = date_span.get_text().strip()
                # Парсим дату в формате DD.MM.YYYY
                date_match = re.search(r'(\d{2})\.(\d{2})\.(\d{4})', date_text)
                if date_match:
                    day, month, year = date_match.groups()
                    published_at = f"{year}-{month}-{day}"
    except (ValueError, AttributeError, Exception) as e:
        print(f"Ошибка при обработке даты: {e}")

    # Извлекаем количество комментариев
    comments_count = extract_comments_count(soup)

    created_at_utc = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    rating = 0

    # Вставляем данные в базу
    try:
        cursor.execute(
            'INSERT INTO articles (guid, title, description, url, published_at, comments_count, created_at_utc, rating) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            (guid, title, cleaned_description, url, published_at, comments_count, created_at_utc, rating))
    except sqlite3.Error as e:
        print(f"Ошибка при вставке в базу данных: {e}")
        return f"Ошибка БД: {title}"

    return f"{title[:50]}... - Дата: {published_at}, Комментарии: {comments_count}"


i = 1
n = 1
max_pages = 110  # Уменьшим для теста

# Парсим статьи
while i <= max_pages:
    try:
        print(f"\nОбработка страницы {i}...")
        resp = requests.get(f'https://1sn.ru/rubric/vlast-i-politika?page={i}', timeout=10)
        resp.raise_for_status()

        html = resp.text
        soup = BeautifulSoup(html, 'html.parser')

        # Ищем таблицу со статьями
        table = soup.find('table', class_='table')
        if table:
            # Находим все строки таблицы
            rows = table.find_all('tr')

            for row in rows:
                # В каждой строке ищем ссылку
                link = row.find('a')
                if link:
                    href = link.get('href', '')
                    if href:
                        # Формируем полный URL
                        if href.startswith('/'):
                            full_url = 'https://1sn.ru' + href
                        else:
                            full_url = href

                        print(f'{n}. ', end='')
                        result = articles(full_url)
                        print(result)
                        n += 1
                        time.sleep(1)  # Пауза между запросами
        else:
            print(f"На странице {i} не найдена таблица со статьями")
            break

        i += 1

    except requests.exceptions.RequestException as e:
        print(f"Ошибка при загрузке страницы {i}: {e}")
        break
    except Exception as e:
        print(f"Неожиданная ошибка на странице {i}: {e}")
        break

connection.commit()
connection.close()

end_time = time.time()

print()
total_seconds = end_time - start_time
hours = int(total_seconds // 3600)
minutes = int((total_seconds % 3600) // 60)
seconds = int(total_seconds % 60)

print(f'Время выполнения: {hours:02d}:{minutes:02d}:{seconds:02d}')
print(f'Обработано статей: {n - 1}')