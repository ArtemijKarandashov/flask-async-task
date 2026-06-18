import asyncio
import aiohttp
from flask import Flask, render_template, request
from typing import List, Dict, Any, Set
import logging

app = Flask(__name__)
app.config['DEBUG'] = True

USERS_URL = 'https://my-json-server.typicode.com/ArtemijKarandashov/flask-async-task/users'
POSTS_URL = 'https://my-json-server.typicode.com/ArtemijKarandashov/flask-async-task/posts'


async def fetch_json(session: aiohttp.ClientSession, url: str, timeout: int = 10) -> List[Dict[str, Any]]:
    try:
        async with session.get(url, timeout=timeout) as response:
            if response.status == 200:
                data = await response.json()
                # Если приходит объект (для /users/{id}) оборачиваем в список
                if isinstance(data, dict):
                    return [data]
                return data
            else:
                logging.warning(f'Ошибка HTTP {response.status} при запросе {url}')
                return []
    except asyncio.TimeoutError:
        logging.error(f'Таймаут при запросе {url}')
        return []
    except aiohttp.ClientError as e:
        logging.error(f'Ошибка клиента при запросе {url}: {e}')
        return []
    except Exception as e:
        logging.error(f'Неизвестная ошибка при запросе {url}: {e}')
        return []


async def fetch_users(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    return await fetch_json(session, USERS_URL)


async def fetch_posts(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    return await fetch_json(session, POSTS_URL)


async def fetch_user_by_id(session: aiohttp.ClientSession, user_id: int) -> Dict[str, Any]:
    url = f'{USERS_URL}/{user_id}'
    result = await fetch_json(session, url)
    return result[0] if result else None


def filter_users(users: List[Dict[str, Any]], username_sub: str = None, email_sub: str = None) -> List[Dict[str, Any]]:
    """
    Фильтрует список пользователей по подстрокам в полях username или email
    """
    if not username_sub and not email_sub:
        return users

    filtered = set()
    for user in users:
        match = False
        if username_sub:
            if username_sub.lower() in user.get('username', '').lower():
                match = True
        if email_sub and not match:
            if email_sub.lower() in user.get('email', '').lower():
                match = True
        if match:
            filtered.add(user['id'])

    return [user for user in users if user['id'] in filtered]


def filter_posts(posts: List[Dict[str, Any]], title_sub: str = None, body_sub: str = None) -> List[Dict[str, Any]]:
    """
    Фильтрует список постов по подстрокам в полях title или body
    """
    if not title_sub and not body_sub:
        return posts

    filtered = set()
    for post in posts:
        match = False
        if title_sub:
            if title_sub.lower() in post.get('title', '').lower():
                match = True
        if body_sub and not match:
            if body_sub.lower() in post.get('body', '').lower():
                match = True
        if match:
            filtered.add(post['id'])
    return [post for post in posts if post['id'] in filtered]


async def process_search(username_sub: str, email_sub: str, title_sub: str, body_sub: str):
    """
    Параллельно запрашивает пользователей и посты, фильтрует, для найденных постов асинхронно подгружает авторов (параллельно через gather).
    """
    async with aiohttp.ClientSession() as session:
        tasks = []
        need_users = bool(username_sub or email_sub)
        need_posts = bool(title_sub or body_sub)

        if need_users:
            tasks.append(fetch_users(session))
        if need_posts:
            tasks.append(fetch_posts(session))

        if not tasks:
            return [], []

        results = await asyncio.gather(*tasks, return_exceptions=True)

        users = []
        posts = []
        idx = 0
        if need_users:
            users = results[idx] if not isinstance(results[idx], Exception) else []
            idx += 1
        if need_posts:
            posts = results[idx] if not isinstance(results[idx], Exception) else []
            idx += 1
    
        filtered_users = filter_users(users, username_sub, email_sub) if need_users else []

        filtered_posts = filter_posts(posts, title_sub, body_sub) if need_posts else []

        posts_with_authors = []
        if filtered_posts:
            user_ids = {post['userId'] for post in filtered_posts}
            user_tasks = [fetch_user_by_id(session, uid) for uid in user_ids]
            users_by_id = await asyncio.gather(*user_tasks, return_exceptions=True)
            user_dict = {}
            for uid, user_data in zip(user_ids, users_by_id):
                if not isinstance(user_data, Exception) and user_data:
                    user_dict[uid] = user_data

            for post in filtered_posts:
                author = user_dict.get(post['userId'])
                post_copy = post.copy()
                post_copy['author_name'] = author.get('name', 'Unknown') if author else 'Unknown'
                # Ограничиваем тело первыми 150 символами
                post_copy['body_preview'] = post['body'][:150] + ('...' if len(post['body']) > 150 else '')
                posts_with_authors.append(post_copy)

        return filtered_users, posts_with_authors


@app.route('/', methods=['GET', 'POST'])
def index():
    """Главная страница с формой и результатами."""
    users_result = []
    posts_result = []
    form_data = {'username': '', 'email': '', 'title': '', 'body': ''}
    error = None

    if request.method == 'POST':
        username_sub = request.form.get('username', '').strip()
        email_sub = request.form.get('email', '').strip()
        title_sub = request.form.get('title', '').strip()
        body_sub = request.form.get('body', '').strip()

        form_data = {'username': username_sub, 'email': email_sub, 'title': title_sub, 'body': body_sub}

        if not any([username_sub, email_sub, title_sub, body_sub]):
            error = 'Заполните хотя бы одно поле для поиска.'
        else:
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    users_result, posts_result = loop.run_until_complete(
                        process_search(username_sub, email_sub, title_sub, body_sub)
                    )
                finally:
                    loop.close()
            except Exception as e:
                logging.exception('Ошибка при выполнении асинхронного поиска')
                error = f'Произошла ошибка: {str(e)}'

    return render_template(
        'index.html',
        users=users_result,
        posts=posts_result,
        form=form_data,
        error=error
    )

if __name__ == '__main__':
    app.run(debug=True)