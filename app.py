import asyncio
from flask import Flask, render_template, request
import aiohttp

app = Flask(__name__)

BASE_URL = "https://my-json-server.typicode.com/typicode/demo"

async def fetch_users(session):
    """Получает список пользователей с API."""
    url = f"{BASE_URL}/users"
    try:
        async with session.get(url, timeout=10) as response:
            response.raise_for_status()
            return await response.json()
    except asyncio.TimeoutError:
        print("Таймаут при получении пользователей")
        return []
    except aiohttp.ClientError as e:
        print(f"Ошибка API users: {e}")
        return []


async def fetch_posts(session):
    """Получает список постов с API."""
    url = f"{BASE_URL}/posts"
    try:
        async with session.get(url, timeout=10) as response:
            response.raise_for_status()
            return await response.json()
    except asyncio.TimeoutError:
        print("Таймаут при получении постов")
        return []
    except aiohttp.ClientError as e:
        print(f"Ошибка API posts: {e}")
        return []


async def fetch_author(session, user_id):
    """Получает пользователя по ID. Используется для отображения автора поста."""
    url = f"{BASE_URL}/users/{user_id}"
    try:
        async with session.get(url, timeout=10) as response:
            response.raise_for_status()
            return await response.json()
    except:
        return {"name": "Неизвестный автор"}


@app.route("/", methods=["GET", "POST"])
async def index():
    users_result = []
    posts_result = []
    error_message = None
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        email = request.form.get("email", "").strip().lower()
        title = request.form.get("title", "").strip().lower()
        body = request.form.get("body", "").strip().lower()

        if not any([username, email, title, body]):
            error_message = "Заполните хотя бы одно поле поиска."

            return render_template(
                "index.html",
                users=[],
                posts=[],
                error=error_message
            )

        try:

            async with aiohttp.ClientSession() as session:
                tasks = []
                need_users = username or email
                if need_users:
                    tasks.append(fetch_users(session))
                else:
                    tasks.append(asyncio.sleep(0, result=[]))
                need_posts = title or body

                if need_posts:
                    tasks.append(fetch_posts(session))
                else:
                    tasks.append(asyncio.sleep(0, result=[]))
                users_data, posts_data = await asyncio.gather(*tasks)

                if users_data:
                    for user in users_data:
                        username_match = True
                        email_match = True
                        if username:
                            username_match = (
                                username in user["username"].lower()
                            )
                        if email:
                            email_match = (
                                email in user["email"].lower()
                            )
                        if username_match and email_match:
                            users_result.append(user)

                filtered_posts = []
                if posts_data:
                    for post in posts_data:
                        title_match = True
                        body_match = True
                        if title:
                            title_match = (
                                title in post["title"].lower()
                            )
                        if body:
                            body_match = (
                                body in post["body"].lower()
                            )
                        if title_match and body_match:
                            filtered_posts.append(post)

                if filtered_posts:
                    author_tasks = [
                        fetch_author(session, post["userId"])
                        for post in filtered_posts
                    ]
                    authors = await asyncio.gather(*author_tasks)

                    for post, author in zip(filtered_posts, authors):
                        post["author_name"] = author.get(
                            "name",
                            "Неизвестный автор"
                        )
                    posts_result = filtered_posts
        except Exception as e:
            error_message = f"Ошибка приложения: {e}"

    return render_template(
        "index.html",
        users=users_result,
        posts=posts_result,
        error=error_message
    )


if __name__ == "__main__":
    app.run(debug=True)