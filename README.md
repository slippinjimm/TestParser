# Парсер
Парсер который  обходит первые две страницы по 44ФЗ, собирает ссылку на его
печатную форму и выводит значение  XML-поля publishDTInEIS

Для запуска необходимо ввести следующие комманды

1. Для установки необходимых библиотек
```pip install requirements.txt```

2. для запуска redis контейнера
```docker run --name my-redis -p 6379:6379 -d redis:alpine redis-server --requirepass "redis_pas"```

3. для запуска воркера
```celery -A tasks worker --loglevel=INFO --pool=gevent```

4. для запуска таска 
```python main.py ```
