import pandas as pd
import aiohttp
import asyncio
import logging
import json
import os
from aiohttp import ClientTimeout

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Пути к файлам
input_file = "4_dig.xlsx"
output_file = "feedback_analysis.xlsx"

# Загрузка данных
df = pd.read_excel(input_file, engine="openpyxl")

# Если файл с результатами уже существует, загружаем его
if os.path.exists(output_file):
    df_existing = pd.read_excel(output_file, engine="openpyxl")
    if 'Обратная связь' in df_existing.columns:
        df['Обратная связь'] = df_existing['Обратная связь']
    else:
        df['Обратная связь'] = None
else:
    df['Обратная связь'] = None

# Ограничение числа одновременных запросов
semaphore = asyncio.Semaphore(1)

# Асинхронная функция для запроса к Ollama
async def fetch_feedback(session, prompt):
    timeout = ClientTimeout(total=600)
    async with semaphore:
        try:
            async with session.post(
                "http://127.0.0.1:11434/api/chat",
                json={"model": "mistral", "messages": [{"role": "user", "content": prompt}]},
                timeout=timeout
            ) as response:
                feedback = ""
                async for line in response.content:
                    if line:
                        try:
                            data = json.loads(line.decode('utf-8'))
                            feedback += data.get("message", {}).get("content", "")
                        except json.JSONDecodeError:
                            logging.error("Ошибка декодирования JSON")
                return feedback
        except asyncio.TimeoutError:
            logging.error(f"Тайм-аут при обработке запроса: {prompt[:50]}...")
            return "Ошибка: превышено время ожидания"
        except Exception as e:
            logging.error(f"Ошибка при запросе к Ollama: {e}")
            return "Ошибка: не удалось получить обратную связь"

# Основная асинхронная функция
async def main():
    timeout = ClientTimeout(total=600)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
        for index, row in df.iterrows():
            if pd.notna(row.get('Обратная связь')):  # Пропускаем, если уже есть обратная связь
                logging.info(f"Пропускаем {row['Имя']}, так как у него уже есть данные")
                continue
            
            prompt = (
                f"Проведи психологический анализ и дай обратную связь для ученика {row['Имя']}. "
                f"Самое трудное в школе: {row['Что самое трудное в школе?']}. "
                f"Самое интересное в школе: {row['Что самое интересное в школе?']}. "
                f"Самое привлекательное в школе: {row['Что самое привлекательное в школе?']}. "
                f"В чем нужна помощь: {row['В чем тебе нужна помощь?']}. "
                f"Дай рекомендации, как можно помочь ученику."
            )
            tasks.append((index, fetch_feedback(session, prompt)))
            logging.info(f"Обрабатываем ученика {index + 1}/{len(df)}: {row['Имя']}")

        # Запускаем асинхронные задачи
        results = await asyncio.gather(*[t[1] for t in tasks])

        # Записываем результаты в DataFrame и сохраняем файл
        for (index, feedback) in zip([t[0] for t in tasks], results):
            df.at[index, 'Обратная связь'] = feedback
            df.to_excel(output_file, index=False)  # Сохраняем прогресс

# Запуск кода
logging.info("Запуск асинхронной генерации обратной связи...")
asyncio.run(main())

# Финальное сохранение
df.to_excel(output_file, index=False)
logging.info(f"Файл '{output_file}' успешно сохранен!")
