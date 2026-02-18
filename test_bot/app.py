import asyncio
import logging

from maxapi import Bot, Dispatcher
from maxapi.types import BotStarted, Command, MessageCreated

from shared.database import get_session, init_database
from shared.models import Staff

logging.basicConfig(level=logging.INFO)

bot = Bot('f9LHodD0cOJyRwg2Wh9-AVQhw-8hcSkswc-QPVf2ejN0UA52QuOsMJkFRYuTfHDcaeDUS_P8u7Y3hlLjwvpq')
dp = Dispatcher()

# Инициализация БД
engine = init_database()


# Ответ бота при нажатии на кнопку "Начать"
@dp.bot_started()
async def bot_started(event: BotStarted):
    await event.bot.send_message(
        chat_id=event.chat_id,
        text='Привет! Отправь мне /start'
    )


# Ответ бота на команду /start
@dp.message_created(Command('start'))
async def hello(event: MessageCreated):
    # Создаем новую сессию для каждого запроса
    session = get_session(engine)
    try:
        # Ищем по person_id (ID из API), а не по внутреннему id
        x = session.query(Staff).filter(Staff.person_id == 58).first()

        if x:
            await event.message.answer(
                f"✅ Найден сотрудник:\n"
                f"👤 Имя: {x.name}\n"
                f"📧 Email: {x.email}\n"
                f"📞 Телефон: {x.phone}\n"
                f"🆔 Person ID: {x.person_id}"
            )
        else:
            await event.message.answer("❌ Сотрудник с person_id=58 не найден")

    except Exception as e:
        await event.message.answer(f"❌ Ошибка: {e}")
        logging.error(f"Database error: {e}")
    finally:
        session.close()  # Важно закрыть сессию


async def main():
    await dp.start_polling(bot)


if __name__ == '__main__':
    asyncio.run(main())