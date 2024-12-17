import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import logging
from typing import Union, Dict, Any, List
import os
import inspect
import requests
import datetime
import json

from urllib.parse import urlparse

def set_custom_logger(name_logger:str,
                      level_logger="INFO"
                      ) -> logging.Logger:
    #MARK: set_custom_logger
    # Создание логгера
    logger = logging.Logger(name_logger)
    logger.setLevel(level_logger.upper())

    # Создание обработчика и формата логирования
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level_logger.upper())
    formatter = CustomFormatter()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return (logger)

class CustomFormatter(logging.Formatter):
    #MARK: CustomFormatter
    """ ANSI color codes """
    BLACK = "\033[0;30m"
    RED = "\033[0;31m"
    GREEN = "\033[0;32m"
    BROWN = "\033[0;33m"
    BLUE = "\033[0;34m"
    PURPLE = "\033[0;35m"
    CYAN = "\033[0;36m"
    LIGHT_GRAY = "\033[0;37m"
    DARK_GRAY = "\033[1;30m"
    LIGHT_RED = "\033[1;31m"
    LIGHT_GREEN = "\033[1;32m"
    YELLOW = "\033[1;33m"
    LIGHT_BLUE = "\033[1;34m"
    LIGHT_PURPLE = "\033[1;35m"
    LIGHT_CYAN = "\033[1;36m"
    LIGHT_WHITE = "\033[1;37m"
    BOLD = "\033[1m"
    FAINT = "\033[2m"
    ITALIC = "\033[3m"
    UNDERLINE = "\033[4m"
    BLINK = "\033[5m"
    NEGATIVE = "\033[7m"
    CROSSED = "\033[9m"
    END = "\033[0m"

    # Время: ИмяЛоггера - УровеньЛога - Сообщение (ИмяФайла.py : НомерСтроки)
    # format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s : %(lineno)d)"
    format = "%(asctime)s %(levelname)s: %(message)s"

    FORMATS = {
        logging.DEBUG: LIGHT_BLUE + format + END,
        logging.INFO: format,
        logging.WARNING: YELLOW + format + END,
        logging.ERROR: RED + format + END,
        logging.CRITICAL: BOLD + LIGHT_RED + format + END
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)



def create_folder_if_not_exists(list_folders:List[Any]
                                , logger: logging.Logger):
    #MARK: create_folder_if_not_exists
    """
    Проверяет существование всех путей из list_folders.
    Создает отсутствующие папки, выбрасывает исключение при пустых значениях.
    """
    # Настраиваем логгер с наименованием текущей функции
    if not isinstance(logger, logging.Logger):
        logger = logging.getLogger(inspect.currentframe().f_code.co_name)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        logger.addHandler(handler)
    for folder in list_folders:
        if not isinstance(folder, str) or folder == '':
            return (False, "Не должно быть пустых путей, список должен состоять из типа STRING")
        full_path = os.path.abspath(folder)
        logger.info(f"Смотрим, существует ли путь {full_path}")
        if not os.path.exists(full_path):
            logger.info(f"ПУТЬ НЕ СУЩЕСТВУЕТ. Создаем")
            os.makedirs(full_path)
            logger.info(f"Создали")
        else:
            logger.info(f"Такой путь существует")


def prepare_ResponseStatisticInsert(response: Union[requests.Response]
                             , date_begin: datetime.datetime
                             , date_end: datetime.datetime
                             , size_of_data_bytes: int
                             , **kwargs) -> Dict[str, Union[str, int]]:
    #MARK: prepare_ResponseStatisticInsert
    # Вытаскиваем URL, хост, статус
    if isinstance(response, requests.Response):
        full_url = response.url
        host = urlparse(response.url).hostname
        status_code = response.status_code
        text_answer = response.text
    #elif isinstance(response, aiohttp.ClientResponse):
    #    full_url = str(response.url)
    #    host = response.url.host
    #    status_code = response.status
    #    text_answer = await response.text()

    # Пытаемся преобразовать текст в JSON
    try:
        if isinstance(text_answer, str):
            text_answer = json.loads(text_answer)  # Преобразуем строку в JSON
            text_answer = json.dumps(text_answer, indent=4, ensure_ascii=False)  # Красивый вывод JSON
        else:
            text_answer = str(text_answer)
    except Exception as e:
        text_answer = str(text_answer)
    duration = max((date_end - date_begin).total_seconds(), 1e-6)  # Защита от деления на ноль, минимум 1e-6
    size_mb = max(size_of_data_bytes / 1024.0 / 1024.0, 0.0)  # Размер данных в МБ
    speed = max((((size_of_data_bytes * 8.0) / 1024.0) / 1024.0) / duration, 0.0)  # Скорость в Mbit/s

    kwargs.update({
        "Status-code": status_code
        , "Host": host
        , "URL": full_url
        , "ВРЕМЯ НАЧАЛО": date_begin
        , "ВРЕМЯ КОНЕЦ": date_end
        , "ДЛИТЕЛЬНОСТЬ РАБОТЫ (сек)": round(duration, 3)
        , "РАЗМЕР ДАННЫХ": f"{str(round(size_mb, 3))} МБ"
        , "СКОРОСТЬ": f"{str(round(speed, 3))} Mbit\\s"
        , "ОТВЕТ": text_answer
    })

    return kwargs


async def prepare_ResponseData(response: Union[requests.Response]
                             , **kwargs) -> Dict[str, Any]:
    #MARK: prepare_ResponseData

    # Вытаскиваем URL, хост, статус
    if isinstance(response, requests.Response):
        full_url = response.url
        host = urlparse(response.url).hostname
        status_code = response.status_code
        text_answer = response.text
    #elif isinstance(response, aiohttp.ClientResponse):
    #    full_url = str(response.url)
    #    host = response.url.host
    #    status_code = response.status
    #    text_answer = await response.text()

    # Пытаемся преобразовать текст в JSON
    try:
        if isinstance(text_answer, str):
            text_answer = json.loads(text_answer)  # Преобразуем строку в JSON
            text_answer = json.dumps(text_answer, indent=4, ensure_ascii=False)  # Красивый вывод JSON
        else:
            text_answer = str(text_answer)
    except Exception as e:
        text_answer = str(text_answer)

    kwargs.update({
        "Status-code": status_code
        , "Host": host
        , "URL": full_url
        , "ОТВЕТ": text_answer
    })

    return kwargs


