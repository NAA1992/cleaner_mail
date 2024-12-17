import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent))

import os
import yaml

from libraries.emailer import EmailDataProcessor
from libraries.common_funcs import CustomFormatter, set_custom_logger

from typing import Tuple, Union, Optional, Dict, List, Any

def read_yaml(file_path) -> Optional[Dict[str, Any]]:
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = yaml.safe_load(file)
            return data
    except FileNotFoundError:
        print(f"Файл {file_path} не найден.")
    except yaml.YAMLError as e:
        print(f"Ошибка при чтении YAML файла: {e}")



if __name__ == '__main__':
    #MARK: __main__
    logger = set_custom_logger("email_collector")
    logger.info("Привет! Я - чистилка почты. Разработчик Алексей Нихаенко, telegram: @AlekseyN92")
    file_path = os.path.abspath("config_email.yaml")
    yaml_data = read_yaml(file_path)
    if yaml_data is None:
        exit(1) # Выходим с ошибкой
    processor = EmailDataProcessor(
        imap_server=yaml_data.get("imap_server"),
        email_user=yaml_data.get("email"),
        email_password=yaml_data.get("password"),
        exclude_folders=yaml_data.get("exclude_folders"),
        output_excel_file=yaml_data.get("output_excel"),
        input_excel_file=yaml_data.get("input_excel")
    )
    # processor.init_self_logger(logger_name=logger.name)
    processor.run()
