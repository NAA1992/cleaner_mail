import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import pandas as pd
import os
from io import StringIO

def has_header(df):
    return isinstance(df.columns[0], str)

def create_empty_csv(file_path:str, column_names:list, overwrite=False):
    """
    Создает пустой CSV-файл с указанными именами колонок.

    :param file_path: Полный путь к создаваемому файлу.
    :param column_names: Список имен колонок.
    :param overwrite: Флаг, указывающий, нужно ли перезаписать существующий файл.
    :return: Путь к созданному файлу.
    """
    if overwrite or not os.path.isfile(file_path):
        df = pd.DataFrame(columns=column_names)
        df.to_csv(file_path, index=False, sep=';', quoting=1, na_rep='')  # quoting=1 соответствует csv.QUOTE_ALL
    else:
        try:
            # Читаем существующий файл
            df = pd.read_csv(file_path, sep=';', quoting=1)
            # Определяем недостающие колонки
            missing_columns = [col for col in column_names if col not in df.columns]
            if missing_columns:
                # Добавляем недостающие колонки в конец
                for column in missing_columns:
                    df[column] = pd.Series(dtype='object')
                # Сохраняем файл с добавленными колонками
                df.to_csv(file_path, index=False, sep=';', quoting=1, na_rep='')
        except pd.errors.EmptyDataError:
            # Если файл пустой, создаем его заново с заданными колонками
            df = pd.DataFrame(columns=column_names)
            df.to_csv(file_path, index=False, sep=';', quoting=1, na_rep='')
    return file_path


def append_to_csv(file_path:str, data):
    """
    Добавляет данные в существующий CSV-файл.

    :param file_path: Полный путь к файлу.
    :param data: Данные для добавления (строка, список или список списков).
    """
    # Определяем, существует ли файл
    file_exists = os.path.isfile(file_path)
    # И если да, то
    if file_exists:
        # Определяем количество столбцов в существующем файле
        existing_df = pd.read_csv(file_path, sep=';')
        existing_columns = list(existing_df.columns)
        num_existing_columns = len(existing_columns)
    else: return


    # Если в качестве данных строка, например - "chtoto;Чото2;ПУСТОТА С ПРОБЕЛОМ", то
    if isinstance(data, str) and data:
        # Добавляем новую строку в конец данных, если ее нет
        if not data.endswith('\n'):
            data += '\n'
        # Оборачиваем строку как файл (объект StringIO)
        file_like_object = StringIO(data)
        # Дата фрейм - полученный объект
        new_data_frame = pd.read_csv(file_like_object, sep=';', header=None)
        # у DataFrame из STRING по умолчанию нет заголовков
        flag_has_headers_new_dataframe = False

    # Если в качестве данных список, например ["знач1","Значение2", "Value 3"] то
    elif isinstance(data, list) and data:
        # Если внутри LIST еще один LIST (или несколько), например [[1,2,3],[4,5,6]], то
        if data and isinstance(data[0], list):
            new_data_frame = pd.DataFrame(data)
        # Иначе читаем список по-другому
        else:
            new_data_frame = pd.DataFrame([data])
        # у DataFrame из LIST по умолчанию нет заголовков
        flag_has_headers_new_dataframe = False

    # И пробуем читать DICT, если в качестве данных словарь
    elif isinstance(data, dict) and data:
        # у DataFrame из DICT по любому в качестве ключей - headers (заголовки)
        flag_has_headers_new_dataframe = True
        # Убедимся, что словарь имеет все существующие столбцы
        new_data_frame = pd.DataFrame([data])
        for column in existing_columns:
            if column not in new_data_frame.columns:
                new_data_frame[column] = ''
        new_columns = [col for col in new_data_frame.columns if col not in existing_columns]
        existing_df = existing_df.reindex(columns=existing_columns + new_columns)
        existing_columns = list(existing_df.columns)
        num_existing_columns = len(existing_columns)
        new_data_frame = new_data_frame.reindex(columns=existing_columns)
    else:
        raise ValueError("Unsupported data format")

    # Убедимся, что у новой строки нужное количество столбцов
    num_new_columns = new_data_frame.shape[1]
    if num_new_columns < num_existing_columns:
        for _ in range(num_existing_columns - num_new_columns):
            new_data_frame[num_new_columns] = ''
            num_new_columns += 1
    elif num_new_columns > num_existing_columns:
        for i in range(num_existing_columns, num_new_columns):
            existing_df[f"unnamed_{i - num_existing_columns + 1}"] = ''

    # Сохраняем DataFrame в CSV-файл с добавлением строки в конец файла
    new_data_frame.to_csv(file_path, mode='a', header=False, index=False, sep=';', quoting=1, na_rep='')
    existing_df.to_csv(file_path, index=False, sep=';', quoting=1, na_rep='')

    # Понимаем сколько колонок у нового dataframe + сколько МАКСИМУМ колонок (у существующего файла или новых данных)
    num_new_columns = new_data_frame.shape[1]
    num_final_columns = max(num_existing_columns, num_new_columns)
    # Сколько колонок нужно добавить в существующий файл
    how_much_add_columns = num_final_columns - existing_df.shape[1]

    # Если у вставляемых данных есть заголовки
    i_tmp = 0
    if flag_has_headers_new_dataframe:
        for i in range(num_new_columns):
            if existing_df.columns[i] in new_data_frame.columns:
                pass
            else:
                extracted_col = existing_df[existing_df.columns[i]]
                new_data_frame.insert(1, existing_df.columns[i], extracted_col)
    for i in range(1, how_much_add_columns+1):
        i_tmp += 1
        name_column_new = f"unnamed_{i_tmp}"
        while name_column_new not in new_data_frame.columns:
            i_tmp += 1
            name_column_new = f"unnamed_{i_tmp}"
            new_data_frame.columns = [*new_data_frame.columns[:-i], name_column_new]


    # Сохраняем DataFrame в CSV-файл с добавлением строки в конец файла
    if file_exists:
        new_data_frame.to_csv(file_path, mode='a', header=False, index=False, sep=';', quoting=1, na_rep='')
    else:
        new_data_frame.to_csv(file_path, mode='a', header=True, index=False, sep=';', quoting=1, na_rep='')



"""csv_path = "./__pycache__/test.csv"
csv_path_noheader = "./__pycache__/noheader.csv"
csv_columns = ["one", "two", "three with space"]
create_empty_csv(csv_path, csv_columns, overwrite=True)
data_1 = "chtoto;Чото2;ПУСТОТА С ПРОБЕЛОМ"
data_2 = ["hz", "жопа", ""]
data_3 = [[1,2,3],[4,5,6]]
data_4 = ["Засада","вот-вот"]
data_5 = ["Засада","вот-вот","вот щас", "ВОТ;ОНА"]
data_6_dict = {"NOTEXIST_COLUMN":"HUI_!"}
data_7_dict = {"two":"пищлец!"}
append_to_csv(csv_path, data_1 )
append_to_csv(csv_path, data_2 )
append_to_csv(csv_path, data_3 )
append_to_csv(csv_path, data_4 )
append_to_csv(csv_path, data_5 )
append_to_csv(csv_path, data_6_dict )
append_to_csv(csv_path, data_7_dict )"""
