import uuid
import psycopg2
import config
import io
import zipfile
import uuid
import xmlschema

def find_key(data, target_key) -> dict:
    
    """
    Функция принимает на вход словарь
    Цель функции - поиск в словаре ключа, соответствующего переменной "target_key".
    На выходе получим словарь или None, в зависимости от результата поиска.
    """

    if isinstance(data, dict):
        for key in data:
            if target_key in key:
                return data[key]  # Ключ найден, прекращаем рекурсию
            # Рекурсивно проверяем значение текущего ключа
            deep_dict = find_key(data[key], target_key)
            if deep_dict:
                return deep_dict  # Передаем сигнал остановки наверх
        return None  # Ключ не найден в этом словаре
    
    elif isinstance(data, (list, tuple)):
        for item in data:
            deep_dict = find_key(item, target_key)
            if deep_dict:
                return deep_dict  # Останавливаем поиск в списке/кортеже
        return None
    
    return None  # Другие типы данных не обрабатываем


def create_conn_test_db():
    """
    Функция для создания подключения к БД "test_root"
    """
    # Читаю конфиг файл
    test_root_host = config.TEST_ROOT_HOST
    test_root_name = config.TEST_ROOT_NAME
    test_root_user = config.TEST_ROOT_USER
    test_root_password = config.TEST_ROOT_PASSWORD

    conn = psycopg2.connect(
    host=test_root_host,
    database=test_root_name,
    user=test_root_user,
    password=test_root_password)
    return conn


def reader_func(request) -> list :
    """
    Функция для получения данных таблиц
    """
    try:
        conn = create_conn_test_db()
        cur = conn.cursor()
        str_select = f'{request}'
        cur.execute(str_select)
        CURR_DATA = cur.fetchall()
        return CURR_DATA
    except Exception as ex:
        print(f"\n{ex}")
    finally:
        cur.close()
        conn.close()

def create_dict_tables_with_columns() -> dict:
    
    """
    Функция для создания словаря, где
    *Ключ* - Наименование таблицы из БД;
    *Значение* - Список, где
                          а) Список наименований столбцов;
                          б) Строчка из плейсхолдеров (%s).
    (Количество столбцов = количество плейсхолдеров)
    """
    
    result_dict = {}
    
    all_objects = reader_func(f'''

                SELECT table_name
                FROM INFORMATION_SCHEMA.tables
                WHERE table_schema = 'fz_223'
    ''')

    list_tables = [el[0] for el in all_objects if 'dict' not in el[0]]
    
    for table_name in list_tables:
        raw_columns_of_table = reader_func(f'''

                    SELECT column_name
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{table_name}' and table_schema = 'fz_223'

                ''')
        result_dict[table_name] = [[el[0] for el in raw_columns_of_table], ', '.join(['%s'] * len(raw_columns_of_table))]
    return result_dict
        
def cicle_parsing(data, past_table_name, past_key_name, uid, parent_uid=None, global_list_tables = None):
    """
    Функция для разбиения ветвистой структуры словаря в плоскую
    На вход получаем:
    1) data - Данные (словарь), которые будет превращаться в плоскую структуру;
    
    2) past_table_name - название предыдущей по наследованию таблицы. (Например, если значение ключа является списком, то ключ становится наименованием таблицы, а структура внутри списка данными этой таблицы);
    
    3) past_key_name - Наследуемое наименование родительского уровня
        (Родитель - customer, дочерний итоговый элемент customer.mainInfo.fullName);
        
    4) uid - генерируемый идентификатор для текущей записи;
    
    5) parent_uid - наследуемый генерируемый идентификатор с уровня выше 
        (Например, существует родительская запись в таблице contractData с uid = A, а дочерняя запись будет иметь связку с родителем через parent_uid = A, а);
        
    6) global_list_tables - глобальная переменная, переданная в функцию, которая является коробкой в которую складываются преобразованные данные (после окончания парсинга, коробка перезаписывается = {}).
    
    На выходе получаем словарь плоской структуры 
    
    
    *** То, что я подразумеваю под плоской структурой, пример расположен в самом низу этого файла ***
    
    Так как ключи имеют лишние подстроки, такие как "ns2:", "body.item" и, возможно, точки, то приходится постоянно их заменять на "" пустую стоку.


    """
    parent_uid = str(parent_uid)
    
    if isinstance(data, dict):
        sub_dict = {'uid': uid}  # Список для формирования заносящегося списка
        if parent_uid:  # Если есть родитель, добавляем связь
            sub_dict['parent_uid'] = parent_uid
        for key in data:
            
            if isinstance(data[key], dict):
                sub_dict.update(cicle_parsing(data[key], past_table_name, past_key_name + '.' + key, uid, parent_uid, global_list_tables))  # parent_uid для связи
            elif isinstance(data[key], (tuple, list)):
                # Наименование таблицы
                table_name = (past_table_name + '.' + key).replace('ns2:', '').replace('body.item', '').strip('.')  # Формируем наименование для ключа в словаре sub_dict

                if table_name not in global_list_tables:
                    global_list_tables[table_name] = []
                cicle_parsing(data[key], table_name, '', str(uuid.uuid4()), uid, global_list_tables)  # Передаем uid текущей компании как parent_uid
            else:
                sub_dict[(past_key_name + '.' + key).replace('ns2:', '').replace('body.item', '').strip('.')] = data[key]
        return sub_dict

    elif isinstance(data, (list, tuple)):
        for el in data:
            if isinstance(el, dict):
                nested_uid = str(uuid.uuid4())  # Генерируем уникальный id для вложенной записи
                global_list_tables[past_table_name.replace('ns2:', '').replace('body.item', '').strip('.')].append(cicle_parsing(el, past_table_name, past_key_name, nested_uid, parent_uid, global_list_tables))  # Передаем parent_uid
            
            # Сюда скрипт никак не должен заходить, по хорошему тут вывести raise
            elif isinstance(el, (list, tuple)):
                raise ValueError
            else:
                global_list_tables[past_table_name.replace('ns2:', '').replace('body.item', '').strip('.')].append({'uid': uid, 'parent_uid': parent_uid, 'VALUE': el})  # Привязываем к родителю
               

            

def execute_transaction(data, dict_tables_with_columns, cur, conn):
    """
    Функция для создания транзакции и ее отмены в случае ошибки.
    На вход принимаем:
        1) словарь, данные которого необходимо распределить по таблицам;
            (В моем случае наименованиями таблиц будут являться ключи словаря)
        2) заранее созданный словарь с наименованием таблицы и его столбцами. 
            (Структура словаря str(наименование_таблицы) : list(наименования_столбцов_таблицы))
            
    При выполнении функции происходит занесение данных в соответствующие таблицы
    """

    try:

        cur.execute("BEGIN;")  # Начало транзакции
    
        # Прохожусь по каждому ключу, значения котрого являются будущими записями в соответствующей таблице в БД
        for table_name in data:
            try:
                
                # Наименования некоторых таблиц не умещаются в 64 символа, сокращаем их
                if 'longTermContractVolumeDetail' in table_name:
                    new_table_name = table_name.replace('longTermContractVolumeDetail', 'longTermContVolDet')
                else:
                    new_table_name = table_name
                    
                # Берем сформированные заранее наименования столбцов таблицы и их плейсхолдеры
                columns_name = dict_tables_with_columns[new_table_name][0]
                placeholders =  dict_tables_with_columns[new_table_name][1]
                
                # Список для формирования набора строчек данных
                data_truck = []

                # Так как данные в списке и там может быть несколько записей, делаю проходку по всем элементам
                for el in data[table_name]:
                    # Список для формирования строчки данных
                    data_box = []
                    for column_name in columns_name:
                        # Операция заполнения данных
                        try:
                            data_box.append(el[column_name])
                        except:
                            data_box.append(None)

                    data_truck.append(tuple(data_box))



                # Приводим нужные наименования колонок в единую строку с разделителем ", "

 
                string_columns_name = ', '.join([f'"{el}"' for el in columns_name])

                insert_query = f"""
                    INSERT INTO "fz_223"."{new_table_name}" ({string_columns_name})
                    VALUES ({placeholders})
                """

                cur.executemany(
                        insert_query,
                        data_truck
                    )
                
            except Exception as ex:
                print(f"Error of preprocessing data_transaction {ex}, \n{data}")
        
        conn.commit()  # Фиксация транзакции
    except Exception as ex:
        conn.rollback()  # Откат транзакции при ошибке
        print(f"\nError during transaction execution\nException: {ex}\n{table_name}\n{data_truck}\n {data}\n{insert_query}\n")


def take_data_from_test_root(request, cur) -> list:
    cur.execute(request)
    return cur.fetchall()


def table_filling(path_to_zip, zip_name, ftp, cur, conn, xs, dict_tables_with_columns, index_error = 0):
    """
    Данная функция заносит ZIP файл в RAM и распаковывает его, после чего итеративно проходит
    по каждому из XML.
    """
    try:
        with io.BytesIO() as zip_data:
            # Загружаем zip-архив в память
            ftp.getfo(path_to_zip, zip_data)
            # Перемещаем указатель на начало буфера
            zip_data.seek(0)
            # Проверяем, является ли файл zip-архивом
            if zipfile.is_zipfile(zip_data):                

                with zipfile.ZipFile(zip_data, 'r') as zip_file:

                    # Бегаем по всем xml-файлам
                    for file_path in zip_file.namelist()[:]:
                        
                        db_xml_files = take_data_from_test_root(f'''

                                                    SELECT "xml_name"
                                                    FROM "fz_223"."XML_LIST"
                                                    WHERE "zip_name" = '{zip_name}'

                                                                ''', cur)
                        
                        if (file_path,) not in db_xml_files:
                            ## Открываю XML и записываю в БД
                            try:
                                with zip_file.open(file_path) as f:
                                    file = f.read()
                                    file_str = file.decode('utf-8')  # Преобразуем bytes в строку

                                    # Проверка пустой ли файл или нет
                                    if file_str:

                                        xml_dict = xs.to_dict(io.StringIO(file_str), validation='lax')
                                        raw_dict = xml_dict[0]
                                        for el in ['contractCancellationData', 'contractCancellation', 'electronicContractInfoData', 'performanceContractData', 'performanceContract', 'subcontractorInfoData', 'contractData']:
                                            
                                            need_info = find_key(raw_dict, el)
                                    
                                            # Если я нашел нужный мне ключ, то заношу его в БД
                                            if need_info:
                                                global_list_tables = {} # Формируем болванку изменяемого типа данных
                                                
                                                if type(need_info) == list:
                                                    cicle_parsing({el : need_info}, '', '', uuid.uuid4(), global_list_tables = global_list_tables)
                                                else:
                                                    cicle_parsing({el : [need_info]}, '', '', uuid.uuid4(), global_list_tables = global_list_tables)

                                                execute_transaction(global_list_tables, dict_tables_with_columns, cur, conn)

                                                # Заполнение таблицы XML_LIST
                                                xml_list_data = {'XML_LIST' : [{'zip_name' : zip_name, 'xml_name' : file_path, 'xml_status' : 'F'}]}
                                                list_xml_with_placeholders = {'XML_LIST' : [['zip_name', 'xml_name', 'xml_status'], '%s, %s, %s']}
                                                execute_transaction(xml_list_data, list_xml_with_placeholders, cur, conn)

                                                break

                            except Exception as ex:
                                index_error = 1
                                print('Проблема при чтении xml-файла: ', ex)

                        else:
                            pass
                    
    except Exception as ex:
        index_error = 1
        print('Проблема при чтении zip: ', ex)
        
    return ftp, index_error


    # {'subcontractorInfoData': [{'uid': '4e1f5600-7ca8-4fbd-a11c-d4df3ea23900', 'parent_uid': 'a5c9acf8-9caa-4a3a-bc43-85ceb06564e5', 'guid': '24ee2b97-6dad-4d13-96c1-b0a328e295de', 'registrationNumber': '51001013117230001650004', 'createDateTime': '2023-11-30T08:41:14', 'customer.mainInfo.fullName': 'АКЦИОНЕРНОЕ ОБЩЕСТВО "ПРИОНЕЖСКАЯ СЕТЕВАЯ КОМПАНИЯ"', 'customer.mainInfo.shortName': 'АО "ПСК"', 'customer.mainInfo.iko': '51001013117100101001', 'customer.mainInfo.inn': '1001013117', 'customer.mainInfo.kpp': '100101001', 'customer.mainInfo.ogrn': '1061001073242', 'customer.mainInfo.legalAddress': '185013, Г.. ПЕТРОЗАВОДСК, УЛ НОВОСУЛАЖГОРСКАЯ (РЫБКА Р-Н), Д.22', 'customer.mainInfo.postalAddress': '185013, г Петрозаводск, р-н Рыбка, Новосулажгорская улица, дом 22', 'customer.mainInfo.okato': '86401000000', 'customer.mainInfo.okopf': '12267', 'customer.mainInfo.okopfName': 'Непубличные акционерные общества', 'customer.mainInfo.okpo': '97160650', 'customer.mainInfo.okfs': '16', 'customer.mainInfo.okfsName': 'Частная собственность', 'placer.mainInfo.fullName': 'АКЦИОНЕРНОЕ ОБЩЕСТВО "ПРИОНЕЖСКАЯ СЕТЕВАЯ КОМПАНИЯ"', 'placer.mainInfo.shortName': 'АО "ПСК"', 'placer.mainInfo.iko': '51001013117100101001', 'placer.mainInfo.inn': '1001013117', 'placer.mainInfo.kpp': '100101001', 'placer.mainInfo.ogrn': '1061001073242', 'placer.mainInfo.legalAddress': '185013, Г.. ПЕТРОЗАВОДСК, УЛ НОВОСУЛАЖГОРСКАЯ (РЫБКА Р-Н), Д.22', 'placer.mainInfo.postalAddress': '185013, г Петрозаводск, р-н Рыбка, Новосулажгорская улица, дом 22', 'placer.mainInfo.okato': '86401000000', 'placer.mainInfo.okopf': '12267', 'placer.mainInfo.okopfName': 'Непубличные акционерные общества', 'placer.mainInfo.okpo': '97160650', 'placer.mainInfo.okfs': '16', 'placer.mainInfo.okfsName': 'Частная собственность', 'publicationDate': '2023-11-30T09:01:50', 'status': 'P', 'version': 1, 'additionalInfo': 'Дополнительное соглашение № 11 от 29.11.2023 г. к договору №41/59 от 23.10.2023', 'contractRegNumber': '51001013117230001650000', 'contractInfo.contractInfoRegNumber': '51001013117230001650001', 'contractInfo.agencyName': 'АО "ПСК"', 'contractInfo.contractDate': '2023-09-22', 'contractInfo.contractSubject': 'Реконструкция Л-46П-19 (замена на КЛ-10кВ) участок от оп. № 1 до ВС-57, г. Петрозаводск, мр ЮПЗ – с. Деревянное, Прионежский р-н', 'name': '11', 'contractDate': '2023-11-29', 'contractSubject': 'Производство электрических испытаний и измерений на объекте: «Реконструкция Л-46П-19 (замена на КЛ-10кВ) участок от оп. № 1 до ВС-57, г. Петрозаводск, мр ЮПЗ – с. Деревянное, Прионежский р-н»', 'price': Decimal('0'), 'currency.code': 'RUB', 'currency.digitalCode': '643', 'currency.name': 'Российский рубль', 'startExecutionDate': '2023-11-29', 'endExecutionDate': '2024-12-31', 'longTermContractVolumes.volume': Decimal('0'), 'hasOkpdAndOkdpRows': False, 'hasOkpd2Rows': True}], 'subcontractorInfoData.longTermContractVolumeDetail': [{'uid': '15bf02d5-78a4-41ec-af17-ea13f2bf3c91', 'parent_uid': '4e1f5600-7ca8-4fbd-a11c-d4df3ea23900', 'year': 2023, 'summ': Decimal('0')}, {'uid': 'e7f0cabb-e579-4815-919f-7c6acea1003e', 'parent_uid': '4e1f5600-7ca8-4fbd-a11c-d4df3ea23900', 'year': 2024, 'summ': Decimal('0')}], 'subcontractorInfoData.contractPosition': [{'uid': '00a012ca-4f1f-4e18-aedc-f22de8c57c29', 'parent_uid': '4e1f5600-7ca8-4fbd-a11c-d4df3ea23900', 'guid': '265916e3-dd0a-4f0f-b25d-8749a532e4bc', 'name': 'Производство электрических испытаний и измерений на объекте: «Реконструкция Л-46П-19 (замена на КЛ-10кВ) участок от оп. № 1 до ВС-57, г. Петрозаводск, мр ЮПЗ – с. Деревянное, Прионежский р-н»', 'ordinalNumber': 1, 'okpd2.code': '42.22.12.112', 'okpd2.name': 'Линии электропередачи местные кабельные', 'typeObjectPurchase': 'W', 'impossibleToDetermineAttr': False, 'okei.code': '876', 'okei.name': 'Условная единица', 'qty': Decimal('1'), 'unitPrice': Decimal('0'), 'currency.code': 'RUB', 'currency.digitalCode': '643', 'currency.name': 'Российский рубль'}]}