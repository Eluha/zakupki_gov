import config
import paramiko

import xmlschema

# здесь находятся, написанные функции для 
from functions_parsing_zakupki import table_filling, take_data_from_test_root, execute_transaction, create_dict_tables_with_columns, create_conn_test_db



if __name__ == "__main__":

    try:
        # Читаем переменные конфигурационного файла
        llm_host = config.LLM_HOST
        llm_user = config.LLM_USER
        llm_password = config.LLM_PASSWORD

        # Считываем схему, в соответствии с которой собираются данные XML-файлов 
        xs = xmlschema.XMLSchema('xsd_dir/contract.xsd')

        # Заходим на сервер
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(llm_host, username=llm_user, password=llm_password)
        # Переходим в формат ftp, чтобы открывать файлы
        ftp=ssh.open_sftp()
        dict_tables_with_columns = create_dict_tables_with_columns()
        # Подключение к test_root
    
        test_root_conn = create_conn_test_db()
        cur = test_root_conn.cursor()

        # Получаем полный перечень путей zip [(path,), (path,), (path,)]
        list_of_path = take_data_from_test_root('''
                                                SELECT "path_to_zip" FROM "fz_223"."PATHS_TO_ZIP"
                                                ''', cur)
        # Считываем все пути до директорий (так как документов очень много, то было принято решение заполнять данные по убыванию года)
        for path in list_of_path:

            try:
                # Смотрим, есть ли zip уже в нашем списке
                status_zip = take_data_from_test_root(f'''
                            SELECT "zip_status"
                            FROM "fz_223"."ZIP_LIST"
                            WHERE "zip_name" = '{path[0]}'
                            ''', cur)

                if status_zip:
                    # Если zip в моем репозитории и он недопарсился
                    if status_zip[0][0] == 'I':


                        ftp, index_error = table_filling('/home/user/ZAKUPKI/fz223free/out/published/' + path[0], path[0], ftp, cur, test_root_conn, xs, dict_tables_with_columns)
                        # Если вышла ошибка, то записываем 'I', иначе перезаписываем 'I' на 'F'
                        if index_error:
                            pass
                        else:
                            try:
                                cur.execute(f'''
                                    UPDATE "fz_223"."ZIP_LIST"
                                    SET "zip_status" = 'F'
                                    WHERE "zip_name" = '{path[0]}'
                                    ''')
                                test_root_conn.commit()
                            except Exception as ex:
                                print(f'Error. Не перезаписалась запись с "I" на "F"')

                    # Если зип допарсился - пропускаем
                    else:
                        pass
                else:
                    dict_zip_data = {'ZIP_LIST' : [{'zip_name' : path[0], 'zip_status' : 'I'}]}
                    list_zip_with_placeholders = {'ZIP_LIST' : [['zip_name', 'zip_status'], '%s, %s']}
                    execute_transaction(dict_zip_data, list_zip_with_placeholders, cur, test_root_conn)

                    ftp, index_error = table_filling('/home/user/ZAKUPKI/fz223free/out/published/' + path[0], path[0], ftp, cur, test_root_conn, xs, dict_tables_with_columns)
                    # Если индекс ошибки показывает == 1, значит произошел вызов ошибки
                    if not index_error:

                        try:
                            cur.execute(f'''
                                UPDATE "fz_223"."ZIP_LIST"
                                SET "zip_status" = 'F'
                                WHERE "zip_name" = '{path[0]}'
                                ''')
                            test_root_conn.commit()
                        except Exception as ex:
                            print(f'Error. Не перезаписалась запись с "I" на "F"')
            except Exception as ex:
                print(f'Ошибка при парсинге zip: {ex}')

    except Exception as ex:
        print(f"\n{ex}")
    finally:
        cur.close()
        test_root_conn.close()

        ftp.close()
        ssh.close()