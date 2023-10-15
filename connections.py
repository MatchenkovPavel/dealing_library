# подключение к postgress
from sqlalchemy import create_engine
import configparser
import datetime



class Connection:

    def connect_dxcore(self):
        config = configparser.ConfigParser()
        config.read('/Users/p.matchenkov/Desktop/configurations/config.ini')
        password, localhost, bd_type, bd_name, login = (config['DXCORE_prod']['password'], config['DXCORE_prod']['localhost'],
                                                        config['DXCORE_prod']['bd_type'], config['DXCORE_prod']['bd_name'],
                                                        config['DXCORE_prod']['login'])

        engine = create_engine(f'{bd_type}://{login}:{password}@{localhost}/{bd_name}')
        try:
            engine.connect()
            #print('connections to dxcore success')
        except Exception as err:
            print(err)

        return engine

    def connect_cex_clickhouse(self):
        config = configparser.ConfigParser()
        config.read('/Users/p.matchenkov/Desktop/configurations/config.ini')
        password, localhost, bd_type, bd_name, login = (config['PASSWORDS']['password'], config['LOCALHOST']['localhost'],
                                                        config['NAMES']['bd_type'], config['NAMES']['bd_name'],
                                                        config['PASSWORDS']['login'])
        engine = create_engine(f'{bd_type}://{login}:{password}@{localhost}/{bd_name}')
        try:
            engine.connect()
            print('connections to cex clickhouse success')
        except Exception as err:
            print(err)

    def connect_to_fin_control(self):
        config = configparser.ConfigParser()
        config.read('/Users/p.matchenkov/Desktop/configurations/config.ini')
        password = config['FINANCE_CONTROL']['password']
        login = config['FINANCE_CONTROL']['login']
        localhost = config['FINANCE_CONTROL']['localhost']
        engine = create_engine(f'postgresql://{login}:{password}@{localhost}:5432/finance_control')
        try:
            engine.connect()
            print('connections to fin_control success')
        except Exception as err:
            print(err)
        return engine

    def connect_to_accountmng(self):
        config = configparser.ConfigParser()
        config.read('/Users/p.matchenkov/Desktop/configurations/config.ini')
        password = config['ACCOUNTMNG']['password']
        login = config['ACCOUNTMNG']['login']
        localhost = config['ACCOUNTMNG']['localhost']
        engine = create_engine(f'postgresql://{login}:{password}@{localhost}:5432/accountmng')
        try:
            engine.connect()
            print('connections to accountmng success')
        except Exception as err:
            print(err)

        return engine

