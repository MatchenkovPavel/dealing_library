import configparser
import requests
from requests.auth import HTTPBasicAuth
import json
import warnings
import pandas as pd
import uuid
import logging
warnings.simplefilter(action='ignore', category=FutureWarning)


class DevexApi():
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[
                            logging.FileHandler("/Users/p.matchenkov/Desktop/devex log/prod_log.log"),
                            logging.StreamHandler()
                        ])


    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('/Users/p.matchenkov/Desktop/configurations/config.ini')
        self.login = self.config['AURORA_prod']['login']
        self.password = self.config['AURORA_prod']['password']
        self.base_url = self.config['AURORA_API']['base_url']


class DevexApiConnection(DevexApi):
    def _get_token_dx_api(self) -> str:
        config = configparser.ConfigParser()
        config.read('/Users/p.matchenkov/Desktop/configurations/config.ini')
        username = config['AURORA_API']['login']
        password = config['AURORA_API']['password']
        json_data = {"username": username,
                     "domain": "default",
                     "password": password}
        response = requests.post(f'{self.base_url}/dxsca-web/login', json=json_data)
        token = json.loads(response.text)['sessionToken']
        return token


class DevexAccountInfo(DevexApi):
    """
    получение датафрйма с основными параметрами всех аккаунтов юзера
    """
    def get_user_accounts_info(self, user_up: str, clearingCode='LIVE') -> pd.DataFrame:

        try:
            url = f'{self.base_url}/dxweb/rest/api/register/client/default/{user_up}'
            response = requests.get(url, auth=HTTPBasicAuth(self.login, self.password))
            response.raise_for_status()
            logging.info(f"get_user_accounts {user_up}: successful with result: {response.status_code}.")
        except requests.exceptions.RequestException as err:
            logging.error(f"get_user_accounts for {user_up}: requests.exceptions.RequestException: {response.status_code}", exc_info=True)
            raise SystemExit(err)

        response = response.json()
        if 'accounts' in response.keys():
            response = pd.DataFrame(response['accounts'])
            response = response.loc[response['clearingCode'] == clearingCode]

            accounts_info = pd.DataFrame()
            for row in range(len(response)):
                accounts_info = pd.concat([accounts_info,
                                           pd.DataFrame(response.iloc[row]['categories']).pivot_table(columns='category', values='value', aggfunc=lambda x: ' '.join(x))], axis=0)
            accounts_info = pd.concat([response, accounts_info.reset_index()], axis=1).drop(columns={'categories'})
            accounts_info['user_id'] = user_up
            return accounts_info
        else:
            logging.error(f'{user_up} user has no accounts')


    def get_metrics(self, account_id: str, token: str, include_positions: str = 'false') -> pd.DataFrame:
        """
        получение датафрмейма с метриками (фпл, пнл, баланс, еквити)
        :param token: авторизация с помощью токена _get_token_dx_api
        :param include_positions: true вернет датафрейм с позициями
        :return:
        """
        try:
            url = f"{self.base_url}/dxsca-web/accounts/LIVE:{account_id}/metrics?include-positions={include_positions}"
            response = requests.get(url, headers={'Authorization': f"DXAPI {token}"})
            response.raise_for_status()
            logging.info(f"{response.status_code}, metrics for {account_id} successfully received")
        except requests.exceptions.RequestException as err:
            logging.error(f"{response.status_code}, data for {account_id} doesnt received, {err}")
            raise SystemExit(err)

        response = response.json()
        df = pd.DataFrame({
            'account': [response['metrics'][0]['account']],
            'equity': [response['metrics'][0]['equity']],
            'balance': [response['metrics'][0]['balance']],
            'openPL': [response['metrics'][0]['openPL']],
            'totalPL': [response['metrics'][0]['totalPL']]
        })
        return df


    def get_positions_id(self, account_id: str, token: str) -> pd.DataFrame:
        """
        вернет датафрйм со всеми позициями
        """

        try:
            url = f'{self.base_url}/dxsca-web/accounts/LIVE%3A{account_id}/positions'
            response = requests.get(url, headers={'Authorization': f"DXAPI {token}"})
            response.raise_for_status()
            logging.info(f"{response.status_code}, position for {account_id} received")
        except requests.exceptions.RequestException as err:
            logging.error(f"{response.status_code}, position for {account_id} doesnt received, {err}")
            raise SystemExit(err)

        positions = pd.DataFrame(response.json()['positions'])
        if positions.empty:
            logging.info(f"{account_id} has no positions")
        else:
            return positions


    def get_accounts_orders(self, account_id: str, token: str) -> pd.DataFrame:
        """
        вернет датафрйм со отложенными ордерами
        """
        # orderCode нужно передать в отмену ордеров
        try:
            url = f"{self.base_url}/dxsca-web/accounts/LIVE%3A{account_id}/orders"
            response = requests.get(url, headers={'Authorization': f"DXAPI {token}"})
            response.raise_for_status()
            logging.info(f"{response.status_code}, orders for {account_id} successfully received")
        except requests.exceptions.RequestException as err:
            logging.error(f"{response.status_code}, data for {account_id} doesnt received, {err}")

        orders = pd.DataFrame(response.json()['orders'])
        if orders.empty:
            logging.info(f"{account_id} has no orders")
        else:
            return orders


class DevexApiOperation(DevexApi):
    def change_domain_group(self, account: str, category: str, body: dict):
        try:
            url = f'{self.base_url}/dxweb/rest/api/register/account/LIVE/{account}/category/{category}'
            response = requests.put(url, json=body, auth=HTTPBasicAuth(self.login, self.password))
            response.raise_for_status()
            logging.info(f"{response.status_code} - set category '{category}' for {account} successfully changed {body['value']}")
        except requests.exceptions.RequestException as err:
            logging.error(f"{err} - '{category}' group doesnt changed for {account}")


    def make_adjustment(self, account_id, amount, comment, currency='USDT'):
        try:
            url = f"{self.base_url}/dxweb/rest/api/register/account/LIVE/{account_id}/adjustment/{str(uuid.uuid4())}"
            body = {
                "currency": currency,
                "amount": amount,
                "description": comment
            }
            response = requests.put(url, auth=HTTPBasicAuth(self.login, self.password), json=body)
            response.raise_for_status()
            logging.info(f"{response.status_code}, adjustment for {account_id} completed, amount = {amount}")
        except requests.exceptions.RequestException as err:
            logging.error(f"{response.status_code}, adjustment for {account_id} failed: {err}")


    def delete_open_order(self, order_id: str, account_id: str, token: str):
        order_id = order_id.replace(':', '%3A')
        try:
            url = f'{self.base_url}/dxsca-web/accounts/LIVE%3A{account_id}/orders/{order_id}'
            response = requests.delete(url, headers={'Authorization': f"DXAPI {token}"})
            response.raise_for_status()
            logging.info(f"{response.status_code}, order {order_id} for {account_id} delete")
        except requests.exceptions.RequestException as err:
            logging.error(f"{response.status_code}, order {order_id} for {account_id} doesn't delete, {err}")