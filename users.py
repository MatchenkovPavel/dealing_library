from connections import Connection
import datetime
import pandas as pd
import ast
import warnings
import sys
import configparser
import requests
from requests.auth import HTTPBasicAuth
import json
import yfinance as yf
from sqlalchemy import text


class TradingPlatform():
    up = list()

    def __init__(self, users: str=None, freq='W-MON', date_from: str='2023-03-01', date_to=datetime.date.today()):
        if isinstance(users, list) == True:
            if len(users) > 1:
                self.up = tuple(users)
                self.user_req_cond = f"principals.name in {self.up}"
            elif len(users) == 1:
                self.up = users[0]
                self.user_req_cond = f"principals.name = '{self.up}'"
        elif isinstance(users, str) == True:
            self.up = users
            self.user_req_cond = f"principals.name = '{self.up}'"
        elif users is None:
            self.user_req_cond = f"principals.name is not null"
        else:
            sys.exit('Datatype error, should be str or list format')

        self.date_from = date_from
        self.date_to = str(date_to)
        dates = list()
        for date in pd.date_range(start=self.date_from, end=datetime.date.today()):
            dates.append(date.strftime("%Y-%m-%d"))
        self.dates = dates
        connect = Connection()
        self.engine_dxcore = connect.connect_dxcore()
        self.freq = freq

    def _get_market_data(self):
        if len(self.dates) == 1:
            date_cond_req = f"and bid_time::TIMESTAMP::DATE = '{self.dates[0]}'"
        elif len(self.dates) > 1:
            date_cond_req = f"and bid_time::TIMESTAMP::DATE in {tuple(self.dates)}"
        else:
            sys.exit('No dates from orders dataframe - orders df is empty')

        req = f"""
        select bid_time::TIMESTAMP::DATE as transaction_time, trim('USD/|/USD' from event_symbol) as quote_currency,
            case
                when event_symbol in ('USD/JPY', 'USD/CNH', 'USD/MXN') then (1/bid_price)
                when event_symbol in ('BTC/USD', 'ETH/USD', 'EUR/USD', 'GBP/USD') then bid_price
            end as bid_price
        from dxcore.dxcore.quotes_history qh 
        where event_symbol in ('BTC/USD', 'ETH/USD', 'EUR/USD', 'GBP/USD', 'USD/JPY', 'USD/CNH', 'USD/MXN')
        {date_cond_req}
            """
        with self.engine_dxcore.connect() as conn:
            df = pd.DataFrame(conn.execute(text(req))).drop_duplicates(subset=['transaction_time', 'quote_currency'])
        df = df.astype({'bid_price': float})
        return df

    def _get_last_quote(self):
        req = f"""
        SELECT event_symbol, snapshot_time, bid_price
        FROM ( SELECT event_symbol, snapshot_time, bid_price, ROW_NUMBER() OVER (PARTITION BY event_symbol ORDER BY snapshot_time DESC) 
            as row_num FROM dxcore.dxcore.quotes_history ) as subquery
        WHERE row_num = 1 and event_symbol in ('BTC/USD', 'ETH/USD')
            """
        with self.engine_dxcore.connect() as conn:
            df = pd.DataFrame(conn.execute(text(req)))
        df = df.astype({'bid_price': float})
        return df

    def login_info(self):
        req = f"""
        select principals.name, accounts.account_code, principals.created_time::DATE as DATE, expire_at
        from dxcore.dxcore.user_sessions as user_sessions
        left join dxcore.dxcore.principals as principals on user_sessions.user_id = principals.id
        right join dxcore.dxcore.accounts as accounts on accounts.owner_id = principals.id 
        where principals.created_time >= '2023-02-01'
        and {self.user_req_cond}
        """
        with self.engine_dxcore.connect() as conn:
            logins = pd.DataFrame(conn.execute(text(req))).drop_duplicates()
        logins['date'], logins['expire_at'] = pd.to_datetime(logins['date']),  pd.to_datetime(logins['expire_at'])
        return logins

    def get_users_orders(self):
        req = f"""
            select
            principals.name as user_id,
            accounts.account_code as account_id,
            order_instrument.symbol as order_symbol,
            split_part(order_instrument.symbol, '/', 2) as symbol,
            order_instrument.additional_fields::json->-0->'val' as pair_type,
            orders.order_side,
            activities.order_id as order_id, orders.created_time, orders.parameters,
            coalesce(order_legs.position_code, activity_legs.position_code, activities.linked_position_code, '') as position_code,
            abs(activity_legs.quantity) as quantity, activities.transaction_time,
            order_legs.price as price,
            coalesce(orders.extensions::json->0->'val'->'PL_SETTLED_IN_TRADE_CURRENCY', '0')::text::decimal as PNL_3,
            coalesce(orders.extensions::json->1->'val'->'PL_SETTLED_IN_TRADE_CURRENCY', '0')::text::decimal as PNL_1,
            coalesce(orders.extensions::json->2->'val'->'PL_SETTLED_IN_TRADE_CURRENCY', '0')::text::decimal as PNL_2,
            coalesce(orders.extensions::json->3->'val'->'PL_SETTLED_IN_TRADE_CURRENCY', '0')::text::decimal as PNL_4,
            coalesce(case when coalesce(orders.parameters::json->>'ORDER_EXEC_STRATEGY_NAME', opening_order.parameters::json->>'ORDER_EXEC_STRATEGY_NAME', 'B_BOOK') = 'FX_STP' 
            then round(abs((activity_legs.price - hedge.price) * orders.filled_quantity), 5) end, 0) as markup,
            coalesce(order_legs.position_effect, '') as position_effect
            from
                dxcore.dxcore.activities as activities
            inner join
                dxcore.dxcore.accounts as accounts on activities.account_id = accounts.id and accounts.id not in (111) and accounts.clearing_code in ('LIVE')
            inner join
                dxcore.dxcore.principals as principals on accounts.owner_id = principals.id
            inner join
                dxcore.dxcore.instruments account_instrument on accounts.currency_id = account_instrument.id
            inner join
                dxcore.dxcore.activity_legs as activity_legs on activities.id = activity_legs.activity_id
            inner join
                dxcore.dxcore.instruments order_instrument on activity_legs.instrument_id = order_instrument.id
            inner join
                dxcore.dxcore.orders as orders on activities.order_id = orders.id and (orders.status is null or orders.status = 'COMPLETED')
            inner join
                dxcore.dxcore.order_legs as order_legs on orders.id = order_legs.order_id and activity_legs.leg_type = 'POS_ADJUST'
            left join
                dxcore.dxcore.orders as opening_order on (order_legs.position_code = opening_order.order_chain_id::text and opening_order.status = 'COMPLETED')
            left join
                (SELECT orders.id, coalesce(orders.extensions::json->0->'val'->'hedgingOrderId', 
                    orders.extensions::json->1->'val'->'hedgingOrderId',
                    orders.extensions::json->2->'val'->'hedgingOrderId',
                    orders.extensions::json->3->'val'->'hedgingOrderId',  '0')::text::decimal as hedge_id FROM dxcore.dxcore.orders) hedge_order
                on orders.id = hedge_order.id
            left join
                dxcore.dxcore.order_legs hedge on hedge.order_id = hedge_order.hedge_id
            where
              (activities.transaction_time::DATE between '{self.date_from}' and '{datetime.datetime.strptime(self.date_to, '%Y-%m-%d') + datetime.timedelta(days=1)}') -- #- datetime.timedelta(days=1)
            and
                activities.activity_type = 'TRADE'
            and 
                {self.user_req_cond}
            order by
                activities.transaction_time asc
            """
        with self.engine_dxcore.connect() as conn:
            orders = pd.DataFrame(conn.execute(text(req)))

            if orders.empty:
                sys.exit('orders dataframe is empty')

            orders = (orders.drop_duplicates()
                     .astype({
                            'quantity': float,
                            'price': float,
                            'pnl_4': float,
                            'pnl_3': float,
                            'pnl_2': float,
                            'pnl_1': float,
                            'markup': float
                            })
                    )

        orders = orders.replace({'Forex Majors': 'Forex',
                                 'Forex Minors': 'Forex',
                                 'Forex Metals': 'Forex',
                                 'Indices': 'Equities',
                                 'MANUAL': 'FX_STP'})

        # getting orders strategy
        parameters = orders['parameters'].apply(lambda x: ast.literal_eval(x)).apply(pd.Series)
        orders['order_strategy'] = parameters['ORDER_EXEC_STRATEGY_NAME']

        orders['quote_currency'] = orders['order_symbol'].str.extract('/(.*)').fillna('USD').replace('USDT', 'USD')
        orders['trade_time'] = orders['transaction_time']
        orders['transaction_time'] = pd.to_datetime(orders['transaction_time']).dt.date

        _market_data = self._get_market_data()
        orders = orders.merge(_market_data, how='left', on=['transaction_time', 'quote_currency'])
        orders['transaction_time'] = pd.to_datetime(orders['transaction_time'])
        orders['bid_price'] = orders['bid_price'].fillna(1)

        # convert volume and pnl to USD
        orders['volume'] = orders['quantity'] * orders['price'] * orders['bid_price']
        orders['pnl'] = (orders['pnl_1'] + orders['pnl_2'] + orders['pnl_3'] + orders['pnl_4']) * orders['bid_price']
        orders['markup'] = orders['markup'] * orders['bid_price']
        orders = orders.drop(columns={
            'pnl_1', 'pnl_2', 'pnl_3', 'pnl_4', 'symbol', 'parameters', 'quote_currency'})
        return orders

    def get_financial_transaction(self, sort_time=True):
        req = f"""
        SELECT account_code, activity_type, activities.created_time::DATE as transaction_time, activities.created_time as date_time, principals.name as user_id, activities.description, 
        trim(trailing '$' FROM instruments.symbol) AS quote_currency, activity_legs.quantity as amount
        FROM dxcore.dxcore.activity_legs
        LEFT JOIN dxcore.dxcore.activities ON activities.id = activity_legs.activity_id
        LEFT JOIN dxcore.dxcore.accounts on accounts.id = activities.account_id
        LEFT JOIN dxcore.dxcore.principals on principals.id = accounts.owner_id
        LEFT JOIN dxcore.dxcore.instruments on instruments.id = activity_legs.instrument_id
        where activities.activity_type in ('DEPOSIT', 'WITHDRAWAL', 'ADJUSTMENT', 'FINANCING')
        and (activities.description not similar to ('%%(demo|Demo|test_|Test_|hedge|Hedge)%%') or activities.description is null)
        and activities.action_code not like '%%COMP%%'
        and accounts.clearing_code = 'LIVE'   
        and activities.created_time::DATE >= '{self.date_from}'
        and {self.user_req_cond}
        ORDER BY activities.created_time desc
        """
        with self.engine_dxcore.connect() as conn:
            df = pd.DataFrame(conn.execute(text(req))).drop_duplicates()
        if df.empty:
            sys.exit("financial dataframe is empty")

        df = (
            df.merge(self._get_market_data(), how='left', on=['transaction_time', 'quote_currency'])
            .fillna({'bid_price': 1})
        )
        df['usd'] = df['amount'].astype(float) * df['bid_price']
        df['transaction_time'] = pd.to_datetime(df['transaction_time'])

        if sort_time:
            df = (df.groupby([pd.Grouper(key='transaction_time', freq=self.freq, closed='left'), 'activity_type'])
                  .agg({'usd': 'sum'}).reset_index()
                  .astype({'usd': float})
                  .pivot_table(index='transaction_time', columns='activity_type', values='usd').reset_index()
                  .fillna(0)
                  )
        else:
            return df
        return df


    def positions(self):
        req = f"""
        select principals.name as user_id, accounts.account_code, instruments.symbol as order_symbol, 
        	positions.quantity, positions.cost, positions.code as position_code, opening_time , instruments.id as symbol, 
        	instruments.instrument_type
        from dxcore.dxcore.positions as positions
        	join dxcore.dxcore.instruments as instruments on positions.instrument_id = instruments.id
        	join dxcore.dxcore.accounts as accounts on accounts.id = positions.account_id
        	join dxcore.dxcore.order_legs as order_legs on order_legs.position_code = positions.code
        	join dxcore.dxcore.orders as orders on orders.id = order_legs.order_id
        	left join dxcore.dxcore.principals as principals on principals.id = accounts.owner_id 
        	where positions.code is not null -- убираем отдельные валюты (т.е. клиенские балансы)
        	and positions.quantity != 0
            and {self.user_req_cond}
        	order by orders.transaction_time desc
        """
        with self.engine_dxcore.connect() as conn:
            positions = (pd.DataFrame(conn.execute(text(req)))
                         .drop_duplicates()
                         .astype({'quantity': float,
                                  'cost': float,
                                  'quantity': float}
                                 ))
        if positions.empty:
            warnings.warn('no positions')
        else:
            positions = positions.groupby(['user_id', 'order_symbol']).agg(
                {'quantity': sum, 'cost': sum, 'position_code': pd.Series.unique}).reset_index()
            positions['open_price'] = positions['cost'] / positions['quantity']
            positions['date'] = pd.to_datetime(datetime.datetime.now())
            return positions

    def _get_user_categories(self):
        config = configparser.ConfigParser()
        config.read('/Users/p.matchenkov/Desktop/configurations/config.ini')
        login = config['AURORA_prod']['login']
        password = config['AURORA_prod']['password']
        if type(self.up) == tuple:
            sys.exit('only one user can be passed (has passed tuple)')
        else:
            url = f"https://cexprod.prosp.devexperts.com/dxweb/rest/api/register/client/default/{self.up}"
            response = json.dumps(requests.get(url, auth=HTTPBasicAuth(login, password)).json())
            return json.loads(response)

    def balance(self):
        req = f"""
        SELECT
            trim(trailing '$' from instruments.symbol) as symbol,
            principals.name AS user_id, 
            accounts.account_code,
            sum(positions.quantity) AS balance
        FROM dxcore.dxcore.positions as positions
            INNER JOIN dxcore.dxcore.accounts as accounts ON positions.account_id = accounts.id
            INNER JOIN dxcore.dxcore.principals as principals ON accounts.owner_id = principals.id
            INNER JOIN dxcore.dxcore.instruments as instruments  on positions.instrument_id = instruments.id 
        WHERE
            positions.quantity != 0
            AND positions.code IS NULL
            AND clearing_code IN ('LIVE')
            AND positions.account_id NOT IN (121, 70500, 213023, 212931)
            AND positions.quantity > 0
            AND {self.user_req_cond}
        GROUP BY
            accounts.account_code, principals.name, instruments.symbol
            """
        with self.engine_dxcore.connect() as conn:
            df = pd.DataFrame(conn.execute(text(req))).astype({'balance': float})

        # get BTC and ETH prices for convert balance to usd
        ticker = yf.Ticker("BTC-USD")
        btc = ticker.history(start=f"{datetime.date.today()}").reset_index().iloc[0]['Close']
        ticker = yf.Ticker("ETH-USD")
        eth = ticker.history(start=f"{datetime.date.today()}").reset_index().iloc[0]['Close']

        df['usd'] = 0
        df.loc[df['symbol'] == 'BTC', 'usd'] = df['balance'] * btc
        df.loc[df['symbol'] == 'ETH', 'usd'] = df['balance'] * eth
        df.loc[df['symbol'] == 'USDT', 'usd'] = df['balance'] * 1
        df = df.groupby('user_id').agg({'usd': sum})
        return df


class KeyClock():
    def __init__(self, users):
        if isinstance(users, list):
            if len(users) > 1:
                self.ups = users
            elif len(users) == 1:
                self.ups = users
        elif isinstance(users, str):
            self.ups = [users]
        else:
            sys.exit('Datatype error, should be str or list format')


    def _get_kc_token(self):
        config = configparser.ConfigParser()
        config.read('/Users/p.matchenkov/Desktop/configurations/config.ini')
        username = config['KEYCLOAK_prod']['login']
        password = config['KEYCLOAK_prod']['password']
        base_url = config['KEYCLOAK_prod']['client_secret']
        json_data = {
            'grant_type': 'password',
            'username': username,
            'password': password,
            'client_id': 'support-api',
            'scope': 'openid',
            'client_secret': base_url
        }
        req_kc_token = requests.post(f'{base_url}/auth/realms/master/protocol/openid-connect/token',
                          data=json_data, verify=False)
        token_kc = json.loads(req_kc_token.text)['access_token']
        return token_kc


    def personal_info(self, token_kc):
        ups_info = pd.DataFrame()
        for up in self.ups:
            try:
                # request data by api keyclock
                req_up_info = requests.get(f"https://auth.prod.broker.internal/auth/admin/realms/general/users?username={up}",
                                            headers={'Authorization': f'bearer {token_kc}'}, verify=False)
                req_up_info.raise_for_status()
            except requests.exceptions.RequestException as err:
                print(req_up_info.status_code)
                raise SystemExit(err)

        client_for_check = pd.DataFrame(json.loads(req_up_info.text))
        df = pd.concat([client_for_check.drop(['attributes'], axis=1),
                        client_for_check['attributes'].apply(pd.Series)], axis=1)
        # df = df[['username', 'email', 'country', 'phoneNumber']] # исправить, если нет phoneNumber у юзера
        # extract country and number from list type
        # df['country'] = df['country'].apply(lambda x: x[0]) # исправить, если нет phoneNumber у юзера
        # df['phoneNumber'] = df['phoneNumber'].apply(lambda x: x[0]) # исправить, если нет phoneNumber у юзера
        ups_info = pd.concat([ups_info, df])
        return ups_info


