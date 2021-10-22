import re
import ipaddress
import socket

import pandas as pd

from concurrent.futures import ThreadPoolExecutor


class FilterTools:
    """
    Simple tools for filtering and modifying logs DataFrames.
    - Filter DataFrame by RegEx match in selected column.
    - Get DataFrame by date range.
    """

    @staticmethod
    def filter_df(data_frame, col, pattern):
        """
        :param data_frame: DataFrame
        :type data_frame:
        :param col: DataFrame column name
        :type col: str
        :param pattern: RegEx condition
        :type pattern: str
        :return: filtered by RegEx condition
        """
        filtered_df = data_frame.loc[data_frame[col].str.contains(pattern, flags=re.I, regex=True, na=False)]
        return filtered_df

    @staticmethod
    def get_date_range(data_frame, start, end):
        """
        Get rows from dataframe by date range
        :param data_frame: DataFrame
        :type data_frame:
        :param start: start date (YYYY-MM-DD)
        :type start: str
        :param end: end date (YYYY-MM-DD)
        :type end: str
        :return: DataFrame by date range
        """
        df_date_range = data_frame
        period = pd.to_datetime([start, end])
        mask = (df_date_range['date'] >= period[0]) & (df_date_range['date'] <= period[1])
        return df_date_range.loc[mask]


class IPTools:
    """
    Tools for checking IP supernet and Hostname (rDNS) and validating bots.
    """

    @staticmethod
    def get_supernet(ip):
        """
        Gets supernet by IP
        :param ip:
        :type ip: str
        :return: IP and supernet
        :rtype tuple:
        """
        try:
            net = ipaddress.ip_network(ip)
            supernet = net.supernet()
            return ip, str(supernet)

        except Exception as e:
            # print('supernet exception: ', e, type(e))
            return ip, 'N/A'

    @staticmethod
    def get_host(ip):
        """
        Gets Hostname by IP
        :param ip:
        :type ip: str
        :return: IP and host
        :rtype tuple:
        """
        try:
            name, alias, address_list = socket.gethostbyaddr(ip)
            return ip, str(name)

        except Exception as e:
            # print('rdns exception: ', e, type(e))
            return ip, 'N/A'

    def get_ip_data(self, data_frame, supernet='yes', rdns='yes', workers=50):
        """
        Worker func to get ip data (supernet, rdns)
        :param data_frame: DataFrame
        :type data_frame:
        :param supernet: get supernet data permission
        :type supernet: str
        :param rdns: get rdns data permission
        :type rdns: str
        :param workers: num of threads to parse
        :type workers: int
        :return: DataFrame with IP data
        """
        print('Start get ip data func. Please wait...')
        df_ip_data = data_frame
        checked_ip = {'rdns': {}, 'supernet': {}}
        ips = list(set(df_ip_data['ip'].tolist()))

        with ThreadPoolExecutor(max_workers=workers) as executor:
            if supernet == 'yes':
                for net in executor.map(self.get_supernet, ips):
                    checked_ip['supernet'][net[0]] = net[1]
            if rdns == 'yes':
                for dns in executor.map(self.get_host, ips):
                    checked_ip['rdns'][dns[0]] = dns[1]

        df_ip_data['ip_host'] = df_ip_data['ip'].map(checked_ip['rdns'])
        df_ip_data['ip_supernet'] = df_ip_data['ip'].map(checked_ip['supernet'])

        return df_ip_data

    @staticmethod
    def validate_bots(data_frame, host_mask='google|yandex|msn'):
        """
        Bot validation by comparing a mask with IP Hostname
        :param data_frame: DataFrame with parsed IP data
        :type data_frame:
        :param host_mask: RegEx with Hostname mask for bots
        :type host_mask: str
        :return: DataFrame with bot validation data
        """
        print('Start bot validation func. Please wait...')
        df_bot_valid = data_frame
        df_bot_valid['bot_valid'] = df_bot_valid['ip_host'].str.contains(host_mask, flags=re.I, regex=True, na=False)
        return df_bot_valid


class PageClassifier:
    def __init__(self, contains_pat, match_pat):
        """
        :param match_pat: A dict with exact match patterns in values as lists
        :type match_pat: dict
        :param contains_pat: A dict with contains patterns in values as lists
        :type contains_pat: dict
        """
        self.match_pat = match_pat
        self.contains_pat = contains_pat

    def worker(self, data_frame):
        """
        Classifies pages by type (url patterns)
        :param data_frame: DataFrame to classify
        :return: Classified by URL patterns DataFrame
        """
        try:
            print('Start URL classify. Please wait...')
            for key, value in self.contains_pat.items():
                reg_exp = '|'.join(value)
                data_frame.loc[data_frame['url'].str.contains(reg_exp, regex=True), ['page_class']] = key

            for key, value in self.match_pat.items():
                data_frame.loc[data_frame['url'].isin(value), ['page_class']] = key

            return data_frame

        except Exception as e:
            print('page_classifier(): ', e, type(e))


if __name__ == '__main__':
    print('Import this module')
    # --- DataFrame structure ---
    # 'ip', 'date', 'type_request', 'url', 'resp_code', 'resp_size', 'ref_url', 'user_agent' - default fields
    # 'ip_host', 'ip_supernet' - IPTools.get_ip_data() func result
    # 'bot_valid' - IPTools.validate_bots() func result
    # 'page_class' - PageClassifier.worker() func result

    # --- Useful pd options ---
    # pd.options.mode.chained_assignment = None
    # pd.set_option('display.width', 199)
    # pd.set_option('display.max_columns', None)
    # pd.set_option('display.max_rows', None)
    # pd.options.mode.chained_assignment = None
