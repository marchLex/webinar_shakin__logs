# SET correct date RegEx in get_date method
# SET correct log line RegEx in log_extract method

import re
import csv
from datetime import datetime


class LogScraper:
    def __init__(self, res_file, log_file):
        """
        :param res_file: csv with results filename
        :param log_file: logs filename
        """
        self.res_file = res_file
        self.log_file = log_file

        # Date format: Day, Month (letters), Year
        self.date_regex = r'(\d{2}/.*/\d{4})'
        # Log Format: IP, Date, Type Request, URL, Response Code, Size, Referrer, User Agent
        self.log_regex = r'(\S+).*\[(.*)\] "(\S+) (\S+) HTTP/.*" ' \
                         r'(\S+) (\S+) "(.*)" "(.*)"$'

        # If log line starts with abracadabra nginx.1 abracadabra :)
        # self.log_regex = r'domain\.com (\S+).*\[(.*)\] "(\S+) (\S+) HTTP.*" (\S+) (\S+) "(.*)" "(.*)"$'

    def get_date(self, raw_line):
        """
        Str format to date
        """
        try:
            match = re.search(rf'{self.date_regex}', raw_line)
            f_date = datetime.strptime(match.group(1), '%d/%b/%Y').date()
            return f_date
        except Exception as e:
            print('get_date() exception: \n', raw_line, e)

    def log_extract(self, raw_line):
        """
        Returns Date, IP, Type Request, URL, Response Code, Response Size, Referrer, User Agent
        """
        try:
            match = re.search(rf'{self.log_regex}', raw_line)
            return match.groups()
        except Exception as e:
            print('log_extract() exception: \n', e, type(e), raw_line)

    def worker(self):
        print('Start logs scraping. Please wait')
        with open(self.log_file, 'r') as f:
            with open(self.res_file, 'w', encoding='utf-8', newline='') as res:
                header = ["ip", "date", "type_request", "url", "resp_code", "resp_size", "ref_url", "user_agent"]
                my_csv = csv.writer(res, delimiter='\t')
                my_csv.writerow(header)
                for line in f:
                    try:
                        conn_data = list(self.log_extract(line))
                        conn_data[1] = self.get_date(conn_data[1])
                        # print(conn_data)
                        my_csv.writerow(conn_data)
                    except Exception as e:
                        print('worker() exception: \n', e, type(e))
                        continue

                print(f'Done! Check {self.res_file}')


if __name__ == '__main__':
    log_scrape = LogScraper('output.csv', 'logs_filename')
    log_scrape.worker()



