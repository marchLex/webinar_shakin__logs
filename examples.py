import pandas as pd
import matplotlib.pyplot as plt

from log_unite import LogExtract
from log_scraper import LogScraper
from log_tools import FilterTools, IPTools, PageClassifier

# -----------------
# Объединение логов
log_unite = LogExtract('log_file', 'log_directory/')
log_unite.worker()

# -----------------
# Извлечение логов
# Date format: Day, Month (letters), Year
date_regex = r'(\d{2}/.*/\d{4})'
# Log Format: IP, Date, Type Request, URL, Response Code, Size, Referrer, User Agent
log_regex = r'(\S+).*\[(.*)\] "(\S+) (\S+) HTTP/.*" ' \
                 r'(\S+) (\S+) "(.*)" "(.*)"$'

# If log line starts with abracadabra nginx.1 abracadabra :)
# self.log_regex = r'domain\.com (\S+).*\[(.*)\] "(\S+) (\S+) HTTP.*" (\S+) (\S+) "(.*)" "(.*)"$'
log_scraper = LogScraper('result_file.csv', 'logs_file', date_regex, log_regex)
log_scraper.worker()

# -----------------
# Загружаем датафрейм
# -----------------
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

df = pd.read_csv('logfile.csv',
                 delimiter='\t',
                 names=['ip', 'date', 'type_request', 'url', 'resp_code', 'resp_size', 'ref_url', 'user_agent'
                        # 'page_class'
                        # 'ip_host', 'ip_supernet'
                        ],
                 skiprows=1,
                 # parse_dates=['date'],  # Skip if we don't need it
                 # chunksize=1000000
                 )

# -----------------
# Классификация
# -----------------
# Faster classifier
# -----------------
# classify = PageClassifier(contains, match)
# df_list = []
# for num, chunk in enumerate(df, 1):
#     print('Start chunk ', num)
#     df_list.append(classify.worker(chunk))
# new_pd = pd.concat(df_list, ignore_index=True)
# new_pd.to_csv('classified.csv', sep='\t', index=False)

# Safer classifier
# -----------------
# classify = PageClassifier(contains, match)
# for num, chunk in enumerate(df):
#     print('Start chunk: ', num)
#     classify.worker(chunk).to_csv(
#         'classified.csv', sep='\t', index=False,
#         header=(num == 0),  # only write the header for the first chunk
#         mode='w' if num == 0 else 'a'  # append mode after the first iteration
#     )

# -----------------
# IP
# -----------------
for num, chunk in enumerate(df):
    print('Start chunk: ', num)
    ip_tools.validate_bots(ip_tools.get_ip_data(chunk)).to_csv(
        'logs/result_filename.csv', sep='\t', index=False,
        header=(num == 0),  # only write the header for the first chunk
        mode='w' if num == 0 else 'a'  # append mode after the first iteration
    )

# -----------------
# Визуализация
# -----------------
# Забираем из датафрейма только запросы ботов
df2 = df[['date', 'type_request', 'resp_code', 'user_agent']].loc[df['user_agent'].str.contains('google|yandex|bing', flags=re.I, regex=True, na=False)]
# Группируем запросы и считаем
df3 = df2.groupby(['date', 'resp_code', 'type_request']).size().reset_index(name='count')
# Получаем "фейковые" User-agent ботов
df_not_valid = df.loc[(df['user_agent'].str.contains('google|yandex|bing', flags=re.I, regex=True)) & (df['bot_valid'] == False)]
# И сбрасываем индекс
df_not_valid.reset_index(drop=True)

# Bar chart
df3.plot.bar(y='count', x='type_request')

# Line chart
df3.plot(y='count', x='date')

# Scatter
df3.plot.scatter(x='count', y='resp_code')

# Pie
labels = list(set(df3['resp_code']))
df3.plot.pie(y='count', labels=labels, figsize=(5, 5))
