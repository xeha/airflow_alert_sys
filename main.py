from datetime import datetime, timedelta, date
from time import sleep
import pandas as pd
from io import StringIO
import requests
import seaborn as sns
import telegram
import numpy as np
import matplotlib.pyplot as plt
import io

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable
import pandahouse


class Getch:
    def __init__(self, query, db='simulator'):
        self.connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': db,
        }
        self.query = query
        self.getchdf

    @property
    def getchdf(self):
        try:
            self.df = pandahouse.read_clickhouse(self.query, connection=self.connection)

        except Exception as err:
            print("\033[31m {}".format(err))
            exit(0)
# Для телеги
my_token = 'тут_ваш_токен' # тут нужно заменить на токен вашего бота
bot = telegram.Bot(token=my_token) # получаем доступ
chat_id = -1001573194187 

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'k.agrova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 4, 23)
}


# Интервал запуска DAG
schedule_interval = '*/15 * * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_agrova_airflow_4():
    
    def check_anomaly(df, metric, a=3, n=5):
#     алгоритм поиска аномалий в данных (межквартильный размах)
        df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
        df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
        df['iqr'] = df['q75'] -  df['q25']
        df['up'] = df['q75'] + a*df['iqr']
        df['low'] = df['q25'] - a*df['iqr']
        
        df['up'] = df['up'].rolling(n, center=True, min_periods =1).mean()
        df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()
    
        if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1]> df['up'].iloc[-1]:
            is_alert = 1
        else:
            is_alert = 0
                
        return is_alert, df

    @task
    def run_alerts(chat = chat_id):
#     система алертов
        chat_id = -1001573194187
        bot = telegram.Bot(token= 'тут_ваш_токен')
        
        data = Getch("""SELECT
                            toStartOfFifteenMinutes(time) as ts
                            , toDate(time) as date
                            , formatDateTime(ts, '%R') as hm
                            , uniqExact(user_id) as users_feed
                            , countIf(user_id, action = 'view') as views
                            , countIf(user_id, action = 'like') as likes
                        FROM simulator_20230320.feed_actions
                        WHERE time >= today() -1 and time < toStartOfFifteenMinutes(now())
                        GROUP BY ts, date
                        ORDER BY ts
                        """).df
        print(data)
        
        metrics_list = ['users_feed', 'views', 'likes']
        for metric in metrics_list:
            # print(metric)
            df = data [['ts', 'date', 'hm', metric]].copy()
            is_alert, df = check_anomaly(df, metric)
            
            if is_alert == 1:
                msg = '''Метрика {metric}: \n текущее значение {current_val:.2f}\n отклонение от предыдущего значения {last_val_diff:.2%}'''.format(metric = metric,
                                                                                                                                            current_val = df[metric].iloc[-1],
                                                                                                                                            last_val_diff = abs(1 - (df[metric].iloc[-1]/df[metric].iloc[-2]))                                                                                                                                                                                                                )

            sns.set(rc={'figure.figsize':(16, 10)})
            plt.tight_layout()

            ax = sns.lineplot(x=df['ts'], y=df[metric], label = 'metric')
            ax = sns.lineplot(x=df['ts'], y=df['up'], label = 'up')
            ax = sns.lineplot(x=df['ts'], y=df['low'], label='low')

            for ind, label in enumerate(ax.get_xticklabels()):
                if ind % 15 == 0:
                    label.set_visible(True)
                else:
                    label.set_visible(False)

            ax.set(xlabel ='time')
            ax.set(ylabel=metric)

            ax.set_title(metric)
            ax.set(ylim =(0, None))

            plt.title('anomaly')
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'anomaly.png'
            plt.close()
            if is_alert == 1:
                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    run_alerts(chat = chat_id)

dag_agrova_airflow_4 = dag_agrova_airflow_4()