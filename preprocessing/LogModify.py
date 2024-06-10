import pandas as pd
from sqlalchemy import create_engine

'''
class LogModify(object):

    @staticmethod
    def start():
        # MySQL 配置
        user = 'userbb'
        password = 'userbb'
        host = '8.130.94.175'
        port = '3306'
        database = 'test'
        url = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'

        # 创建数据库引擎
        engine = create_engine(url)

        # 读取表
        df = pd.read_sql('SELECT * FROM tbl_logs', con=engine)

        # 修改log_time列，加上1670天
        df['log_time'] = pd.to_datetime(df['log_time']) + pd.Timedelta(days=1670)

        # 显示转换后的DataFrame
        print(df.head(185))

        # 写回数据库
        df.to_sql('tbl_logs_new', con=engine, if_exists='replace', index=False)
        print("日志修改完成！")


if __name__ == '__main__':
    LogModify.start()
'''