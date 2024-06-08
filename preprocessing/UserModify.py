import pandas as pd
from sqlalchemy import create_engine

'''
class UserModify(object):

    @staticmethod
    def start():
        # MySQL 配置
        user = 'userbb'
        password = 'userbb'
        host = '8.130.94.175'
        port = '3306'
        database = 'tags_dat'
        url = f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'

        # 创建数据库引擎
        engine = create_engine(url)

        # 读取表
        df = pd.read_sql('SELECT * FROM tbl_users', con=engine)

        # 修改lastLoginTime列，加上3067天
        df['lastLoginTime'] = pd.to_datetime(df['lastLoginTime'], unit='s') + pd.Timedelta(days=3067)

        # 显示转换后的DataFrame
        print(df.head(185))

        # 写回数据库
        df.to_sql('tbl_users_new', con=engine, if_exists='replace', index=False)
        print("用户数据修改完成！")

if __name__ == '__main__':
    UserModify.start()
'''