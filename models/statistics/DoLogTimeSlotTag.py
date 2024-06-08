import pandas as pd
from sqlalchemy import create_engine
from TagTools import rule_to_tuple

class DoLogTimeSlotTag(object):

    # @staticmethod
    # def rule_to_tuple(rule):
    #     start, end = map(int, rule.split("-"))
    #     return start, end

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

        # 读取基础标签表 tbl_basic_tags
        df = pd.read_sql('SELECT * FROM tbl_basic_tags', con=engine)

        # 取四级标签的 rule
        rule_row = df.query("level == 4 and id == 259").iloc[0]
        rule = rule_row['rule'].split(";")
        selectTable = rule[0].split("=")[1]
        selectField = rule[1].split("=")[1].split("|")

        # 取五级标签
        attr = df.query("level == 5 and pid == 259")[['name', 'rule']]
        attr[['start', 'end']] = attr['rule'].apply(rule_to_tuple).apply(pd.Series)

        # 读取日志表
        df2 = pd.read_sql(f'SELECT {", ".join(selectField)} FROM {selectTable}', con=engine)

        # 获取log_time的时间
        df2['log_time'] = pd.to_datetime(df2['log_time'])
        df2['hour'] = df2['log_time'].dt.hour

        # 按照用户ID和小时分组，统计次数
        time_slot_df = df2.groupby(['global_user_id', 'hour']).size().reset_index(name='count')

        # 获取每个用户访问次数最多的时间段
        time_slot_df['rank'] = time_slot_df.groupby('global_user_id')['count'].rank(method='first', ascending=False)
        most_frequent_times = time_slot_df[time_slot_df['rank'] == 1]

        # 打标签
        results = []
        for _, row in attr.iterrows():
            temp_df = most_frequent_times[(most_frequent_times['hour'] >= row['start']) & (most_frequent_times['hour'] <= row['end'])]
            temp_df['timeSlot'] = row['name']
            results.append(temp_df[['global_user_id', 'timeSlot']].rename(columns={'global_user_id': 'userId'}))

        rst = pd.concat(results)

        # 存储打好标签的数据
        rst.to_sql('tbl_logTimeSlot_tag', con=engine, if_exists='replace', index=False)
        print("浏览时段标签计算完成！")

if __name__ == '__main__':
    DoLogTimeSlotTag.start()
