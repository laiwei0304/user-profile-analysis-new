import pandas as pd
from sqlalchemy import create_engine
from MLModelTools import MLModelTools


class DoRfeTag(object):

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

        # 读取数据
        df = pd.read_sql('SELECT * FROM tbl_logs', con=engine)

        # 计算RFE值
        rfe_df = df.groupby("global_user_id").agg(
            last_time=pd.NamedAgg(column="log_time", aggfunc="max"),
            frequency=pd.NamedAgg(column="loc_url", aggfunc="count"),
            engagements=pd.NamedAgg(column="loc_url", aggfunc="nunique")
        ).reset_index()
        rfe_df['recency'] = (pd.to_datetime('now') - pd.to_datetime(rfe_df['last_time'])).dt.days

        # 按照RFE值进行打分
        rfe_df['r_score'] = pd.cut(rfe_df['recency'], bins=[0, 15, 30, 45, 60, float('inf')], labels=[5, 4, 3, 2, 1],
                                   right=False).astype(float)
        rfe_df['f_score'] = pd.cut(rfe_df['frequency'], bins=[0, 99, 199, 299, 399, float('inf')],
                                   labels=[1, 2, 3, 4, 5], right=False).astype(float)
        rfe_df['e_score'] = pd.cut(rfe_df['engagements'], bins=[0, 49, 149, 199, 249, float('inf')],
                                   labels=[1, 2, 3, 4, 5], right=False).astype(float)

        # 使用RFE_SCORE进行KMeans聚类（K=4）
        model = MLModelTools.load_model(rfe_df[['r_score', 'f_score', 'e_score']], "rfe")
        rfe_df['prediction'] = model.predict(rfe_df[['r_score', 'f_score', 'e_score']])

        # 获取聚类中心，并根据rfe大小修改索引
        centers = model.cluster_centers_
        index_map = {i: v for i, v in enumerate(centers.flatten())}
        rfe_df['cluster'] = rfe_df['prediction'].map(index_map)

        # 读取基础标签表tbl_basic_tags
        df2 = pd.read_sql('SELECT * FROM tbl_basic_tags', con=engine)
        attr = df2.query("level == 5 and pid == 307")[['name', 'rule']]

        # 打标签
        rst = rfe_df.merge(attr, left_on='prediction', right_on='rule', how='left')
        rst = rst.drop(columns=['prediction', 'rule']).rename(columns={'name': 'rfe'})
        rst = rst.sort_values(by='global_user_id')

        # 存储打好标签的数据
        rst.to_sql('tbl_rfe_tag', con=engine, if_exists='replace', index=False)
        print("用户活跃度标签计算完成！")


if __name__ == '__main__':
    DoRfeTag.start()
