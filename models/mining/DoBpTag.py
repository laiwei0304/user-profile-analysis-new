import pandas as pd
from sqlalchemy import create_engine
from tools.TagTools import url_to_product, string_to_product
from MLModelTools import MLModelTools


class DoBpTag(object):

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
        df = pd.read_sql('SELECT * FROM tbl_logs_new', con=engine)

        # 提取 productId
        df['productId'] = df['loc_url'].apply(url_to_product)
        df = df[['global_user_id', 'productId']]
        df.columns = ['userId', 'productId']
        df = df[df['productId'] != 'not_a_product']

        # 统计每个用户点击各个商品的次数
        ratings_df = df.groupby(['userId', 'productId']).size().reset_index(name='rating')
        ratings_df['userId'] = ratings_df['userId'].astype(int)
        ratings_df['productId'] = ratings_df['productId'].astype(int)
        ratings_df['rating'] = ratings_df['rating'].astype(int)

        # 使用Surprise库的SVD算法进行推荐
        als_model = MLModelTools.load_model(ratings_df, "bp")

        # 获取每个用户的推荐
        def get_top_n(predictions, n=5):
            top_n = {}
            for uid, iid, true_r, est, _ in predictions:
                if uid not in top_n:
                    top_n[uid] = []
                top_n[uid].append((iid, est))
            for uid, user_ratings in top_n.items():
                user_ratings.sort(key=lambda x: x[1], reverse=True)
                top_n[uid] = [iid for iid, _ in user_ratings[:n]]
            return top_n

        # 构造推荐结果的DataFrame
        rec_df = pd.DataFrame([
            {'userId': user_id, 'productIds': ','.join(map(str, items))}
            for user_id, items in get_top_n(als_model.test(als_model.trainset.build_anti_testset()), n=5).items()
        ])
        rec_df['products'] = rec_df['productIds'].apply(string_to_product)
        rec_df[['top1', 'top2', 'top3', 'top4', 'top5']] = pd.DataFrame(rec_df['products'].tolist(), index=rec_df.index)
        rec_df = rec_df[['userId', 'top1', 'top2', 'top3', 'top4', 'top5']]

        # 存储打好标签的数据
        rec_df.to_sql('tbl_bp_tag', con=engine, if_exists='replace', index=False)
        print("用户购物偏好标签计算完成！")


if __name__ == '__main__':
    DoBpTag.start()
