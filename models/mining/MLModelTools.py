import pandas as pd
from surprise import Dataset, Reader, SVD, KNNBasic
from surprise.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler
import os


class MLModelTools(object):

    @staticmethod
    def load_model(dataframe: pd.DataFrame, ml_type: str):
        model = None
        model_path = f"D:\\suncaper\\MLmodels\\{ml_type}.pkl"

        if os.path.exists(model_path):
            print("模型存在，加载模型")
            model = pd.read_pickle(model_path)
        else:
            print("模型不存在，开始训练模型")
            if ml_type == "rfm":
                model = MLModelTools.train_best_rfm_model(dataframe, 5)
            elif ml_type == "psm":
                model = MLModelTools.train_best_psm_model(dataframe, 5)
            elif ml_type == "rfe":
                model = MLModelTools.train_best_rfe_model(dataframe, 4)
            elif ml_type == "usg":
                model = MLModelTools.train_best_pipeline_model(dataframe)
            elif ml_type == "bp":
                model = MLModelTools.train_svd_model(dataframe)
            pd.to_pickle(model, model_path)

        return model

    @staticmethod
    def train_best_rfm_model(dataframe: pd.DataFrame, k_clusters):
        # 数据标准化
        scaler = MinMaxScaler()
        features = scaler.fit_transform(dataframe[['r_score', 'f_score', 'm_score']])

        # 训练KMeans模型
        best_model = None
        best_sse = float('inf')
        for max_iter in [5, 10, 20]:
            kmeans = KMeans(n_clusters=k_clusters, max_iter=max_iter, random_state=31)
            kmeans.fit(features)
            sse = kmeans.inertia_  # SSE (Sum of Squared Errors) 作为评估指标
            if sse < best_sse:
                best_sse = sse
                best_model = kmeans

        return best_model

    @staticmethod
    def train_best_psm_model(dataframe: pd.DataFrame, k_clusters):
        # 数据标准化
        scaler = MinMaxScaler()
        features = scaler.fit_transform(dataframe[['psm_score']])

        # 训练KMeans模型
        best_model = None
        best_sse = float('inf')
        for max_iter in [5, 10, 20]:
            kmeans = KMeans(n_clusters=k_clusters, max_iter=max_iter, random_state=31)
            kmeans.fit(features)
            sse = kmeans.inertia_  # SSE (Sum of Squared Errors) 作为评估指标
            if sse < best_sse:
                best_sse = sse
                best_model = kmeans

        return best_model

    @staticmethod
    def train_best_rfe_model(dataframe: pd.DataFrame, k_clusters):
        # 数据标准化
        scaler = StandardScaler()
        features = scaler.fit_transform(dataframe[['r_score', 'f_score', 'e_score']])

        # 训练KMeans模型
        best_model = None
        best_sse = float('inf')
        for max_iter in [5, 10, 20]:
            kmeans = KMeans(n_clusters=k_clusters, max_iter=max_iter, random_state=31)
            kmeans.fit(features)
            sse = kmeans.inertia_  # SSE (Sum of Squared Errors) 作为评估指标
            if sse < best_sse:
                best_sse = sse
                best_model = kmeans

        return best_model

    # @staticmethod
    # def train_best_kmeans_model(dataframe: pd.DataFrame, k_clusters):
    #     from sklearn.cluster import KMeans
    #     from sklearn.preprocessing import StandardScaler
    #
    #     # 数据标准化
    #     scaler = StandardScaler()
    #     features = scaler.fit_transform(dataframe[['feature1', 'feature2', 'feature3']])
    #
    #     # 训练KMeans模型
    #     best_model = None
    #     best_sse = float('inf')
    #     for max_iter in [5, 10, 20]:
    #         kmeans = KMeans(n_clusters=k_clusters, max_iter=max_iter, random_state=31)
    #         kmeans.fit(features)
    #         sse = kmeans.inertia_   # SSE (Sum of Squared Errors) 作为评估指标
    #         if sse < best_sse:
    #             best_sse = sse
    #             best_model = kmeans
    #
    #     return best_model

    @staticmethod
    def train_best_pipeline_model(dataframe: pd.DataFrame):
        # 构建Pipeline
        pipeline = Pipeline([
            ('scaler', StandardScaler()),  # 数据标准化
            ('clf', DecisionTreeClassifier(random_state=31))  # 决策树分类器
        ])

        # 定义参数网格
        param_grid = {
            'clf__max_depth': [5, 10, 20],  # 决策树的最大深度
            'clf__max_features': [None, 'sqrt', 'log2'],  # 每次分裂时考虑的特征数量
            'clf__criterion': ['gini', 'entropy']  # 划分标准
        }

        # 提取特征和标签
        features = dataframe[['color', 'product']]
        label = dataframe['label']

        # 网格搜索
        grid_search = GridSearchCV(pipeline, param_grid, cv=3, scoring='accuracy')
        grid_search.fit(features, label)

        # 返回最佳模型
        return grid_search.best_estimator_

    @staticmethod
    def train_svd_model(dataframe: pd.DataFrame):
        # 使用Surprise库的SVD算法进行推荐
        reader = Reader(rating_scale=(1, dataframe['rating'].max()))
        data = Dataset.load_from_df(dataframe[['userId', 'productId', 'rating']], reader)
        trainset, _ = train_test_split(data, test_size=0.2)
        algo = SVD()
        algo.fit(trainset)
        return algo
