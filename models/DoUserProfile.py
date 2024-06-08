import pandas as pd
from sqlalchemy import create_engine


class DoUserProfile(object):

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

        # 读取各个标签数据
        gender_df = pd.read_sql('SELECT * FROM tbl_gender_tag', con=engine)
        job_df = pd.read_sql('SELECT * FROM tbl_job_tag', con=engine)
        nationality_df = pd.read_sql('SELECT * FROM tbl_nationality_tag', con=engine)
        marriage_df = pd.read_sql('SELECT * FROM tbl_marriage_tag', con=engine)
        politicalFace_df = pd.read_sql('SELECT * FROM tbl_politicalface_tag', con=engine)
        isBlackList_df = pd.read_sql('SELECT * FROM tbl_isblacklist_tag', con=engine)
        rfm_df = pd.read_sql('SELECT * FROM tbl_rfm_tag', con=engine)
        rfe_df = pd.read_sql('SELECT * FROM tbl_rfe_tag', con=engine)
        psm_df = pd.read_sql('SELECT * FROM tbl_psm_tag', con=engine)
        usg_df = pd.read_sql('SELECT * FROM tbl_usg_tag', con=engine)
        ageRange_df = pd.read_sql('SELECT * FROM tbl_ageRange_tag', con=engine)
        buyFrequency_df = pd.read_sql('SELECT userId, value as buyFrequency FROM tbl_buyFrequency_tag', con=engine)
        consumeCycle_df = pd.read_sql('SELECT * FROM tbl_consumeCycle_tag', con=engine)
        exchangeRate_df = pd.read_sql('SELECT userId, value as exchangeRate FROM tbl_exchangeRate_tag', con=engine)
        lastLogin_df = pd.read_sql('SELECT * FROM tbl_lastLogin_tag', con=engine)
        logFrequency_df = pd.read_sql('SELECT userId, value as logFrequency FROM tbl_logFrequency_tag', con=engine)
        logTimeSlot_df = pd.read_sql('SELECT userId, timeSlot as logTimeSlot FROM tbl_logTimeSlot_tag', con=engine)
        maxOrder_df = pd.read_sql('SELECT userId, maxOrderRange FROM tbl_maxOrder_tag', con=engine)
        payType_df = pd.read_sql('SELECT userId, payment as payType FROM tbl_payType_tag', con=engine)
        returnRate_df = pd.read_sql('SELECT userId, value as returnRate FROM tbl_returnRate_tag', con=engine)
        unitPrice_df = pd.read_sql('SELECT userId, unitPriceRange FROM tbl_unitPrice_tag', con=engine)
        bp_df = pd.read_sql(
            'SELECT userId, top1 as BpTop1, top2 as BpTop2, top3 as BpTop3, top4 as BpTop4, top5 as BpTop5 FROM tbl_bp_tag',
            con=engine)

        # 合并所有标签数据
        add_df = gender_df.merge(job_df, on='userId', how='left') \
            .merge(nationality_df, on='userId', how='left') \
            .merge(marriage_df, on='userId', how='left') \
            .merge(politicalFace_df, on='userId', how='left') \
            .merge(isBlackList_df, on='userId', how='left') \
            .merge(rfm_df, on='userId', how='left') \
            .merge(rfe_df, on='userId', how='left') \
            .merge(psm_df, on='userId', how='left') \
            .merge(usg_df, on='userId', how='left') \
            .merge(ageRange_df, on='userId', how='left') \
            .merge(buyFrequency_df, on='userId', how='left') \
            .merge(consumeCycle_df, on='userId', how='left') \
            .merge(exchangeRate_df, on='userId', how='left') \
            .merge(lastLogin_df, on='userId', how='left') \
            .merge(logFrequency_df, on='userId', how='left') \
            .merge(logTimeSlot_df, on='userId', how='left') \
            .merge(maxOrder_df, on='userId', how='left') \
            .merge(payType_df, on='userId', how='left') \
            .merge(returnRate_df, on='userId', how='left') \
            .merge(unitPrice_df, on='userId', how='left') \
            .merge(bp_df, on='userId', how='left') \
            .sort_values(by='userId')

        # 存储合并后的数据
        add_df.to_sql('tbl_user_profile', con=engine, if_exists='replace', index=False)
        print("所有用户画像标签更新完成！")


if __name__ == '__main__':
    DoUserProfile.start()
