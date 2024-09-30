"""
Data manipulate
"""
import os
import pandas as pd
from dotenv import load_dotenv
import psycopg2 as ps

data = pd.read_csv("Insumos/application_data.csv")
dataprev = pd.read_csv("Insumos/previous_application.csv")
dataprev = dataprev.rename(columns={"SK_ID_PREV ": "SK_ID_PREV"})


def connect_to_postgres():
    '''
    establece los parametros para realizar la conexion a postgres
    '''
    host_name = os.getenv("DB_HOST")
    dbname = os.getenv("DB_NAME")
    port = os.getenv("DB_PORT")
    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    try:
        conx = ps.connect(host=host_name, dbname=dbname,
                          user=username, password=password, port=port)
    except ps.OperationalError as e:
        raise e
    else:
        print('conexion exitosa')
        return conx


def insert_data(curr,
                sk_id_curr, target, name_contract_type, code_gender, flag_own_car, flag_own_realty, cnt_children, amt_income_total, amt_credit, amt_annuity, amt_goods_price,
                name_type_suite, name_income_type, name_education_type, name_family_status, name_housing_type, region_population_relative, days_birth, days_employed, days_registration,
                days_id_publish, own_car_age, flag_mobil, flag_emp_phone, flag_work_phone, flag_cont_mobile, flag_phone, flag_email, occupation_type, cnt_fam_members, region_rating_client,
                region_rating_client_w_city, weekday_appr_process_start, hour_appr_process_start, reg_region_not_live_region, reg_region_not_work_region, live_region_not_work_region,
                reg_city_not_live_city, reg_city_not_work_city, live_city_not_work_city, organization_type, ext_source_1, ext_source_2, ext_source_3, apartments_avg, basementarea_avg,
                years_beginexpluatation_avg, years_build_avg, commonarea_avg, elevators_avg, entrances_avg, floorsmax_avg, floorsmin_avg, landarea_avg, livingapartments_avg, livingarea_avg,
                nonlivingapartments_avg, nonlivingarea_avg, apartments_mode, basementarea_mode, years_beginexpluatation_mode, years_build_mode, commonarea_mode,
                elevators_mode, entrances_mode, floorsmax_mode, floorsmin_mode, landarea_mode, livingapartments_mode, livingarea_mode, nonlivingapartments_mode, nonlivingarea_mode,
                apartments_medi, basementarea_medi, years_beginexpluatation_medi, years_build_medi, commonarea_medi, elevators_medi, entrances_medi, floorsmax_medi, floorsmin_medi,
                landarea_medi, livingapartments_medi, livingarea_medi, nonlivingapartments_medi, nonlivingarea_medi, fondkapremont_mode, housetype_mode, totalarea_mode, wallsmaterial_mode,
                emergencystate_mode, obs_30_cnt_social_circle, def_30_cnt_social_circle, obs_60_cnt_social_circle, def_60_cnt_social_circle, days_last_phone_change, flag_document_2,
                flag_document_3, flag_document_4, flag_document_5, flag_document_6, flag_document_7, flag_document_8, flag_document_9, flag_document_10, flag_document_11, flag_document_12,
                flag_document_13, flag_document_14, flag_document_15, flag_document_16, flag_document_17, flag_document_18, flag_document_19, flag_document_20, flag_document_21,
                amt_req_credit_bureau_hour, amt_req_credit_bureau_day, amt_req_credit_bureau_week, amt_req_credit_bureau_mon, amt_req_credit_bureau_qrt, amt_req_credit_bureau_year):

    insert_loan_data = """INSERT INTO loan_defaulter.application_data (sk_id_curr, target, name_contract_type, code_gender, flag_own_car, flag_own_realty, cnt_children, amt_income_total, amt_credit, amt_annuity, amt_goods_price, name_type_suite, name_income_type, name_education_type, name_family_status, name_housing_type, region_population_relative, days_birth, days_employed, days_registration, days_id_publish, own_car_age, flag_mobil, flag_emp_phone, flag_work_phone, flag_cont_mobile, flag_phone, flag_email, occupation_type, cnt_fam_members, region_rating_client, region_rating_client_w_city, weekday_appr_process_start, hour_appr_process_start, reg_region_not_live_region, reg_region_not_work_region, live_region_not_work_region, reg_city_not_live_city, reg_city_not_work_city, live_city_not_work_city, organization_type, ext_source_1, ext_source_2, ext_source_3, apartments_avg, basementarea_avg, years_beginexpluatation_avg, years_build_avg, commonarea_avg, elevators_avg, entrances_avg, floorsmax_avg, floorsmin_avg, landarea_avg, livingapartments_avg, livingarea_avg, nonlivingapartments_avg, nonlivingarea_avg, apartments_mode, basementarea_mode, years_beginexpluatation_mode, years_build_mode, commonarea_mode, 
                        elevators_mode, entrances_mode, floorsmax_mode, floorsmin_mode, landarea_mode, livingapartments_mode, livingarea_mode, nonlivingapartments_mode, nonlivingarea_mode, apartments_medi, basementarea_medi, years_beginexpluatation_medi, years_build_medi, commonarea_medi, elevators_medi, entrances_medi, floorsmax_medi, floorsmin_medi, landarea_medi, livingapartments_medi, livingarea_medi, nonlivingapartments_medi, nonlivingarea_medi, fondkapremont_mode, housetype_mode, totalarea_mode, wallsmaterial_mode, emergencystate_mode, obs_30_cnt_social_circle, def_30_cnt_social_circle, obs_60_cnt_social_circle, def_60_cnt_social_circle, days_last_phone_change, flag_document_2, flag_document_3, flag_document_4, flag_document_5, flag_document_6, flag_document_7, flag_document_8, flag_document_9, flag_document_10, flag_document_11, flag_document_12, flag_document_13, flag_document_14, flag_document_15, flag_document_16, flag_document_17, flag_document_18, flag_document_19, flag_document_20, flag_document_21, amt_req_credit_bureau_hour, amt_req_credit_bureau_day, amt_req_credit_bureau_week, amt_req_credit_bureau_mon, amt_req_credit_bureau_qrt, amt_req_credit_bureau_year)                    
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                        ,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

    insert_row = (sk_id_curr, target, name_contract_type, code_gender, flag_own_car, flag_own_realty, cnt_children, amt_income_total, amt_credit, amt_annuity, amt_goods_price, name_type_suite, name_income_type, name_education_type, name_family_status, name_housing_type, region_population_relative, days_birth, days_employed, days_registration, days_id_publish, own_car_age, flag_mobil, flag_emp_phone, flag_work_phone, flag_cont_mobile, flag_phone, flag_email, occupation_type, cnt_fam_members, region_rating_client, region_rating_client_w_city, weekday_appr_process_start, hour_appr_process_start, reg_region_not_live_region, reg_region_not_work_region, live_region_not_work_region, reg_city_not_live_city, reg_city_not_work_city, live_city_not_work_city, organization_type, ext_source_1, ext_source_2, ext_source_3, apartments_avg, basementarea_avg, years_beginexpluatation_avg, years_build_avg, commonarea_avg, elevators_avg, entrances_avg, floorsmax_avg, floorsmin_avg, landarea_avg, livingapartments_avg, livingarea_avg, nonlivingapartments_avg, nonlivingarea_avg, apartments_mode, basementarea_mode, years_beginexpluatation_mode, years_build_mode, commonarea_mode,
                  elevators_mode, entrances_mode, floorsmax_mode, floorsmin_mode, landarea_mode, livingapartments_mode, livingarea_mode, nonlivingapartments_mode, nonlivingarea_mode, apartments_medi, basementarea_medi, years_beginexpluatation_medi, years_build_medi, commonarea_medi, elevators_medi, entrances_medi, floorsmax_medi, floorsmin_medi, landarea_medi, livingapartments_medi, livingarea_medi, nonlivingapartments_medi, nonlivingarea_medi, fondkapremont_mode, housetype_mode, totalarea_mode, wallsmaterial_mode, emergencystate_mode, obs_30_cnt_social_circle, def_30_cnt_social_circle, obs_60_cnt_social_circle, def_60_cnt_social_circle, days_last_phone_change, flag_document_2, flag_document_3, flag_document_4, flag_document_5, flag_document_6, flag_document_7, flag_document_8, flag_document_9, flag_document_10, flag_document_11, flag_document_12, flag_document_13, flag_document_14, flag_document_15, flag_document_16, flag_document_17, flag_document_18, flag_document_19, flag_document_20, flag_document_21, amt_req_credit_bureau_hour, amt_req_credit_bureau_day, amt_req_credit_bureau_week, amt_req_credit_bureau_mon, amt_req_credit_bureau_qrt, amt_req_credit_bureau_year)

    curr.execute(insert_loan_data, insert_row)


def add_to_postgresql(curr, data):
    for _, row in data.iterrows():
        insert_data(curr, row['SK_ID_CURR'], row['TARGET'], row['NAME_CONTRACT_TYPE'], row['CODE_GENDER'], row['FLAG_OWN_CAR'], row['FLAG_OWN_REALTY'], row['CNT_CHILDREN'], row['AMT_INCOME_TOTAL'], row['AMT_CREDIT'], row['AMT_ANNUITY'], row['AMT_GOODS_PRICE'], row['NAME_TYPE_SUITE'], row['NAME_INCOME_TYPE'], row['NAME_EDUCATION_TYPE'], row['NAME_FAMILY_STATUS'], row['NAME_HOUSING_TYPE'], row['REGION_POPULATION_RELATIVE'], row['DAYS_BIRTH'], row['DAYS_EMPLOYED'], row['DAYS_REGISTRATION'], row['DAYS_ID_PUBLISH'], row['OWN_CAR_AGE'], row['FLAG_MOBIL'], row['FLAG_EMP_PHONE'], row['FLAG_WORK_PHONE'], row['FLAG_CONT_MOBILE'], row['FLAG_PHONE'], row['FLAG_EMAIL'], row['OCCUPATION_TYPE'], row['CNT_FAM_MEMBERS'], row['REGION_RATING_CLIENT'], row['REGION_RATING_CLIENT_W_CITY'], row['WEEKDAY_APPR_PROCESS_START'], row['HOUR_APPR_PROCESS_START'], row['REG_REGION_NOT_LIVE_REGION'], row['REG_REGION_NOT_WORK_REGION'], row['LIVE_REGION_NOT_WORK_REGION'], row['REG_CITY_NOT_LIVE_CITY'], row['REG_CITY_NOT_WORK_CITY'], row['LIVE_CITY_NOT_WORK_CITY'], row['ORGANIZATION_TYPE'], row['EXT_SOURCE_1'], row['EXT_SOURCE_2'], row['EXT_SOURCE_3'], row['APARTMENTS_AVG'], row['BASEMENTAREA_AVG'], row['YEARS_BEGINEXPLUATATION_AVG'], row['YEARS_BUILD_AVG'], row['COMMONAREA_AVG'], row['ELEVATORS_AVG'], row['ENTRANCES_AVG'], row['FLOORSMAX_AVG'], row['FLOORSMIN_AVG'], row['LANDAREA_AVG'], row['LIVINGAPARTMENTS_AVG'], row['LIVINGAREA_AVG'], row['NONLIVINGAPARTMENTS_AVG'], row['NONLIVINGAREA_AVG'], row['APARTMENTS_MODE'], row['BASEMENTAREA_MODE'], row['YEARS_BEGINEXPLUATATION_MODE'], row['YEARS_BUILD_MODE'],
                    row['COMMONAREA_MODE'], row['ELEVATORS_MODE'], row['ENTRANCES_MODE'], row['FLOORSMAX_MODE'], row['FLOORSMIN_MODE'], row['LANDAREA_MODE'], row['LIVINGAPARTMENTS_MODE'], row['LIVINGAREA_MODE'], row['NONLIVINGAPARTMENTS_MODE'], row['NONLIVINGAREA_MODE'], row['APARTMENTS_MEDI'], row['BASEMENTAREA_MEDI'], row['YEARS_BEGINEXPLUATATION_MEDI'], row['YEARS_BUILD_MEDI'], row['COMMONAREA_MEDI'], row['ELEVATORS_MEDI'], row['ENTRANCES_MEDI'], row['FLOORSMAX_MEDI'], row['FLOORSMIN_MEDI'], row['LANDAREA_MEDI'], row['LIVINGAPARTMENTS_MEDI'], row['LIVINGAREA_MEDI'], row['NONLIVINGAPARTMENTS_MEDI'], row['NONLIVINGAREA_MEDI'], row['FONDKAPREMONT_MODE'], row['HOUSETYPE_MODE'], row['TOTALAREA_MODE'], row['WALLSMATERIAL_MODE'], row['EMERGENCYSTATE_MODE'], row['OBS_30_CNT_SOCIAL_CIRCLE'], row['DEF_30_CNT_SOCIAL_CIRCLE'], row['OBS_60_CNT_SOCIAL_CIRCLE'], row['DEF_60_CNT_SOCIAL_CIRCLE'], row['DAYS_LAST_PHONE_CHANGE'], row['FLAG_DOCUMENT_2'], row['FLAG_DOCUMENT_3'], row['FLAG_DOCUMENT_4'], row['FLAG_DOCUMENT_5'], row['FLAG_DOCUMENT_6'], row['FLAG_DOCUMENT_7'], row['FLAG_DOCUMENT_8'], row['FLAG_DOCUMENT_9'], row['FLAG_DOCUMENT_10'], row['FLAG_DOCUMENT_11'], row['FLAG_DOCUMENT_12'], row['FLAG_DOCUMENT_13'], row['FLAG_DOCUMENT_14'], row['FLAG_DOCUMENT_15'], row['FLAG_DOCUMENT_16'], row['FLAG_DOCUMENT_17'], row['FLAG_DOCUMENT_18'], row['FLAG_DOCUMENT_19'], row['FLAG_DOCUMENT_20'], row['FLAG_DOCUMENT_21'], row['AMT_REQ_CREDIT_BUREAU_HOUR'], row['AMT_REQ_CREDIT_BUREAU_DAY'], row['AMT_REQ_CREDIT_BUREAU_WEEK'], row['AMT_REQ_CREDIT_BUREAU_MON'], row['AMT_REQ_CREDIT_BUREAU_QRT'], row['AMT_REQ_CREDIT_BUREAU_YEAR'])


def insert_prev_data(curr, sk_id_prev, sk_id_curr, name_contract_type, amt_annuity, amt_application, amt_credit, amt_down_payment, amt_goods_price, weekday_appr_process_start,
                     hour_appr_process_start, flag_last_appl_per_contract, nflag_last_appl_in_day, rate_down_payment, rate_interest_primary, rate_interest_privileged,
                     name_cash_loan_purpose, name_contract_status, days_decision, name_payment_type, code_reject_reason, name_type_suite, name_client_type, name_goods_category,
                     name_portfolio, name_product_type, channel_type, sellerplace_area, name_seller_industry, cnt_payment, name_yield_group, product_combination, days_first_drawing,
                     days_first_due, days_last_due_1st_version, days_last_due, days_termination, nflag_insured_on_approval):

    insert_prev_data = """INSERT INTO loan_defaulter.application_prev_data (sk_id_prev, sk_id_curr, name_contract_type, amt_annuity, amt_application, amt_credit, amt_down_payment, amt_goods_price, weekday_appr_process_start,
                    hour_appr_process_start, flag_last_appl_per_contract, nflag_last_appl_in_day, rate_down_payment, rate_interest_primary, rate_interest_privileged,
                    name_cash_loan_purpose, name_contract_status, days_decision, name_payment_type, code_reject_reason, name_type_suite, name_client_type, name_goods_category,
                    name_portfolio, name_product_type, channel_type, sellerplace_area, name_seller_industry, cnt_payment, name_yield_group, product_combination, days_first_drawing,
                    days_first_due, days_last_due_1st_version, days_last_due, days_termination, nflag_insured_on_approval)                    
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

    insert_prev_row = (sk_id_prev, sk_id_curr, name_contract_type, amt_annuity, amt_application, amt_credit, amt_down_payment, amt_goods_price, weekday_appr_process_start,
                       hour_appr_process_start, flag_last_appl_per_contract, nflag_last_appl_in_day, rate_down_payment, rate_interest_primary, rate_interest_privileged,
                       name_cash_loan_purpose, name_contract_status, days_decision, name_payment_type, code_reject_reason, name_type_suite, name_client_type, name_goods_category,
                       name_portfolio, name_product_type, channel_type, sellerplace_area, name_seller_industry, cnt_payment, name_yield_group, product_combination, days_first_drawing,
                       days_first_due, days_last_due_1st_version, days_last_due, days_termination, nflag_insured_on_approval)

    curr.execute(insert_prev_data, insert_prev_row)


def add_prev_to_postgresql(curr, dataprev):
    for _, row in dataprev.iterrows():
        insert_prev_data(curr, row['SK_ID_PREV'], row['SK_ID_CURR'], row['NAME_CONTRACT_TYPE'], row['AMT_ANNUITY'], row['AMT_APPLICATION'], row['AMT_CREDIT'], row['AMT_DOWN_PAYMENT'], row['AMT_GOODS_PRICE'], row['WEEKDAY_APPR_PROCESS_START'], row['HOUR_APPR_PROCESS_START'], row['FLAG_LAST_APPL_PER_CONTRACT'], row['NFLAG_LAST_APPL_IN_DAY'], row['RATE_DOWN_PAYMENT'], row['RATE_INTEREST_PRIMARY'], row['RATE_INTEREST_PRIVILEGED'], row['NAME_CASH_LOAN_PURPOSE'], row['NAME_CONTRACT_STATUS'],
                         row['DAYS_DECISION'], row['NAME_PAYMENT_TYPE'], row['CODE_REJECT_REASON'], row['NAME_TYPE_SUITE'], row['NAME_CLIENT_TYPE'], row['NAME_GOODS_CATEGORY'], row['NAME_PORTFOLIO'], row['NAME_PRODUCT_TYPE'], row['CHANNEL_TYPE'], row['SELLERPLACE_AREA'], row['NAME_SELLER_INDUSTRY'], row['CNT_PAYMENT'], row['NAME_YIELD_GROUP'], row['PRODUCT_COMBINATION'], row['DAYS_FIRST_DRAWING'], row['DAYS_FIRST_DUE'], row['DAYS_LAST_DUE_1ST_VERSION'], row['DAYS_LAST_DUE'], row['DAYS_TERMINATION'], row['NFLAG_INSURED_ON_APPROVAL'])


'''conn = connect_to_postgres()
cur = conn.cursor()

add_to_postgresql(cur, data)
add_prev_to_postgresql(cur, dataprev)

conn.commit()
cur.close()
conn.close()
'''
