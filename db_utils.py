import mysql.connector
from datetime import datetime
import pandas as pd
# MySQL 연결 함수
def get_connection():
    return mysql.connector.connect(
        host='localhost',
        user='root',
        password='1234',
        database='water'
    )

# 자동측정망
def save_water_auto_measurement(df):
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
        INSERT INTO tbl_water_auto_measurement (
            siteId, msrDate, 
            M01, M02, M03, M04, M05, M06,
            M12, M13, M14, M15, M16, M17, M18, M19,
            M20, M21, M22, M23, M24, M25, M26, M27, M28, M29,
            M35, M36, M37, M38, M39, M40, M41,
            M69, M70, M71, M72, M73, M74, M75, M76, M77, M78, M79, M80, M81,
            M100
        ) VALUES (
            %(siteId)s, %(msrDate)s,
            %(M01)s, %(M02)s, %(M03)s, %(M04)s, %(M05)s, %(M06)s,
            %(M12)s, %(M13)s, %(M14)s, %(M15)s, %(M16)s, %(M17)s, %(M18)s, %(M19)s,
            %(M20)s, %(M21)s, %(M22)s, %(M23)s, %(M24)s, %(M25)s, %(M26)s, %(M27)s, %(M28)s, %(M29)s,
            %(M35)s, %(M36)s, %(M37)s, %(M38)s, %(M39)s, %(M40)s, %(M41)s,
            %(M69)s, %(M70)s, %(M71)s, %(M72)s, %(M73)s, %(M74)s, %(M75)s, %(M76)s, %(M77)s, %(M78)s, %(M79)s, %(M80)s, %(M81)s,
            %(M100)s
        );
        """
    cursor.execute(sql, df)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ OHLCV 데이터 MySQL 저장 완료")
# 측정망(직접)
def save_water_measurement(df):
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
    INSERT IGNORE INTO tbl_water_measurement (
        PT_NO, wmyr, wmod, wmwk, wmdep,
        lonDgr, lonMin, lonSec, latMin, latSec,
        wmcymd,
        itemLvl, itemAmnt, itemTemp, itemPh, itemDoc,
        itemBod, itemCod, itemSs, itemTcoli, itemTn, itemTp,
        itemCd, itemCn, itemPb, itemCr6, itemAs, itemHg, itemCu,
        itemAbs, itemPcb, itemOp, itemMn, itemTrans, itemCloa,
        itemCl, itemZn, itemCr, itemFe, itemPhenol, itemNhex, itemEc,
        itemPce, itemNo3n, itemNh3n, itemEcoli, itemPop, itemDtn, itemDtp, itemFl, itemCol,
        itemCcl4, itemDceth, itemDcm, itemBenzene, itemChcl3, itemToc, itemDehp, itemAntimon,
        itemDiox, itemHcho, itemHcb, itemNi, itemBa, itemSe
    ) VALUES (
        %(PT_NO)s, %(WMYR)s, %(WMOD)s, %(WMWK)s, %(WMDEP)s,
        %(LON_DGR)s, %(LON_MIN)s, %(LON_SEC)s, %(LAT_MIN)s, %(LAT_SEC)s,
        %(WMCYMD)s,
        %(ITEM_LVL)s, %(ITEM_AMNT)s, %(ITEM_TEMP)s, %(ITEM_PH)s, %(ITEM_DOC)s,
        %(ITEM_BOD)s, %(ITEM_COD)s, %(ITEM_SS)s, %(ITEM_TCOLI)s, %(ITEM_TN)s, %(ITEM_TP)s,
        %(ITEM_CD)s, %(ITEM_CN)s, %(ITEM_PB)s, %(ITEM_CR6)s, %(ITEM_AS)s, %(ITEM_HG)s, %(ITEM_CU)s,
        %(ITEM_ABS)s, %(ITEM_PCB)s, %(ITEM_OP)s, %(ITEM_MN)s, %(ITEM_TRANS)s, %(ITEM_CLOA)s,
        %(ITEM_CL)s, %(ITEM_ZN)s, %(ITEM_CR)s, %(ITEM_FE)s, %(ITEM_PHENOL)s, %(ITEM_NHEX)s, %(ITEM_EC)s,
        %(ITEM_PCE)s, %(ITEM_NO3N)s, %(ITEM_NH3N)s, %(ITEM_ECOLI)s, %(ITEM_POP)s, %(ITEM_DTN)s, %(ITEM_DTP)s, %(ITEM_FL)s, %(ITEM_COL)s,
        %(ITEM_CCL4)s, %(ITEM_DCETH)s, %(ITEM_DCM)s, %(ITEM_BENZENE)s, %(ITEM_CHCL3)s, %(ITEM_TOC)s, %(ITEM_DEHP)s, %(ITEM_ANTIMON)s,
        %(ITEM_DIOX)s, %(ITEM_HCHO)s, %(ITEM_HCB)s, %(ITEM_NI)s, %(ITEM_BA)s, %(ITEM_SE)s
    )
    """
    cursor.execute(sql, df)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ 수질 데이터 MySQL 저장 완료")
#  자동측정망
def load_water_auto_area():
    conn = get_connection()
    df = pd.read_sql("SELECT siteId FROM tbl_water_auto_area ",conn)
    conn.close()
    print("✅ MySQL에서 siteId 리스트 불러오기 완료")
    return df["siteId"].tolist() 


#  수질측정망
def load_water_area():
    conn = get_connection()
    df = pd.read_sql("SELECT PT_NO FROM tbl_water_area where major_category like '%금강%' ",conn)
    conn.close()
    print("✅ MySQL에서 OHLCV 데이터 불러오기 완료")
    return df

def load_water_algae_area():
    conn = get_connection()
    df = pd.read_sql("SELECT PT_NO FROM tbl_water_algae_area ",conn)
    conn.close()
    print("✅ MySQL에서 OHLCV 데이터 불러오기 완료")
    return df


def save_water_alage_measurement(df):
    conn = get_connection()
    cursor = conn.cursor()

    sql ="""
        INSERT IGNORE INTO tbl_water_algae_measurement (
            PT_NO, river_lkmh_se, swmn_nm, swmn_detail_nm, detail_adres, wmcymd,
            iem_wtrtp, iem_ph, iem_doc, iem_trp, iem_tur, iem_chla,
            iem_bgalage_cell_co, iem_bgalage_microsts, iem_bgalage_anba,
            iem_bgalage_osrtria, iem_bgalage_apzo,
            iem_geosm, iem_mib2, iem_microstlr
        ) VALUES (
            %(PT_NO)s, %(river_lkmh_se)s, %(swmn_nm)s, %(swmn_detail_nm)s, %(detail_adres)s, %(wmcymd)s,
            %(iem_wtrtp)s, %(iem_ph)s, %(iem_doc)s, %(iem_trp)s, %(iem_tur)s, %(iem_chla)s,
            %(iem_bgalage_cell_co)s, %(iem_bgalage_microsts)s, %(iem_bgalage_anba)s,
            %(iem_bgalage_osrtria)s, %(iem_bgalage_apzo)s,
            %(iem_geosm)s, %(iem_mib2)s, %(iem_microstlr)s
        )
        """
    cursor.execute(sql, df)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ 수질 데이터 MySQL 저장 완료")
def save_auto_measurement(data):
    conn = get_connection()
    cursor = conn.cursor()

    sql = """
    INSERT IGNORE INTO tbl_water_auto_measurement (
        siteId, msrDate,
        M01, M02, M03, M04, M05, M06,
        M12, M13, M14, M15, M16, M17, M18, M19, M20, M21, M22, M23, M24, M25, M26, M27, M28, M29,
        M35, M36, M37, M38, M39, M40, M41,
        M69, M70, M71, M72, M73, M74, M75, M76, M77, M78, M79, M80, M81, M100
    ) VALUES (
        %(SITE_ID)s, %(MSR_DATE)s,
        %(M01)s, %(M02)s, %(M03)s, %(M04)s, %(M05)s, %(M06)s,
        %(M12)s, %(M13)s, %(M14)s, %(M15)s, %(M16)s, %(M17)s, %(M18)s, %(M19)s, %(M20)s, %(M21)s, %(M22)s, %(M23)s, %(M24)s, %(M25)s, %(M26)s, %(M27)s, %(M28)s, %(M29)s,
        %(M35)s, %(M36)s, %(M37)s, %(M38)s, %(M39)s, %(M40)s, %(M41)s,
        %(M69)s, %(M70)s, %(M71)s, %(M72)s, %(M73)s, %(M74)s, %(M75)s, %(M76)s, %(M77)s, %(M78)s, %(M79)s, %(M80)s, %(M81)s, %(M100)s
    )
    """

    cursor.execute(sql, data)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ 자동 측정 데이터 MySQL 저장 완료")


def load_water_trade_data():
    conn = get_connection()
    # query = """
    #     SELECT 
    #         al_meas.pt_no, 
    #         al_meas.river_lkmh_se, 
    #         al_meas.swmn_nm, 
    #         al_meas.detail_adres, 
    #         al_meas.wmcymd, 
    #         al_meas.iem_bgalage_cell_co,
    #         CASE 
    #             WHEN al_meas.iem_bgalage_cell_co > 10000 THEN '심각'
    #             WHEN al_meas.iem_bgalage_cell_co > 1000  THEN '주의'
    #             WHEN al_meas.iem_bgalage_cell_co > 100   THEN '주의'
    #             WHEN al_meas.iem_bgalage_cell_co > 10    THEN '주의'
    #             ELSE '정상'
    #         END AS alert_level,
    #         auto_meas.*
    #     FROM 
    #         tbl_water_algae_measurement al_meas
    #     JOIN
    #         tbl_water_algae_area algae_area
    #         ON al_meas.PT_NO = algae_area.PT_NO
    #     JOIN
    #         tbl_water_auto_area auto_area
    #         ON auto_area.siteName = algae_area.PT_NM
    #     JOIN
    #         tbl_water_auto_measurement auto_meas
    #         ON auto_meas.siteId = auto_area.siteId 
    #        AND DATE(auto_meas.msrDate) = DATE(al_meas.wmcymd)
    # """
    query = """
                
        SELECT 
            al_meas.pt_no, 
            al_meas.river_lkmh_se, 
            al_meas.swmn_nm, 
            al_meas.detail_adres, 
            al_meas.wmcymd, 
            al_meas.iem_bgalage_cell_co,
            CASE 
                WHEN al_meas.iem_bgalage_cell_co > 10000 THEN '심각'
                WHEN al_meas.iem_bgalage_cell_co > 1000  THEN '경계'
                WHEN al_meas.iem_bgalage_cell_co > 100   THEN '주의'
                WHEN al_meas.iem_bgalage_cell_co > 10    THEN '관심'
                ELSE '정상'
            END AS alert_level,
            auto_meas.*
        FROM 
            tbl_water_algae_measurement al_meas
        JOIN
            tbl_water_algae_area algae_area
            ON al_meas.PT_NO = algae_area.PT_NO
        JOIN
            tbl_water_auto_area auto_area
        ON 
            (auto_area.siteName = algae_area.PT_NM  or algae_area.reference_addr = auto_area.siteName) and auto_area.major_category = algae_area.major_category
        JOIN
            tbl_water_auto_measurement auto_meas
        ON 
            auto_meas.siteId = auto_area.siteId 
        AND 
            DATE(auto_meas.msrDate) = DATE(al_meas.wmcymd)
        """

    df = pd.read_sql(query, conn)
    conn.close()
    print("✅ MySQL에서 수질 데이터 불러오기 완료")
    return df