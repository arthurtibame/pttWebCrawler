#!/usr/bin/env python
# coding: utf-8
'''
To inser date into database PTTDB.

insertIntoPttArticle( input_data: List[List[str]] ) -> effected_rows: int

insertIntoPttComment( input_data: List[List[str]] ) -> effected_rows: int

getAllArticleId() -> List[str]

getAllArticleIdInOneBoard( board_name: str ) -> List[str]

insertIntoPttEtlLog( process_id: str, etl_dt: datetime, crawled_range: str, etlStatus: str ) -> bool

insertIntoPttEtlDetailLog( process_id: str, crawled_range: str, etlStatus: str, etlStatusMessage: str ) -> bool
'''
import pymysql
import json
import datetime

# Load the parameters for db connection
with open('./db.conf', 'r', encoding='utf-8') as f:
    dbconf = json.loads(f.read())

def insertIntoPttArticle(insert_data):
    
    TABLE_NAME = """PTT_ARTICLE"""
      
    COLUMNS_STR = """
    `title` VARCHAR(60) NOT NULL COMMENT '文章標題',
    `boardName` VARCHAR(40) COMMENT '看板名稱',
    `articleId` VARCHAR(45) NOT NULL COMMENT '文章編號',
    `authorId` VARCHAR(40) NOT NULL,
    `authorName` VARCHAR(45) NULL DEFAULT NULL,
    `publishedTime` TIMESTAMP NOT NULL COMMENT '文章發佈時間戳記',
    `canonicalUrl` VARCHAR(100) NOT NULL COMMENT '文章URL',
    `createdTime` DATETIME NOT NULL,
    `updateTime` DATETIME NOT NULL,
    `pushUp` INT NOT NULL COMMENT '文章推數',
    `pushDown` INT NOT NULL COMMENT '文章噓數',
    `content` TEXT NULL DEFAULT NULL COMMENT '文章內容',
    `commentContent` TEXT NULL DEFAULT NULL COMMENT '留言內容',"""
    
    COLUMNS_LIST = [r.strip(' ').replace('`', '').split(' ')[0] for r in COLUMNS_STR.split('\n') if r != '']
    COLUMNS = ', '.join(COLUMNS_LIST)
    COLUMNS_FORMAT = ", ".join(['%s' for i in COLUMNS_LIST])
    
    sql = "INSERT INTO {} ({}) VALUES ({})".format(TABLE_NAME, COLUMNS, COLUMNS_FORMAT)
    try:
        conn = pymysql.connect(**dbconf)
        print('Successfully connected!')
    except:
        return 0
    try:
        cursor = conn.cursor()
        effected_rows = cursor.executemany(sql, insert_data)
    #     cursor.execute(sql, insert_data)
    except:
        effected_rows = 0
    finally:
        cursor.close()
        conn.commit()
        conn.close()
    return effected_rows

def insertIntoPttComment(insert_data):
    
    TABLE_NAME = """PTT_COMMENT"""
      
    COLUMNS_STR = """
    `articleId` VARCHAR(45) NOT NULL COMMENT '文章編號',
    `commentId` VARCHAR(45) NOT NULL COMMENT '留言者ID',
    `pushTag` VARCHAR(10) COMMENT '推或噓',
    `commentContent` VARCHAR(100) NULL COMMENT '留言內容',
    `commentTime` VARCHAR(45) NULL COMMENT '留言時間'"""

    COLUMNS_LIST = [r.strip(' ').replace('`', '').split(' ')[0] for r in COLUMNS_STR.split('\n') if r != '']
    COLUMNS = ', '.join(COLUMNS_LIST)
    COLUMNS_FORMAT = ", ".join(['%s' for i in COLUMNS_LIST])
    
    sql = "INSERT INTO {} ({}) VALUES ({})".format(TABLE_NAME, COLUMNS, COLUMNS_FORMAT)
    
    try:
        conn = pymysql.connect(**dbconf)
        print('Successfully connected!')
    except:
        return 0
    try:
        cursor = conn.cursor()
        effected_rows = cursor.executemany(sql, insert_data)
    except:
        effected_rows = 0
    finally:
        conn.commit()
        cursor.close()
        conn.close()
    return effected_rows

def getAllArticleId():
    sql = """SELECT articleId FROM PTT_ARTICLE;"""
    
    try:
        conn = pymysql.connect(**dbconf)
        print('Successfully connected!')
    except:
        return 0
    cursor = conn.cursor()
    
    cursor.execute(sql)
    articles_id = [id[0] for id in cursor.fetchall()]
    
    conn.commit()
    cursor.close()
    conn.close()
    return articles_id

def getAllArticleIdInOneBoard(board_name):
    sql = """SELECT articleId FROM PTT_ARTICLE WHERE boardName = '{}';""".format(board_name)
    
    try:
        conn = pymysql.connect(**dbconf)
        print('Successfully connected!')
    except:
        return 0
    cursor = conn.cursor()
    
    cursor.execute(sql)
    articles_id = [id[0] for id in cursor.fetchall()]
    
    conn.commit()
    cursor.close()
    conn.close()
    return articles_id

def insertIntoPttEtlLog(process_id, etl_dt, record_dt, crawled_range, etl_status):
    
    TABLE_NAME = 'PTT_ETL_LOG'
    
    COLUMNS_STR = """
      `processId` VARCHAR(30) NOT NULL COMMENT 'boardName + date time',
      `etlDT` DATETIME NOT NULL COMMENT '此程序開始執行的時間',
      `recordDT` DATETIME NOT NULL COMMENT '此資訊被記錄的時間',
      `crawledRange` VARCHAR(30) NOT NULL COMMENT '此次爬取的時間範圍',
      `etlStatus` VARCHAR(15) NOT NULL COMMENT '三種狀態，start、executing及end',"""
    
    COLUMNS_LIST = [r.strip(' ').replace('`', '').split(' ')[0] for r in COLUMNS_STR.split('\n') if r != '']
    COLUMNS = ', '.join(COLUMNS_LIST)
    COLUMNS_FORMAT = ", ".join(['%s' for i in COLUMNS_LIST])
    
    sql = "INSERT INTO {} ({}) VALUES ({})".format(TABLE_NAME, COLUMNS, COLUMNS_FORMAT)
    insert_data = [(process_id, etl_dt, record_dt, crawled_range, etl_status)]
    
    try:
        conn = pymysql.connect(**dbconf)
        print('Successfully connected!')
    except:
        return 0
    
    try:
        cursor = conn.cursor()
        cursor.execute("""DELETE FROM PTT_ETL_LOG WHERE processId = '{}';""".format(process_id))
        effected_rows = cursor.executemany(sql, insert_data)
    except:
        return 0
    finally:
        conn.commit()
        cursor.close()
        conn.close()
    return effected_rows

def insertIntoPttEtlDetailLog(process_id, log_record_dt, etl_status, etlStatus_message):
    
    TABLE_NAME = 'PTT_ETL_DETAIL_LOG'
    
    COLUMNS_STR = """
    `processId` VARCHAR(30) NOT NULL COMMENT 'board_name + date time',
     `logRecordDT` DATETIME NOT NULL COMMENT '此資訊被記錄的時間',
    `etlStatus` VARCHAR(10) NOT NULL COMMENT '訊息狀態，info、warning、error',
    `etlStatusMessage` VARCHAR(200) NOT NULL)"""
    
    COLUMNS_LIST = [r.strip(' ').replace('`', '').split(' ')[0] for r in COLUMNS_STR.split('\n') if r != '']
    COLUMNS = ', '.join(COLUMNS_LIST)
    COLUMNS_FORMAT = ", ".join(['%s' for i in COLUMNS_LIST])
    
    sql = "INSERT INTO {} ({}) VALUES ({})".format(TABLE_NAME, COLUMNS, COLUMNS_FORMAT)
    insert_data = [(process_id, log_record_dt, etl_status, etlStatus_message)]
    
    try:
        conn = pymysql.connect(**dbconf)
        print('Successfully connected!')
    except:
        return 0
    
    try:
        cursor = conn.cursor()
        effected_rows = cursor.executemany(sql, insert_data)
    except:
        return 0
    finally:
        conn.commit()
        cursor.close()
        conn.close()
    return effected_rows

def getErrorProcess():
    TABLE_NAME = 'PTT_ETL_LOG'
    
    COLUMNS_STR = """
      `processId` VARCHAR(30) NOT NULL COMMENT 'boardName + date time',
      `etlDT` DATETIME NOT NULL COMMENT '此程序開始執行的時間',
      `recordDT` DATETIME NOT NULL COMMENT '此資訊被記錄的時間',
      `crawledRange` VARCHAR(30) NOT NULL COMMENT '此次爬取的時間範圍',
      `etlStatus` VARCHAR(15) NOT NULL COMMENT '三種狀態，start、executing及end',"""
    
    COLUMNS_LIST = [r.strip(' ').replace('`', '').split(' ')[0] for r in COLUMNS_STR.split('\n') if r != '']
    COLUMNS = ', '.join(COLUMNS_LIST)
    
    sql = """SELECT {}  
            FROM {}
            WHERE TIMESTAMPDIFF(MINUTE, recordDT, CURRENT_TIMESTAMP()) > 10
            AND etlStatus = 'executing';""".format(COLUMNS, TABLE_NAME)
    
    try:
        conn = pymysql.connect(**dbconf)
        print('Successfully connected!')
    except:
        return 0
    
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        output = cursor.fetchall()
    except:
        return 0
    finally:
        conn.commit()
        cursor.close()
        conn.close()
    return [[d[0], d[3].replace('/', '-'), d[0][0:-10]] for d in output]

def getHistoryProcess():
    TABLE_NAME = 'PTT_ETL_LOG'
    
    COLUMNS_STR = """
      `processId` VARCHAR(30) NOT NULL COMMENT 'boardName + date time',
      `etlDT` DATETIME NOT NULL COMMENT '此程序開始執行的時間',
      `recordDT` DATETIME NOT NULL COMMENT '此資訊被記錄的時間',
      `crawledRange` VARCHAR(30) NOT NULL COMMENT '此次爬取的時間範圍',
      `etlStatus` VARCHAR(15) NOT NULL COMMENT '三種狀態，start、executing及end',"""
    
    COLUMNS_LIST = [r.strip(' ').replace('`', '').split(' ')[0] for r in COLUMNS_STR.split('\n') if r != '']
    COLUMNS = ', '.join(COLUMNS_LIST)
    
    sql = """SELECT {}  
            FROM {}
            WHERE TIMESTAMPDIFF(MINUTE, recordDT, CURRENT_TIMESTAMP()) > 10
            AND etlStatus = 'end';""".format(COLUMNS, TABLE_NAME)
    
    try:
        conn = pymysql.connect(**dbconf)
        print('Successfully connected!')
    except:
        return 0
    
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        output = cursor.fetchall()
    except:
        return 0
    finally:
        conn.commit()
        cursor.close()
        conn.close()
    return [[d[0], d[3].replace('/', '-'), d[0][0:-10]] for d in output]