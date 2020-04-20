#!/usr/bin/env python
# coding: utf-8
'''
To inser date into database PTTDB.

insertIntoPttArticle( input_data: List[List[str]] ) -> effected_rows: int

insertIntoPttComment( input_data: List[List[str]] ) -> effected_rows: int

getAllArticleId() -> List[str]

getAllArticleIdInOneBoard( board_name: str ) -> List[str]
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