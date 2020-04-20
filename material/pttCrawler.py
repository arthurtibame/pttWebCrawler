#!/usr/bin/env python
# coding: utf-8
'''
To get information in each board.

getArticlesInfo(board_url: str,
             start_date: str('4/10'),
             end_date: str('4/15')) -> List( Dict[article_title, article_url, auth_id, date, article_content, comment_content] )
    Return a list of dictionaries which contain keys of article_url, auth_id and date.

getContent(article_url: str) -> Dict[article_title, article_url, auth_id, auth_name, date, article_content, comment_content]

listComments(comment_content: str) -> Dict[comment_id]: List( Dict[push_tag: str, comment: str, date: time] )

convertArticlesInfo( all_articles: List[ Dict[] ] ) -> List[ Tuple[str] ]
    The input shoud be like the data type which is returned by getArticlesInfo()
    Return a list of tuples, which can be insert into PTT_ARTICLE

convertCommentsContent( comment_dict: List[ Dict[ push-tag, push-userid, push-content, push-ipdatetime ] ] )
    
isArticleIdInDb( article_id: str ) -> Bool

'''

import requests
from bs4 import BeautifulSoup
import random
import time
import datetime
import logging

import connectDB

datetime_now = datetime.datetime.now()
datetime_1970 = datetime.datetime(1970, 1, 1)
end_date = datetime_now.strftime('%Y/%m/%d')
start_date = datetime_1970.strftime('%Y/%m/%d')

exist_article_id = connectDB.getAllArticleId()

# Set headers
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'}
cookies = {'over18': '1'}

# The format of date should be like 2020/04/15
def getArticlesInfo(board_url, start_date=start_date, end_date=end_date):
    all_articles = list()
    all_articles_index = 0
    one_page_articles = list()
    board_name = board_url.split('/')[-2]
    # Each id will be like Gossiping_M.1587040493.A.58B.html
    exist_board_article_id = connectDB.getAllArticleIdInOneBoard(board_name)
    
    start_date = datetime.datetime.strptime(start_date, '%Y/%m/%d')
    end_date = datetime.datetime.strptime(end_date, '%Y/%m/%d') + datetime.timedelta(days=1)
    print('start_date:', start_date)
    print('end_date:', end_date)
    print('===============')
    ss = requests.session()
    ss.cookies['over18'] = '1'
    res = ss.get(board_url, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')
    
    last_page_url = 'https://www.ptt.cc'
    article_created_date = start_date
  
    # The date of which article was created should be greater than or equal to start_date
    while last_page_url != '' and article_created_date >= start_date:
        too_early_article_count = 0 # To count the article amount whose created date is earlier than start_date
        # index1.html does not have 'href' key
        try:
            last_page_url = 'https://www.ptt.cc' + \
                            soup.select('div[class="btn-group btn-group-paging"] a.btn')[1]['href']           
        except KeyError:
            last_page_url = ''
            article_created_date = start_date

        titles_soup_list = soup.select('div.r-ent')
        for title_soup in titles_soup_list:
            try:
                article_url = 'https://www.ptt.cc' + title_soup.select('div.title a')[0]['href']
            except IndexError as e:
                print(e.args)
                print(title_soup)
                continue
            article_title = title_soup.select('div.title a')[0].text
            print(article_title)
            print(article_url)
            
            # If article id exists, pass
            if '_'.join(article_url.split('/')[-2:]) in exist_board_article_id:
                print('Exists!')
            else:
                article_content = getContent(article_url)
                # If the article does not have 'date' infotmation, 
                # refer the date in last article to set for this article
                try:
                    article_created_date = article_content['date']
                except KeyError:
                    try:
                        article_content['date'] = all_articles[all_articles_index - 1]['date']
                        article_created_date = article_content['date']
                    # If there is no date information in last article, set it to be end_date
                    except:
                        article_content['date'] = end_date
                        article_created_date = article_content['date']
                if article_created_date < start_date:
                    print('article_created_date < start_date')
                    too_early_article_count += 1
                    # Because there are some bottom articles that are not ordered by date, 
                    # maybe announcement or any other like that.
                    # The process will stop if it find out the first earlier-than-start-date-article
                    # without this condition, so if there are not so many such article, 
                    # the process is no need to stop.
                    if too_early_article_count > 20:
                        print('too_early_article_count > 20')
                        last_page_url = ''
                        break
                elif article_created_date > end_date:
                    print('article_created_date > end_date')
                    pass
                else:
                    all_articles.append(article_content)
                    one_page_articles.append(article_content)
                    print('Appended!')
                time.sleep(random.randint(1, 100) / 50)
        
        # Insert one page data into the two tables, PTT_ARTICLE and PTT_COMMENT
        insert_data = convertArticlesInfo(one_page_articles)
        connectDB.insertIntoPttArticle(insert_data)
        insert_data = convertCommentsContent(one_page_articles)
        connectDB.insertIntoPttComment(insert_data)
        one_page_articles = list()
                
        print('last_page_url:', last_page_url)
        if last_page_url == '': break
        
        # Get into the last page
        res = ss.get(last_page_url, headers=headers)
        soup = BeautifulSoup(res.text, 'html.parser')

    return all_articles

def getContent(article_url):
    article_content = dict()
    
    article_content['article_url'] = article_url
    ss = requests.session()
    ss.cookies['over18'] = '1'
    reconnection_time = 0
    while True:
        try:
            res = ss.get(article_url, headers=headers)
            break
        except:
            print('Connection refused by server!')
            print('Reconnecting!')
            time.sleep(random.randint(10, 20))
            reconnection_time += 1
            continue
    soup = BeautifulSoup(res.text, 'html.parser')

    # Get author information : 作者 -> 標題 -> 時間 -> 版名
    # Dict[article_url, auth_id, auth_name, date, article_content, comment_content]
    # These datas may not exist, so it is necessary to do try-catch
    try:
        article_content['auth_id'] = soup.select('div[class="article-metaline"]')[0] \
                                        .span.find_next_sibling() \
                                        .text \
                                        .split(' ')[0]
        article_content['auth_name'] = soup.select('div[class="article-metaline"]')[0] \
                                        .extract() \
                                        .span.find_next_sibling() \
                                        .text \
                                        .split(' ')[1][1:-1]
        article_content['title'] = soup.select('div[class="article-metaline"]')[0] \
                                        .extract() \
                                        .span.find_next_sibling() \
                                        .text 
        article_content['date'] = soup.select('div[class="article-metaline"]')[0] \
                                        .extract() \
                                        .span.find_next_sibling() \
                                        .text
        article_content['date'] = datetime.datetime.strptime(' '.join(article_content['date'].split(' ')[1:])
                                                             , '%b %d %X %Y')
        article_content['board_name'] = soup.select('div[class="article-metaline-right"]')[0] \
                                            .extract() \
                                            .span.find_next_sibling() \
                                            .text
    except IndexError:
        for k in ['auth_id', 'auth_name', 'title']:
            article_content[k] = 'Unknown'
        article_content['board_name'] = article_url.split('/')[-2]

    # Get article content
    article_content['article_content'] = soup.select('#main-content')[0].text.split('※ 發信站')[0]

    # Get the whole comments without classifying by comment ID
    article_content['comment_content'] = list()
    tmp_dict = dict()
    for tag in soup.select('#main-content')[0].select('span')[2:]:
        for comment_key in ['push-tag', 'push-userid', 'push-content', 'push-ipdatetime']:
            if comment_key in tag.attrs['class']:
                tmp_dict[comment_key] = tag.text.strip('\n')
                # The order of class in span-tag is ['push-tag', 'push-userid', 'push-content', 'push-ipdatetime']
                if comment_key == 'push-ipdatetime': # Reset tmp_dict()
                    article_content['comment_content'].append(tmp_dict)
                    tmp_dict = dict()
    
    return article_content

def listComments(comment_content):
    pass

# Convert the data returned from getArticlesInfo() to the format
# which can be successfully insert into PTT_ARTICLE
def convertArticlesInfo(all_articles):
    # This data can be the input of connectDB.insertIntoPttArticle(insert_data)
    insertData = list()
    for row in all_articles:
        try:
            up_tag = len([c['push-tag'] for c in row['comment_content'] if '推' in c['push-tag']])
            down_tag = len([c['push-tag'] for c in row['comment_content'] if '噓' in c['push-tag']])
        except:
            up_tag = 0
            down_tag = 0
        oneRowData = (
            row['title'],
            row['board_name'],
            '_'.join(row['article_url'].split('/')[-2:]),
            row['auth_id'],
            row['auth_name'],
            row['date'].strftime("%Y-%m-%d %H:%M:%S"),
            row['article_url'],
            row['date'].strftime("%Y-%m-%d %H:%M:%S"),
            row['date'].strftime("%Y-%m-%d %H:%M:%S"),
            up_tag,
            down_tag,
            row['article_content'],
            str(row['comment_content'])
        )
        insertData.append(oneRowData)
    return insertData

# Convert the data returned from getArticlesInfo()[i]['comment_content'] to the format
# which can be successfully insert into PTT_COMMENT
def convertCommentsContent(all_articles):
    # This data can be the input of connectDB.insertIntoPttComment(insert_data)
    insertData = list()
    for row in all_articles:
        try:
            article_id = '_'.join(row['article_url'].split('/')[-2:])
            for comment in row['comment_content']:
                comment_id = comment['push-userid']
                push_tag = comment['push-tag']
                comment_content = comment['push-content']
                comment_datetime = comment['push-ipdatetime']
                oneRowData = (
                    article_id,
                    comment_id,
                    push_tag,
                    comment_content,
                    comment_datetime
                )
                insertData.append(oneRowData)
        except:
            pass
    return insertData

def isArticleIdInDb(article_id):
    pass

def isBoardArticleIdInDb(article_id, board_name):
    pass

if __name__ == '__main__':
    board_url = 'https://www.ptt.cc/bbs/Gossiping/index.html'
    test_data = getArticlesInfo(board_url, start_date='2020/4/19')
    insert_data = convertArticlesInfo(test_data)
    print(len(test_data))
    print(len(insert_data))
#     connectDB.insertIntoPttArticle(insert_data)
    insert_data = convertCommentsContent(test_data)
    print(len(insert_data))
#     connectDB.insertIntoPttComment(insert_data)