#!/use/bin/env python
# coding: utf-8

from flask import Flask, request
from flask import render_template
from flask import Response
from kafka import KafkaConsumer
from pykafka import KafkaClient
from urllib import parse
import webbrowser
import datetime
import time

import getBoards
import pttCrawler
import connectDB

app = Flask(__name__, static_url_path='/static', static_folder='./static')
app.config['JSON_AS_ASCII'] = False

@app.route('/')
def index():
    board_dict = getBoards.getPapularBoards()
    error_process_list = connectDB.getErrorProcess()
    error_process_url_list = list()
    for r in error_process_list:
        url_format = '/crawling?board_url={}&start_date={}&end_date={}&process_id={}'
        q = parse.quote('https://www.ptt.cc/bbs/{}/index.html'.format(r[2]))
        st_date = r[1][1:-1].split(',')[0]
        en_date = r[1][1:-1].split(',')[1]
        pid = r[0]
        error_process_url_list.append(url_format.format(q, st_date, en_date, pid))
    
    history_process_list = connectDB.getHistoryProcess()
    history_process_url_list = list()
    for r in history_process_list:
        url_format = '/crawling?board_url={}&start_date={}&end_date={}&process_id={}'
        q = parse.quote('https://www.ptt.cc/bbs/{}/index.html'.format(r[2]))
        st_date = r[1][1:-1].split(',')[0]
        en_date = r[1][1:-1].split(',')[1]
        pid = r[0]
        history_process_url_list.append(url_format.format(q, st_date, en_date, pid))
    
    return render_template('index.html', 
                           board_dict=board_dict,
                           error_process_list=error_process_list,
                           error_process_url_list=error_process_url_list,
                           history_process_list=history_process_list,
                           history_process_url_list=history_process_url_list)

@app.route('/crawling')
def crawling():
    start_date = request.args.get('start_date').replace('-', '/')
    end_date = request.args.get('end_date').replace('-', '/')
    board_url = request.args.get('board_url')
    board_name = board_url.split('/')[-2]
    process_id = request.args.get('process_id')
    if process_id == None:
        process_id = board_name + str(int(time.time()))
    print('Process ID:', process_id)
    board_data = pttCrawler.getArticlesInfo(board_url=board_url, start_date=start_date, end_date=end_date, process_id=process_id)
    insert_data = pttCrawler.convertArticlesInfo(board_data)
    show_column = ['文章標題', '看板', '作者', '發布時間', '推文數', '噓文數']
    show_data = [r[0:2] + r[3:4] + r[5:6] + r[9:11] for r in insert_data]
    data_url = [u[6] for u in insert_data]
    return render_template('crawling.html',
                          start_date=start_date,
                          end_date=end_date,
                          board_url=board_url,
                          board_name=board_name,
                          process_id=process_id,
                          show_column=show_column,
                          show_data=show_data,
                          data_url=data_url)

@app.route('/log_monitor')
def log_monitor():
    client = KafkaClient(hosts='kafka:29092')
    def events():
        for i in client.topics['etllog'].get_simple_consumer():
            yield '{}\n\n'.format(i.value.decode())
    return Response(events(), mimetype='text/event-stream')

if __name__ == '__main__':
    webbrowser.open('http://localhost:5001/', new=0)
    app.run(debug=True, host='0.0.0.0', port=5001)