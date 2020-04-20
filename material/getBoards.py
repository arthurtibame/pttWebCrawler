#!/usr/bin/env python
# coding: utf-8
'''
To get each board name.

getPapularBoard() -> Dict[boardname]: [boardurl, boardtitle, boardclass]
    Return a list of each papular board name

getAllBoard() -> List[str]
    Return a list of most of board name
'''

import requests
from bs4 import BeautifulSoup

# Set headers
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36'}

# Target url
papular_board_url = 'https://www.ptt.cc/bbs/index.html'

def getPapularBoards():
    board_info = dict()
    res = requests.get(papular_board_url, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')
    boards_list = soup.select('a.board')
    for b in boards_list:
        board_name = b.select('div.board-name')[0].text
        board_url = 'https://www.ptt.cc' + b['href']
        board_title = b.select('div.board-title')[0].text
        board_class = b.select('div.board-class')[0].text

        tmp_dict = {
            'board_url': board_url,
            'board_title': board_title,
            'board_class': board_class
        }
        board_info[board_name] = tmp_dict
    
    return board_info

def getAllBoards():
    pass