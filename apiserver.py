#!/usr/bin/env python3

from flask import request
from flask import Flask
from flask import g
from flask import jsonify
from flask import abort
from flask import Response
import scraper.config
import scraper.account
import scraper.task
import json
import logging
import urllib.request
import urllib.parse
import scraper.datastore
from googlesearch import search
import requests
import re
import random
import time
import numpy as np
import redis
from config import get_redis_config
from ast import literal_eval
from requests.cookies import cookiejar_from_dict
from fbchat import Client
from bs4 import BeautifulSoup
import re
app = Flask(__name__)

API_VERSION = "/v1"

FACEBOOK_API_TOKEN="EAACBo7bZBFDkBAFR8twhTtaqIgFt7QylBRvmqWAuQy2HPq1jxpgXuLsk9Tqt15EFcHr4tZBGKiABBd52466us8FChhLyk3RGXE5etTuohQxu43ZAE19Qywj7GcFeYiXLWUbqK7p7i3O2GNpGMhN3jSeZC7pN3qxXtFgZCY2eLTQZDZD"
# This new long-lived access token will expire on June 11th, 2018

FACEBOOK_SEARCH_URL="https://graph.facebook.com/v2.12/search"
FACEBOOK_PICTURE_URL="https://graph.facebook.com/v2.12/picture"

ACCOUNT_ACTIVE = 'fb:accounts:active'
PROXY_LIST = 'proxies'
SUCCESS_PROXIES = 'success_proxies'
USER_COOKIES = 'fb:user:cookies'

useragent = "Mozilla/5.0 (Windows NT 6.3; WOW64; ; NCT50_AAP285C84A1328) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36"
headers = {
    'user-agent': "Mozilla/5.0 (Windows NT 6.3; WOW64; ; NCT50_AAP285C84A1328) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.90 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    'accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Cache-Control': "no-cache"
    }

friendsurl = 'https://m.facebook.com/friends/center/search/?q='

API_SECRET = "615976aa4c13ee5ed67ab3c3bd95621d"

def check_api_secret(request):
    secret = request.headers.get("secret")
    if API_SECRET != secret:
        abort(403)



def search_friends(keyword):
    url = friendsurl+keyword
    redisconfig = get_redis_config('config.ini')
    r   = redis.StrictRedis(host=redisconfig['host'],
                                        port=int(redisconfig['port']), db=0,
                                        password=redisconfig['password'])
    if(r.llen(ACCOUNT_ACTIVE) and r.llen(SUCCESS_PROXIES)):
        userpass = r.rpop(ACCOUNT_ACTIVE).decode('utf-8')
        success_prox_len = r.llen(SUCCESS_PROXIES)
        proxylist = r.rpop(SUCCESS_PROXIES).decode('utf-8')
        userpass = literal_eval(userpass)
        print(userpass)
        cookies = None
        if(r.llen(USER_COOKIES) == 0):
            cookies = None
            print("cookie is NONE")
        else:
            cookies = r.rpop(USER_COOKIES).decode()
            cookies = literal_eval(cookies)
            print("load cookie")
        try:
            client = Client(userpass[0], userpass[1],user_agent=useragent,user_proxy=proxylist,session_cookies = cookies)
            cookies = client.getSession()
            print('login success')
            resp = requests.Session()
            resp.cookies = cookiejar_from_dict(cookies)
            resp.proxies = proxylist
            resp.headers = headers
            rs = resp.get(url,timeout=5)
            print(url)
            r.lpush(USER_COOKIES,cookies)
            r.lpush(ACCOUNT_ACTIVE,userpass)
            r.lpush(SUCCESS_PROXIES,proxylist)
            return rs
        except Exception as error:
            print(error)
            return 0
        else:
            return 0

def google_search_user(keyword,limit = 2):
    maxties = 5 
    out = 0
    userdata = {}
    while(maxties>0 and out==0):
        out = search_friends(keyword)
        maxties = maxties-1
        print(maxties)
    with open('tempout','w') as f:
        f.write(out.text)
    if(out == 0):
        userdata = {'data':''}
    else:
        userdata = parser_search_info(out.text)
    return userdata

def parser_search_info(data):
    soup = BeautifulSoup(data,'lxml')
    templist = soup.find_all("div", {"class":'w ca'})
    userinfo = []
    regfriendsimg = r'class=\"cb .\" src=\"(.*?)\"'
    reguid = r'\/a\/mobile\/friends\/add_friend.php\?id=(.*?)&'
    for temp in templist:
        print(temp)
        user_image = re.search(regfriendsimg,str(temp)).group(1)
        profile_img_url = user_image.replace('\\3a ',':')
        profile_img_url = profile_img_url.replace('\\3d ','=')
        profile_img_url = profile_img_url.replace('\\26 ','&')
        profile_img_url = profile_img_url.replace('amp;','')
        name = temp.find("a", {"class":'cc'})
        user_id = re.search(reguid,str(temp)).group(1)
        useritem = {"id":user_id,"name":name.get_text(),"pic":profile_img_url}
        userinfo.append(useritem)
    return userinfo

def facebook_graph_api_search_user_proxy(keyword, limit=25, offset=0):
    params = urllib.parse.urlencode({'q': keyword, 'type': 'user', 'access_token': FACEBOOK_API_TOKEN,
                                     'limit': limit, 'offset': offset})
    url = "{}?{}".format(FACEBOOK_SEARCH_URL, params)
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as f:
        result_json = f.read().decode("utf-8")
        return result_json

def facebook_graph_api_search_user(keyword, limit=25, offset=0):
    params = urllib.parse.urlencode({'q': keyword, 'type': 'user', 'access_token': FACEBOOK_API_TOKEN,
                                     'limit': limit, 'offset': offset})
    url = "{}?{}".format(FACEBOOK_SEARCH_URL, params)
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as f:
        result_json = f.read().decode("utf-8")
        result = json.loads(result_json)
        ret = dict()
        if "data" in result:
            data = result['data']
            ret['data'] = data
            ids = ""
            for d in data:
                ids = ids + d['id'] + ","
            ids = ids[:-1]
            if ids != "":
                params = urllib.parse.urlencode({'access_token': FACEBOOK_API_TOKEN, 'redirect': 'false'})
                url = "{}?{}&ids={}".format(FACEBOOK_PICTURE_URL, params, ids)
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req) as p:
                    pic_data = p.read().decode("utf-8")
                    pics = json.loads(pic_data)
                for d in data:
                    if d['id'] in pics:
                        pic = pics[d['id']]['data']['url']
                        new_url = get_image_db().upload(pic)
                        d['pic'] = new_url

        if "paging" in result:
            paging = result['paging']
            if 'previous' in paging:
                previous = paging['previous']
                parsed_url = urllib.parse.urlparse(previous)
                parsed_query = urllib.parse.parse_qs(parsed_url.query)
                limit = parsed_query['limit']
                offset= parsed_query['offset']
                ret['previous'] = {'limit': limit, 'offset': offset}
            if 'next' in paging:
                next = paging['next']
                parsed_url = urllib.parse.urlparse(next)
                parsed_query = urllib.parse.parse_qs(parsed_url.query)
                limit = parsed_query['limit']
                offset= parsed_query['offset']
                ret['next'] = {'limit': limit, 'offset': offset}
        return ret
    return None


def get_account_db():
    db = getattr(g, '_account_db', None)
    if db is None:
        redis_conf = scraper.config.get_redis_config("config.ini")
        db = g._account_db = scraper.account.AccountsManager(redis_conf)
    return db


def get_task_db():
    db = getattr(g, '_task_db', None)
    if db is None:
        redis_conf = scraper.config.get_redis_config("config.ini")
        db = g._task_db = scraper.task.ScrapeTaskDB(redis_conf)
    return db


def get_image_db():
    db = getattr(g, '_image_db', None)
    if db is None:
        db = g._image_db = scraper.datastore.FbImageDb()
    return db


def get_user_db():
    db = getattr(g, '_user_db', None)
    if db is None:
        db = g._user_db = scraper.datastore.FbUserDb()
    return db

@app.route(API_VERSION + '/tasks/todo', methods=['GET'])
def handle_tasks_get_todo():
    """
    GET API_VERSION + "/tasks/todo?page=0&limit=25"
    limit: max item per page, default is 25
    page: int page number, default is 0
    :return:
    """
    check_api_secret(request)
    limit = request.args.get("limit", default=25, type=int)
    page = request.args.get("page", default=0, type=int)

    tasks = get_task_db().list_todo_tasks(limit=limit, page=page)
    return jsonify(tasks)


@app.route(API_VERSION + '/tasks/working', methods=['GET'])
def handle_tasks_get_working():
    """
    GET API_VERSION + "/tasks/working?page=0&limit=25"
    limit: max item per page, default is 25
    page: int page number, default is 0
    :return:
    """
    check_api_secret(request)
    limit = request.args.get("limit", default=25, type=int)
    page = request.args.get("page", default=0, type=int)

    tasks = get_task_db().list_working_tasks(limit=limit, page=page)
    return jsonify(tasks)


@app.route(API_VERSION + '/tasks/failed', methods=['GET'])
def handle_tasks_get_failed():
    """
    GET API_VERSION + "/tasks/failed?page=0&limit=25"
    limit: max item per page, default is 25
    page: int page number, default is 0
    :return:
    """
    check_api_secret(request)
    limit = request.args.get("limit", default=25, type=int)
    page = request.args.get("page", default=0, type=int)

    tasks = get_task_db().list_failed_tasks(limit=limit, page=page)
    return jsonify(tasks)


@app.route(API_VERSION + '/tasks/failed_twice', methods=['GET'])
def handle_tasks_get_failed_twice():
    """
    GET API_VERSION + "/tasks/failed_twice?page=0&limit=25"
    limit: max item per page, default is 25
    page: int page number, default is 0
    :return:
    """
    check_api_secret(request)
    limit = request.args.get("limit", default=25, type=int)
    page = request.args.get("page", default=0, type=int)

    tasks = get_task_db().list_failed_twice_tasks(limit=limit, page=page)
    return jsonify(tasks)


@app.route(API_VERSION + '/tasks/<task_id>', methods=['GET'])
def handle_task_get_by_id(task_id):
    check_api_secret(request)
    task = get_task_db().get_task_by_id(task_id)
    if task is not None:
        return jsonify(task)
    else:
        abort(404)


@app.route(API_VERSION + '/tasks/<task_id>', methods=['DELETE'])
def handle_task_delete_by_id(task_id):
    check_api_secret(request)
    task = get_task_db().get_task_by_id(task_id)
    if task is None:
        abort(404)
    get_task_db().delete_task_by_id(task_id)
    ok = dict()
    ok["status"] = "ok"
    resp = Response(json.dumps(ok), status=200, mimetype='application/json')
    return resp


@app.route(API_VERSION + '/tasks/scrape_info/appuid', methods=['POST'])
def handle_scrape_info_by_appuid():
    check_api_secret(request)
    json_data = request.get_json()
    if 'appuid' not in json_data:
        abort(404)
    uid = json_data['appuid']
    if 'depth' in json_data:
        depth = json_data['depth']
    else:
        depth = 0
    task = scraper.task.make_scrape_info_app_uid_task(uid, depth=depth, emergency=True)
    get_task_db().publish_task(task)
    return jsonify(task)


@app.route(API_VERSION + '/tasks/scrape_post/appuid', methods=['POST'])
def handle_scrape_post_by_appuid():
    check_api_secret(request)
    json_data = request.get_json()
    if 'appuid' not in json_data:
        abort(404)
    uid = json_data['appuid']
    task = scraper.task.make_scrape_post_app_uid_task(uid, emergency=True)
    get_task_db().publish_task(task)
    return jsonify(task)


@app.route(API_VERSION + '/tasks/scrape_info/uid', methods=['POST'])
def handle_scrape_info_by_uid():
    check_api_secret(request)
    json_data = request.get_json()
    if 'uid' not in json_data:
        abort(404)
    uid = json_data['uid']
    if 'depth' in json_data:
        depth = json_data['depth']
    else:
        depth = 0
    if 'screen_name' in json_data:
        screen_name = json_data['screen_name']
    else:
        screen_name ='someone'
    task = scraper.task.make_scrape_info_uid_task(uid, depth=depth, emergency=True,screen_name=screen_name)
    get_task_db().publish_task(task)
    return jsonify(task)


@app.route(API_VERSION + '/tasks/scrape_post/uid', methods=['POST'])
def handle_scrape_post_by_uid():
    check_api_secret(request)
    json_data = request.get_json()
    if 'uid' not in json_data:
        abort(404)
    uid = json_data['uid']
    task = scraper.task.make_scrape_post_uid_task(uid, emergency=True)
    get_task_db().publish_task(task)
    return jsonify(task)


@app.route(API_VERSION + '/tasks/scrape_info/pathname', methods=['POST'])
def handle_scrape_info_by_pathname():
    check_api_secret(request)
    json_data = request.get_json()
    if 'pathname' not in json_data:
        abort(404)
    pathname = json_data['pathname']
    if 'depth' in json_data:
        depth = json_data['depth']
    else:
        depth = 0
    if 'screen_name' in json_data:
        screen_name = json_data['screen_name']
    task = scraper.task.make_scrape_info_pathname_task(pathname, depth=depth, emergency=True,screen_name=screen_name)
    get_task_db().publish_task(task)
    return jsonify(task)


@app.route(API_VERSION + '/tasks/scrape_post/pathname', methods=['POST'])
def handle_scrape_post_by_pathname():
    check_api_secret(request)
    json_data = request.get_json()
    if 'pathname' not in json_data:
        abort(404)
    pathname = json_data['pathname']
    task = scraper.task.make_scrape_post_pathname_task(pathname, emergency=True)
    get_task_db().publish_task(task)
    return jsonify(task)


@app.route(API_VERSION + '/accounts/all', methods=['GET'])
def handle_accounts_all_get():
    check_api_secret(request)
    accounts = get_account_db().account_get_all()
    data = []
    for account in accounts:
        a = dict()
        a["username"] = account
        a["password"] = get_account_db().account_get_password(account)
        meta = get_account_db().account_get_metadata(account)
        if meta is not None:
            a["metadata"] = json.loads(meta)
        a["twitter"] = get_account_db().account_get_twuser(account)
        a["failed"] = get_account_db().account_is_failed(account)
        a["online"] = get_account_db().account_has_heartbeat(account)
        data.append(a)
    return jsonify(data)


@app.route(API_VERSION + '/accounts/new', methods=['POST'])
def handle_accounts_new():
    check_api_secret(request)
    json_data = request.get_json()
    if 'username' not in json_data:
        abort(404)
    username = json_data['username']
    if 'password' not in json_data:
        abort(404)
    password = json_data['password']
    if 'twuser' not in json_data:
        abort(404)
    twuser = json_data['twuser']
    get_account_db().account_add_available(username, password)
    get_account_db().account_set_twuser(username, twuser)


@app.route(API_VERSION + '/accounts/online', methods=['GET'])
def handle_accounts_online_get():
    check_api_secret(request)
    accounts = get_account_db().account_get_all_online()
    data = []
    for account in accounts:
        a = dict()
        a["username"] = account
        a["password"] = get_account_db().account_get_password(account)
        meta = get_account_db().account_get_metadata(account)
        if meta is not None:
            a["metadata"] = json.loads(meta)
        a["twitter"] = get_account_db().account_get_twuser(account)
        a["failed"] = get_account_db().account_is_failed(account)
        a["online"] = get_account_db().account_has_heartbeat(account)
        data.append(a)
    return jsonify(data)


@app.route(API_VERSION + '/accounts/offline', methods=['GET'])
def handle_accounts_offline_get():
    check_api_secret(request)
    accounts = get_account_db().account_get_all_offline()
    data = []
    for account in accounts:
        a = dict()
        a["username"] = account
        a["password"] = get_account_db().account_get_password(account)
        a["twitter"] = get_account_db().account_get_twuser(account)
        a["failed"] = get_account_db().account_is_failed(account)
        a["online"] = get_account_db().account_has_heartbeat(account)
        data.append(a)
    return jsonify(data)


@app.route(API_VERSION + '/accounts/failed', methods=['GET'])
def handle_accounts_failed_get():
    check_api_secret(request)
    accounts = get_account_db().account_get_all_failed()
    data = []
    for account in accounts:
        a = dict()
        a["username"] = account
        a["password"] = get_account_db().account_get_password(account)
        a["twitter"] = get_account_db().account_get_twuser(account)
        a["failed"] = get_account_db().account_is_failed(account)
        a["online"] = get_account_db().account_has_heartbeat(account)
        data.append(a)
    return jsonify(data)


@app.route(API_VERSION + '/search/user/<username>', methods=['GET'])
def handle_search_user(username):
    check_api_secret(request)
    limit = request.args.get("limit", default=10, type=int)
    offset = request.args.get("offset", default=0, type=int)
    #data = facebook_graph_api_search_user(username, limit=limit, offset=offset)
    data = google_search_user(username,limit=limit)
    return jsonify(data)


@app.route(API_VERSION + '/graphproxy/search/user/<username>', methods=['GET'])
def handle_graph_search_user(username):
    check_api_secret(request)
    limit = request.args.get("limit", default=10, type=int)
    offset = request.args.get("offset", default=0, type=int)
    data = facebook_graph_api_search_user_proxy(username, limit=limit, offset=offset)
    return data


@app.route(API_VERSION + '/user/info/by_uid/<uid>', methods=['GET'])
def handle_user_info_uid(uid):
    check_api_secret(request)
    return jsonify(get_user_db().get_user_by_uid(int(uid)))


@app.route(API_VERSION + '/user/info/by_pathname/<pathname>', methods=['GET'])
def handle_user_info_pathname(pathname):
    check_api_secret(request)
    return jsonify(get_user_db().get_user_by_pathname(pathname))


@app.route(API_VERSION + '/user/all_ids', methods=['GET'])
def handle_user_all_ids():
    check_api_secret(request)
    return jsonify(get_user_db().get_all_ids())
