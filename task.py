import datetime
import uuid
import os
import redis
from google.cloud import datastore

TASK_WORK_SCRAPE_INFO_UID = "scrape:info:uid"
TASK_WORK_SCRAPE_INFO_PATHNAME = "scrape:info:pathname"
TASK_WORK_SCRAPE_INFO_APPUID = "scrape:info:appuid"

TASK_WORK_SCRAPE_POST_UID = "scrape:post:uid"
TASK_WORK_SCRAPE_POST_PATHNAME = "scrape:post:pathname"
TASK_WORK_SCRAPE_POST_APPUID = "scrape:post:appuid"

TASK_PROPERTIES = ["uid", "pathname", "work", "depth", "task_id", "emergency", "created_at", "started_at", "done", "finished_at", "appuid","screen_name"]


class ScrapeTaskDB:
    client = None
    kind = 'scrape_tasks'
    tasks_todo = "scrape_tasks"
    tasks_working = "scrape_tasks_working"
    tasks_failed = "scrape_tasks_failed"
    tasks_failed_twice = "scrape_tasks_failed_twice"
    redis = None
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/bunny/Desktop/FBProject/FB1025/scraper_modify/credentials/key.json"


    def __init__(self, redisconfig):
        '''
        self.redis = redis.StrictRedis(host=redisconfig['host'],
                                       port=int(redisconfig['port']), db=0,
                                       password=redisconfig['password'])
        '''
        self.redis = redis.StrictRedis(host='35.234.14.76',port=6379,password='tfb123456.com')


        self.client = datastore.Client()

    def publish_task(self, task):
        task_id = task['task_id']
        task_key = self.client.key(self.kind, task_id)
        task_entity = datastore.Entity(key=task_key)
        for p in TASK_PROPERTIES:
            if p in task:
                task_entity[p] = task[p]
        self.client.put(task_entity)
        if task['emergency'] is True:
            self.redis.rpush(self.tasks_todo, task_id)
        else:
            self.redis.lpush(self.tasks_todo, task_id)

    def update_task(self, task):
        task_id = task['task_id']
        task_key = self.client.key(self.kind, task_id)
        task_entity = datastore.Entity(key=task_key)
        for p in TASK_PROPERTIES:
            if p in task:
                task_entity[p] = task[p]
        self.client.put(task_entity)

    def delete_task_by_id(self, task_id):
        self.redis.lrem(self.tasks_todo, 0, task_id)
        self.redis.lrem(self.tasks_working, 0, task_id)
        self.redis.lrem(self.tasks_failed, 0, task_id)
        self.redis.lrem(self.tasks_failed_twice, 0, task_id)
        task_key = self.client.key(self.kind, task_id)
        self.client.delete(task_key)

    def delete_task(self, task):
        task_id = task['task_id']
        self.redis.lrem(self.tasks_todo, 0, task_id)
        self.redis.lrem(self.tasks_working, 0, task_id)
        self.redis.lrem(self.tasks_failed, 0, task_id)
        self.redis.lrem(self.tasks_failed_twice, 0, task_id)
        task_key = self.client.key(self.kind, task_id)
        self.client.delete(task_key)

    def get_task_by_id(self, task_id):
        task_key = self.client.key(self.kind, task_id)
        entity = self.client.get(task_key)
        if entity is not None:
            task = dict()
            for p in TASK_PROPERTIES:
                if p in entity:
                    task[p] = entity[p]
            return task
        else:
            return None

    def fetch_one_todo_task(self):
        id = self.redis.rpop(self.tasks_todo)
        if id is not None:
            task_id = id.decode()
            task = self.get_task_by_id(task_id)
            return task
        return None

    def fetch_one_failed_task(self):
        id = self.redis.rpop(self.tasks_failed)
        if id is not None:
            task_id = id.decode()
            task = self.get_task_by_id(task_id)
            return task
        return None

    def set_task_working(self, task):
        task_key = self.client.key(self.kind, task["task_id"])
        entity = self.client.get(task_key)
        entity['done'] = False
        entity["started_at"] = datetime.datetime.utcnow()
        self.client.put(entity)
        self.redis.lpush(self.tasks_working, task["task_id"])

    def set_task_done(self, task):
        task_key = self.client.key(self.kind, task["task_id"])
        entity = self.client.get(task_key)
        entity['done'] = True
        entity['finished_at'] = datetime.datetime.utcnow()
        self.client.put(entity)
        self.redis.lrem(self.tasks_working, 0,  task["task_id"])
        self.redis.lrem(self.tasks_failed, 0, task["task_id"])
        self.redis.lrem(self.tasks_failed_twice, 0, task["task_id"])

    def set_task_failed(self, task):
        task_id = task['task_id']
        p = self.redis.pipeline()
        p.lrem(self.tasks_working, 0, task_id)
        p.lpush(self.tasks_failed, task_id)
        p.execute()

    def set_task_failed_twice(self, task):
        task_id = task['task_id']
        p = self.redis.pipeline()
        p.lrem(self.tasks_working, 0, task_id)
        p.lrem(self.tasks_failed, 0, task_id)
        p.lpush(self.tasks_failed_twice, task_id)
        p.execute()

    def _get_tasks_from_list(self, listkey, limit=25, page=0):
        tasks = dict()
        tasks["limit"] = limit
        tasks["page"] = page
        count = self.redis.llen(listkey)
        tasks["count"] = count
        tasks["tasks"] = []
        if count == 0:
            return tasks
        start = page * limit
        end = page * limit + limit - 1
        task_ids = self.redis.lrange(listkey, start, end)
        for id in task_ids:
            task_id = id.decode()
            task = self.get_task_by_id(task_id)
            if task is not None:
                tasks["tasks"].append(task)
            else:
                self.redis.lrem(listkey, 0, task_id)
        return tasks

    def list_working_tasks(self, limit=25, page=0):
        tasks = self._get_tasks_from_list(self.tasks_working, limit, page)
        return tasks

    def list_failed_tasks(self, limit=25, page=0):
        tasks = self._get_tasks_from_list(self.tasks_failed, limit, page)
        return tasks

    def list_failed_twice_tasks(self, limit=25, page=0):
        tasks = self._get_tasks_from_list(self.tasks_failed_twice, limit, page)
        return tasks

    def list_todo_tasks(self, limit=25, page=0):
        tasks = self._get_tasks_from_list(self.tasks_todo, limit, page)
        return tasks

    def get_todo_tasks_count(self):
        count = self.redis.llen(self.tasks_todo)
        return count

    def clean_all_todo_tasks(self):
        self.redis.delete(self.tasks_todo)

    def clean_all_failed_tasks(self):
        self.redis.delete(self.tasks_failed)

    def clean_all_working_tasks(self):
        self.redis.delete(self.tasks_working)


def _make_scrape_uid_task(uid, work, depth=0, emergency=False,screen_name='someone'):
    task = dict()
    task["task_id"] = str(uuid.uuid4())
    if type(uid) is str:
        uid = int(uid)
    task["uid"] = uid
    task["depth"] = depth
    task["work"] = work
    task["created_at"] = datetime.datetime.utcnow()
    task["done"] = False
    task["screen_name"] = screen_name
    if emergency is True:
        task["emergency"] = True
    elif depth > 0:
        task["emergency"] = True
    else:
        task["emergency"] = False
    return task


def make_scrape_info_uid_task(uid, depth=0, emergency=False,screen_name='someone'):
    """
    make a scrape info by uid task
    :param uid: facebook uid
    :param depth: scrape task uid
    :param emergency: will add to todo list top if True
    :return: task dict to be published
    """
    return _make_scrape_uid_task(uid, TASK_WORK_SCRAPE_INFO_UID, depth=depth, emergency=emergency,screen_name=screen_name)


def make_scrape_post_uid_task(uid, emergency=False):
    """
    make a scrape info by uid task
    :param uid: facebook uid
    :param depth: scrape task uid
    :param emergency: will add to todo list top if True
    :return: task dict to be published
    """
    return _make_scrape_uid_task(uid, TASK_WORK_SCRAPE_POST_UID, emergency=emergency)


def _make_scrape_pathname_task(pathname, work, depth=0, emergency=False,screen_name='someone'):
    task = dict()
    task["task_id"] = str(uuid.uuid4())
    task["pathname"] = pathname
    task["depth"] = depth
    task["work"] = work
    task["created_at"] = datetime.datetime.utcnow()
    task["done"] = False
    task["screen_name"] = screen_name
    if emergency is True:
        task["emergency"] = True
    elif depth > 0:
        task["emergency"] = True
    else:
        task["emergency"] = False
    return task


def make_scrape_info_pathname_task(pathname, depth=0, emergency=False,screen_name='someone'):
    """
    make a scrape info by pathname task
    :param pathname: facebook user pathname
    :param depth: scrape task depth
    :param emergency: will add to todo list top if True
    :return: task dict to be published
    """
    return _make_scrape_pathname_task(pathname, TASK_WORK_SCRAPE_INFO_PATHNAME, depth=depth, emergency=emergency,screen_name='someone')


def make_scrape_post_pathname_task(pathname,  emergency=False):
    """
    make a scrape info by pathname task
    :param pathname: facebook user pathname
    :param emergency: will add to todo list top if True
    :return: task dict to be published
    """
    return _make_scrape_pathname_task(pathname, TASK_WORK_SCRAPE_POST_PATHNAME, emergency=emergency)


def _make_scrape_app_uid_task(appuid, work, depth=0, emergency=False):
    task = dict()
    task["task_id"] = str(uuid.uuid4())
    if type(appuid) is str:
        appuid = int(appuid)
    task["appuid"] = appuid
    task["depth"] = depth
    task["work"] = work
    task["created_at"] = datetime.datetime.utcnow()
    task["done"] = False
    if emergency is True:
        task["emergency"] = True
    elif depth > 0:
        task["emergency"] = True
    else:
        task["emergency"] = False
    return task


def make_scrape_info_app_uid_task(appuid, depth=0, emergency=False):
    """
    make a scrape info by pathname task
    :param app: facebook app scoped user id from search api
    :param depth: scrape task depth
    :param emergency: will add to todo list top if True
    :return: task dict to be published
    """
    return _make_scrape_app_uid_task(appuid, TASK_WORK_SCRAPE_INFO_APPUID, depth=depth, emergency=emergency)


def make_scrape_post_app_uid_task(appuid, emergency=False):
    """
    make a scrape info by pathname task
    :param app: facebook app scoped user id from search api
    :param depth: scrape task depth
    :param emergency: will add to todo list top if True
    :return: task dict to be published
    """
    return _make_scrape_app_uid_task(appuid, TASK_WORK_SCRAPE_POST_APPUID, emergency=emergency)
