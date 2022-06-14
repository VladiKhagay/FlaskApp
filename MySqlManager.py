import datetime
import json
import pymysql
import uuid


class MySqlManager:
    connection = None

    def __init__(self, host="localhost", user="root", password="password", database="db_name"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        if self.connection is None:
            self.connection = pymysql.connect(host=self.host,
                                              user=self.user,
                                              password=self.password,
                                              database=self.database,
                                              cursorclass=pymysql.cursors.DictCursor)

    def insert_activity(self, activity):
        id = uuid.uuid4()
        if self.connection is not None:
            cursor = self.connection.cursor()
            cursor.execute(
                "INSERT INTO activity (activity_id,activity_attributes, created_timestamp, instance,invoked_by ,type) "
                "VALUES (\'{activity_id}\',\'{activity_attributes}\',\'{created_timestamp}\',\'{instance}\',"
                "\'{invoked_by}\',\'{type}\'); "
                    .format(activity_id=f'2022b.timor.bystritskie@@{id}',
                            activity_attributes=json.dumps(activity.get("activityAttributes")),
                            created_timestamp=datetime.datetime.now(),
                            instance=f'{activity.get("instance").get("instanceId").get("domain")}@@{activity.get("instance").get("instanceId").get("id")}',
                            invoked_by=f'{activity.get("invokedBy").get("userId").get("domain")}@@{activity.get("invokedBy").get("userId").get("email")}',
                            type=activity.get("type")))

            self.connection.commit()
