import datetime
import json
import uuid
import psycopg2
import psycopg2.extras


class PostgreSqlManager:
    connection = None

    def __init__(self, host='localhost', database='database', user='postgres', password='password'):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        if self.connection is None:
            self.connection = psycopg2.connect(host=self.host,
                                               database=self.database,
                                               user=self.user,
                                               password=self.password)

    def insert_instance(self, instance):
        result = None
        id = uuid.uuid4()
        if self.connection is not None:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute(
                "INSERT INTO public.instance(instance_id, active, created_by_domain, created_by_email, "
                "created_timestamp, instance_attributes, lat, lng, name, type) "
                "values(\'{instance_id}\',{active},\'{created_by_domain}\',\'{created_by_email}\',"
                "to_timestamp(\'{created_timestamp}\', \'dd-mm-yyyy hh24:mi:ss\'), "
                "\'{instance_attributes}\',{lat},{lng},\'{name}\',\'{type}\');"
                    .format(
                    instance_id=f'{instance.get("instanceId").get("domain")}@@{id}',
                    active=instance.get("active"),
                    created_by_domain=f'{instance.get("createdBy").get("userId").get("domain")}',
                    created_by_email=f'{instance.get("createdBy").get("userId").get("email")}',
                    created_timestamp=datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
                    instance_attributes=json.dumps(instance.get("instanceAttributes")),
                    lat=instance.get("location").get("lat"),
                    lng=instance.get("location").get("lng"),
                    name=instance.get("name"),
                    type=instance.get("type")))

            cursor.execute("SELECT * FROM public.instance WHERE instance.instance_id = \'{instance_id}\'".format(
                instance_id=f'{instance.get("instanceId").get("domain")}@@{id}'))

            result = cursor.fetchone()
            d = dict(instanceId=dict(domain=result.get("instance_id").split('@@')[0],
                                     id=result.get("instance_id").split('@@')[1]),
                     active=result.get("active"),
                     createdBy=dict(
                         userId=dict(domain=result.get("created_by_domain"), email=result.get("created_by_email"))),
                     createdTimestamp=str(result.get("created_timestamp")),
                     instanceAttributes=json.loads(result["instance_attributes"]),
                     location=dict(lat=result.get("lat"), lng=result.get("lng")),
                     name=result.get("name"),
                     type=result.get("type"))
            self.connection.commit()
            cursor.close()
        return d

    def update_instance(self, instance):
        result = None
        if self.connection is not None:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute("UPDATE public.instance SET "
                           "active = \'{active}\',"
                           "instance_attributes = \'{instance_attributes}\',"
                           "lat = {lat},"
                           "lng = {lng},"
                           "name = \'{name}\' "
                           "WHERE instance_id = \'{instance_id}\';"
                .format(
                instance_id=f'{instance.get("instanceId").get("domain")}@@{instance.get("instanceId").get("id")}',
                active=instance.get("active"),
                instance_attributes=json.dumps(instance.get("instanceAttributes")),
                lat=instance.get("location").get("lat"),
                lng=instance.get("location").get("lng"),
                name=instance.get("name"),
                type=instance.get("type")))

            cursor.execute("SELECT * FROM public.instance WHERE instance.instance_id = \'{instance_id}\'".format(
                instance_id=f'{instance.get("instanceId").get("domain")}@@{instance.get("instanceId").get("id")}'))

            result = cursor.fetchone()
            d = dict(instanceId=dict(domain=result.get("instance_id").split('@@')[0],
                                     id=result.get("instance_id").split('@@')[1]),
                     active=result.get("active"),
                     createdBy=dict(
                         userId=dict(domain=result.get("created_by_domain"), email=result.get("created_by_email"))),
                     createdTimestamp=str(result.get("created_timestamp")),
                     instanceAttributes=json.loads(result["instance_attributes"]),
                     location=dict(lat=result.get("lat"), lng=result.get("lng")),
                     name=result.get("name"),
                     type=result.get("type"))

            self.connection.commit()
            cursor.close()
        return d

    def select_instances_by_type(self, type, user_domain, user_email):
        result = None
        if self.connection is not None:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

            cursor.execute("SELECT * FROM public.instance WHERE type = '{type}' and created_by_domain= '{domain}' and "
                           "created_by_email = '{email}' ORDER BY instance_id ASC"
                           .format(type=type, domain=user_domain, email=user_email))
            result = [dict(instanceId=dict(domain=row.get("instance_id").split('@@')[0],
                                           id=row.get("instance_id").split('@@')[1]),
                           active=row["active"],
                           createdBy=dict(
                               userId=dict(domain=row.get("created_by_domain"), email=row.get("created_by_email"))),
                           createdTimestamp=str(row.get("created_timestamp")),
                           instanceAttributes=json.loads(row["instance_attributes"]),
                           location=dict(lat=row.get("lat"), lng=row.get("lng")),
                           name=row["name"],
                           type=row["type"]) for row in cursor.fetchall()]
            cursor.close()
        return result

    def create_user(self, user_boundary_json):
        result = None
        if self.connection is not None:
            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute("SELECT * FROM public.instance WHERE instance.type = \'{type}\' ORDER BY instance_id ASC"
                           .format(type=type))

