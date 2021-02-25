# -*- coding:UTF-8 -*-

import psycopg2
import psycopg2.extras

class ADataBase():
    def __init__(self):
        self.dbconn = None;
        self.db = None;
        self.errorinfo = '';
        pass;

    def connect_db(self, dbconfig={}):
        try:
            host = CONFIG["DB_HOST"] if "host" not in dbconfig else dbconfig["host"]
            port = CONFIG["DB_PORT"] if "port" not in dbconfig else dbconfig["port"]
            database = CONFIG["DB_NAME"] if "database" not in dbconfig else dbconfig["database"]
            user = CONFIG["DB_USER"] if "user" not in dbconfig else dbconfig["user"]
            password = CONFIG["DB_PWD"] if "password" not in dbconfig else dbconfig["password"]
            self.dbconn = psycopg2.connect(host=host,
                                           port=port,
                                           database=database,
                                           user=user,
                                           password=password
                                           )
            ############################# func
            #             self.dbconn.create_function("gen_pass",2,lambda p,t : hashlib.md5("%s%s"%(p,t%612)).hexdigest());

            # get db
            self.db = self.dbconn.cursor(cursor_factory=psycopg2.extras.DictCursor);
            return True;
        except Exception as e:
            self.errorinfo = 'Failed to connect db : ' + str(e);
            # LOG.error(self.errorinfo)
            return False;

    def insert_db(self, tablename, fieldlist, value_list, field_str=''):
        try:
            # check
            if None == self.db:
                self.errorinfo = 'Failed to insert into db, please connect db first';
                return (False, 0);
            # date
            insert_field_str = ''
            insert_value_str = ''
            if len(fieldlist) > 0:
                for field in fieldlist:
                    insert_field_str += "%s," % field
                    insert_value_str += "%(" + field + ")s,"
                insert_field_str = insert_field_str[:-1]
                insert_value_str = insert_value_str[:-1]

            else:
                return False, "field_list is []!!!"
            sql = 'insert into %s (%s) values (%s)' % (tablename, insert_field_str, insert_value_str);
            # print sql,data
            param_dict = {}
            for i, field in enumerate(fieldlist):
                param_dict[field] = value_list[field]
            # insert
            if field_str != '':
                sql += 'RETURNING ' + field_str
            if tablename.lower() == 'iep_client' or tablename.lower() == 'iep_group' or tablename.lower() == "iep_company":
                rf = self.refresh_clients()
                if not rf:
                    return False, 0
            self.db.execute(sql, param_dict)
            return (True, self.db.fetchone()[0] if field_str != '' else 1);

        except Exception as e:
            self.errorinfo = 'Failed to insert into db : %s : %s' % (str(e), sql);
            print(self.errorinfo)
            self.dbconn.rollback()
            return (False, self.errorinfo);

    def update_db(self, tablename, flushdata, wherelist):
        # check db
        if None == self.db:
            self.errorinfo = 'Failed to update db, please connect db first';
            print(self.errorinfo)
            return False;
        # data
        sql = 'update %s set %s where %s' % (tablename, \
                                             ','.join([' %s=' % item + '%s' for item in flushdata]), \
                                             ' and '.join(['%s' % item + '=%s' for item in wherelist]));
        data = []
        for i in flushdata:
            data.append(flushdata[i])
        for i in wherelist:
            data.append(wherelist[i])
        # update
        try:
            if not self.run_sql(sql, data):
                return False;
            return True;
        except Exception as e:
            self.errorinfo = 'Failed to update db : ' + str(e);
            print(self.errorinfo)
            self.dbconn.rollback()
            return False;

    def get_table_flag(self, basesql, params=None):
        try:
            # check
            if None == self.db:
                self.errorinfo = 'Failed to get_table from db, please connect db first';
                return (False, None);
            # request
            if None == params:
                self.db.execute(basesql);
            else:
                self.db.execute(basesql, params);
            return (True, self.db.fetchall());
        except Exception as e:
            self.errorinfo = 'Failed to get data : %s : %s' % (str(e), basesql);
            self.dbconn.rollback()
            return (False, None);

    def get_table(self, basesql, params=None):
        try:
            # check
            if None == self.db:
                self.errorinfo = 'Failed to get_table from db, please connect db first';
                return False;
            # request
            if None == params:
                self.db.execute(basesql);
            else:
                self.db.execute(basesql, params);
            return self.db.fetchall();
        except Exception as e:
            self.errorinfo = 'Failed to get data : %s : %s' % (str(e), basesql);
            return False;

    def run_sql(self, basesql, params=None, flagarray=False):
        try:
            # check
            if None == self.db:
                self.errorinfo = 'Failed to execute sql, please connect db first';
                return False;
            # request
            if None == params:
                self.db.execute(basesql);
                # LOG.debug(">>>>>>sql>>>>>>: %s" % (self.db.mogrify(basesql)))
            else:
                if flagarray:
                    self.db.executemany(basesql, params);
                else:
                    self.db.execute(basesql, params);
                # LOG.debug(">>>>>>sql>>>>>>: %s" % (self.db.mogrify(basesql, params)))
            if (
                    'iep_client ' in basesql.lower() or 'iep_group ' in basesql.lower() or "iep_company " in basesql.lower()) and (
                    "insert" in basesql.lower() or "update" in basesql.lower()):
                rf = self.refresh_clients()
                if not rf:
                    return False
            return True;
        except Exception as e:
            self.errorinfo = 'Failed to runsql : %s : %s' % (str(e), basesql);
            print(self.errorinfo)
            self.dbconn.rollback()
            return False;

    def commit(self):
        try:
            self.dbconn.commit();
            return True;
        except Exception as e:
            self.errorinfo = 'Failed to commit into db : ' + str(e);
            if 'database is locked' in str(e).lower():
                self.errorinfo = u"数据库繁忙，请稍后重试"
            self.dbconn.rollback()
            return False;

    def close(self):
        self.dbconn.close();
        return True;

    def get_last_error(self):
        return self.errorinfo;