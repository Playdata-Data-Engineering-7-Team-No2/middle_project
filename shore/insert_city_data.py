import pymysql
import logging
import configparser

# init configuration
config = configparser.ConfigParser()
config.read("./config/config.ini")

def main():

    try:
        conn_db = pymysql.connect(host="localhost", port=3306, user=config["DB"]["USERNAME"], passwd=config["DB"]["PASSWORD"], db="weather", charset="utf8")
        cursor = conn_db.cursor()
    except Exception as ex:
        logging.error("DB Connection Issue {}".format(ex))

    try:
        with open("./jeju.txt", "r", encoding="utf8") as f:
            lines = f.readlines()
            for line in lines:
                line = line.strip("\n")
                name, lat, lon = line.split(",")
                sql = "INSERT INTO location VALUES('{}', {}, {})".format(name, float(lat), float(lon))
                try:
                    cursor.execute(sql)
                except pymysql.err.IntegrityError: # name이 중복이라면 무시
                    continue
        conn_db.commit()
    except Exception as ex:
        logging.warning("Insert Data Issue {}".format(ex))

    cursor.close()
    conn_db.close()

if __name__ == "__main__":

    main()
