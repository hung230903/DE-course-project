import psycopg2
from config import load_config

def write_blob(part_id, path_to_file, file_extension):
    """ Insert a BLOB into a table """
    # read database configuration
    params = load_config()

    # read data from a picture
    data = open(path_to_file, 'rb').read()


    try:
        # connect to the PostgresQL database
        with psycopg2.connect(**params) as conn:
            # create a new cursor object
            with  conn.cursor() as cur:
                # execute the INSERT statement
                cur.execute("INSERT INTO part_drawings(part_id,file_extension,drawing_data) " +
                            "VALUES(%s,%s,%s)",
                            (part_id, file_extension, psycopg2.Binary(data)))

            conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def read_blob(part_id, path_to_dir):
    """ Read BLOB data from a table """
    # read database configuration
    config = load_config()
    try:
        # connect to the PostgresQL database
        with  psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                # execute the SELECT statement
                cur.execute(""" SELECT part_name, file_extension, drawing_data
                                FROM part_drawings
                                INNER JOIN parts on parts.part_id = part_drawings.part_id
                                WHERE parts.part_id = %s """,
                            (part_id,))
                blob = cur.fetchone()
                # write blob data into file
                open(path_to_dir + blob[0] + '.' + blob[1], 'wb').write(blob[2])
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


if __name__ == '__main__':
    write_blob(1, 'images/cartoon-style-summer-scene-with-cute-animal.jpg', 'png')
    write_blob(2, 'images/japan-background-digital-art.jpg', 'png')

    read_blob(1, 'images/output/')
    read_blob(2, 'images/output/')