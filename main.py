import base64
import os
from datetime import date

from fastapi import FastAPI
import pyodbc
from pydantic import BaseModel

data = (
    "Driver={SQL Server};"
    "Server=TERMINAL\CDS;"
    "Database=Teste;"
)

connection = pyodbc.connect(data)
print("Successfully connected")

cursor = connection.cursor()

storage_folder = 'C:\\Registro_Organizador'

table = 'Entradas'
edit_image_path = ''

class Query(BaseModel):
    query: str

class Entry(BaseModel):
    budget: str
    client: str
    equip: str
    fab: str
    id: str
    images: list
    has_images: bool
    arrive: str
    departure: str

class EditEntry(BaseModel):
    budget: str
    client: str
    equip: str
    fab: str
    id: str
    images: list
    has_images: bool
    arrive: str
    departure: str
    previous: dict

class EntrySearch(BaseModel):
    client: str = ''
    equip: str = ''
    fab: str = ''
    id: str = ''
    arrive: list = ''
    departure: list = ''
class ImageData(BaseModel):
    name: str
    content: str

app = FastAPI()

@app.get("/get_query/")
async def get_query(query: str):
    return cursor.execute(query).fetchall()


@app.get("/insert_query/")
async def insert_query(query: str):
    cursor.execute(query)
    cursor.commit()
    return


@app.get("/edit_page_data/")
async def fetch_edit_page_data(rClient, rType, rArrive, rID):
    wdata = []
    wdata += [[item[0] for item in cursor.execute(f"SELECT DISTINCT cliente FROM {table}").fetchall()]]
    wdata += [[item[0] for item in cursor.execute(f"SELECT DISTINCT fabricante FROM {table}").fetchall()]]
    wdata += [[item[0] for item in cursor.execute(f"SELECT DISTINCT tipo FROM {table}").fetchall()]]


@app.get("/read_page_data/")
async def fetch_read_page_data(rClient, rType, rArrive, rID):
    return cursor.execute(f"""SELECT orcamento, imagens From {table} WHERE id = '{rID}' AND data_entrada = '{rArrive}'
                            AND cliente = '{rClient}' AND tipo = '{rType}'""").fetchall()


@app.get("/write_page_data/")
async def fetch_write_page_data():
    wdata = []
    wdata += [[item[0] for item in cursor.execute(f"SELECT DISTINCT cliente FROM {table}").fetchall()]]
    wdata += [[item[0] for item in cursor.execute(f"SELECT DISTINCT fabricante FROM {table}").fetchall()]]
    wdata += [[item[0] for item in cursor.execute(f"SELECT DISTINCT tipo FROM {table}").fetchall()]]
    return wdata


@app.post("/search_result/")
async def fetch_search_result(data: EntrySearch):
    if data.arrive[0] == '' and data.arrive[1] == '' and data.arrive[2] == '':
        data.arrive = ''

    if data.departure[0] == '' and data.departure[1] == '' and data.departure[2] == '':
        data.departure = ''

    infos = [('cliente', data.client), ('tipo', data.equip), ('fabricante', data.fab), ('id', data.id),
             ('data_entrada', data.arrive), ('data_saida', data.departure)]

    query = f"SELECT * From {table} "

    start = False
    for info in infos:
        if info[1] != '' and info[1] != 'desconhecido':

            empty = False
            if type(info[1]) is tuple:
                empty = True
                for i in info[1]:
                    print(i)

                    if i.strip() != '' and i.strip() != 'desconhecido':
                        empty = False

            if not start and not empty:
                start = True
                query += "WHERE "
                print(info)
            elif not empty:
                query += "AND "
                print(info)

            if 'data' in info[0]:

                year = False
                month = False

                if info[1][0].strip() != '' and info[1][0].strip() != 'desconhecido':
                    query += f"YEAR({info[0]}) = {info[1][0]} "
                    year = True

                if info[1][1].strip() != '' and info[1][1].strip() != 'desconhecido':
                    if year:
                        query += "AND "
                    query += f"MONTH({info[0]}) = {info[1][1]} "
                    month = True

                if info[1][2].strip() != '' and info[1][2].strip() != 'desconhecido':
                    if month or year:
                        query += "AND "
                    query += f"DAY({info[0]}) = {info[1][2]} "

                continue

            query += f"{info[0]} LIKE '{info[1]}' "

    print(query)
    sdata = cursor.execute(query).fetchall()
    result = []
    column_names = [column[0] for column in cursor.description]
    for row in sdata:
        result.append(dict(zip(column_names, row)))

    return result


@app.get("/fetch_file_names/")
async def fetch_file_names(path):
    print(path)
    return os.listdir(path)


@app.get("/fetch_image/")
async def fetch_image(path, file_name):
    img_file = open(os.path.join(path, file_name), 'rb')
    base_64 = base64.b64encode(img_file.read())
    print(file_name)
    #base64.b64encode(img_file.read()
    return base_64



@app.get("/search_page_data/")
async def fetch_search_page_data():
    sdata = []
    sdata += [['desconhecido']+[item[0] for item in cursor.execute(f"SELECT DISTINCT cliente FROM {table}").fetchall()]]
    sdata += [['desconhecido']+[item[0] for item in cursor.execute(f"SELECT DISTINCT fabricante FROM {table}").fetchall()]]
    sdata += [['desconhecido']+[item[0] for item in cursor.execute(f"SELECT DISTINCT tipo FROM {table}").fetchall()]]
    sdata += [['desconhecido']+[item[0] for item in cursor.execute(f"SELECT DISTINCT YEAR(data_entrada) FROM {table}").fetchall()]]
    sdata += [['desconhecido']+[item[0] for item in cursor.execute(f"SELECT DISTINCT YEAR(data_saida) FROM {table}").fetchall()]]
    return sdata

write_data = {}
@app.post("/write_entry/")
async def write_entry(data: Entry):
    print(data)
    global write_data

    if not data.has_images:
        query = f"""
                    INSERT INTO {table} (cliente, tipo, fabricante, id, orcamento, imagens, data_entrada, data_saida)
                    VALUES ('{data.client.capitalize()}', '{data.equip.capitalize()}', '{data.fab.capitalize()}',
                    '{data.id}', '{data.budget}', 'None', {data.arrive},
                    {data.departure}) 
                """

        print(query)
        cursor.execute(query)
        cursor.commit()
        print("submetido")
        return


    write_data = data

@app.post("/write_entry/image")
async def write_entry_image(info: ImageData):
    print(info)
    global write_data
    write_data.images.append({'name': info.name, 'content': info.content})

@app.post("/write_entry/stop")
async def write_entry_stop():
    global write_data
    print(write_data)

    images_path = None

    if len(write_data.images):
        created = False
        while not created:
            if os.path.isdir(storage_folder):
                year_folder = os.path.join(storage_folder, date.today().strftime('%Y'))

                if os.path.isdir(year_folder):
                    client_folder = os.path.join(year_folder, write_data.client.capitalize())

                    if os.path.isdir(client_folder):
                        equipment_folder = os.path.join(client_folder, write_data.id)

                        if os.path.isdir(equipment_folder):
                            date_folder = os.path.join(equipment_folder, date.today().strftime('%Y-%m-%d'))

                            if os.path.isdir(date_folder):
                                images_path = date_folder
                                created = True

                            else:
                                os.mkdir(date_folder)

                        else:
                            os.mkdir(equipment_folder)

                    else:
                        os.mkdir(client_folder)

                else:
                    os.mkdir(year_folder)

            else:
                os.mkdir(storage_folder)

    print(images_path)

    for img in write_data.images:
        decoded_data = base64.b64decode((img['content']))
        img_file = open(f"{images_path}\\{img['name']}", 'wb')
        img_file.write(decoded_data)
        img_file.close()

    query = f"""
            INSERT INTO {table} (cliente, tipo, fabricante, id, orcamento, imagens, data_entrada, data_saida)
            VALUES ('{write_data.client.capitalize()}', '{write_data.equip.capitalize()}', '{write_data.fab.capitalize()}',
            '{write_data.id}', '{write_data.budget}', '{images_path}', {write_data.arrive},
            {write_data.departure}) 
        """

    print(query)

    cursor.execute(query)
    cursor.commit()
    print("submetido")


@app.post("/edit_entry/")
async def edit_entry(data: EditEntry):
    global edit_image_path
    print(data)
    query = f"""UPDATE {table}
                SET cliente = '{data.client.capitalize()}',
                tipo = '{data.equip.capitalize()}',
                fabricante = '{data.fab.capitalize()}',
                id = '{data.id}',
                orcamento = '{data.budget}',
                data_entrada = {data.arrive},
                data_saida = {data.departure}
                WHERE id = '{data.previous["id"]}' AND cliente = '{data.previous["cliente"]}' 
                AND data_entrada = '{data.previous["data_entrada"]}'
            """
    print(query)
    cursor.execute(query)
    cursor.commit()

    edit_image_path = ''

    if data.has_images:
        edit_image_path = data.previous['imagens']



@app.post("/edit_entry/image")
async def edit_entry_image(info: ImageData):
    global edit_image_path
    print(info)
    print(edit_image_path)
    if edit_image_path != '':
        decoded_data = base64.b64decode((info.content))
        img_file = open(f"{edit_image_path}\\{info.name}", 'wb')
        img_file.write(decoded_data)
        img_file.close()

    edit_image_path = ''