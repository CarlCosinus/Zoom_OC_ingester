import pika
import json
import requests
import wget
import os
from requests.auth import HTTPDigestAuth
import sys
import configparser
import xmltodict
import logging
import time

url = ""
user = ""
password = ""
rabbit_url = ""
rabbit_user = ""
rabbit_password = ""
save_local = False


def rcv_rabbit_callback(method, properties, body):
    logging.info("Received Rabbit msg")
    data = json.loads(body.decode("utf-8"))
    if "recording_files" not in data:
        logging.error("No recording found")
    files = data["recording_files"]
    dl_url = ''
    id = ''
    for key in files[0].keys():
        if key == "download_url":
            dl_url = files[0][key]
        elif key == "recording_id":
            id = files[0][key]
    try:

        recording = wget.download(dl_url + '/?access_token=' + data["token"], id + '.mp4')

    except Exception as e:
        logging.error("Could not download file {}".format(e))

    logging.info("downloaded zoom recording - id {}".format(id))
    oc_upload(data["creator"], data["topic"], id, recording)


def oc_upload(creator, title, rec_id, recording_file):
    logging.info("Title =====>   " + title + "   creator =========>  " + creator)

    response = requests.get(url + '/admin-ng/series/series.json', auth=HTTPDigestAuth(user, password),
                            headers={'X-Requested-Auth': 'Digest'})

    series_list = json.loads(response.content.decode("utf-8"))
    try:
        response = requests.get(url + '/users/' + creator + '.json', auth=HTTPDigestAuth(user, password),
                                headers={'X-Requested-Auth': 'Digest'})
        data = response.json()
        username = data['user']['name']
    except ValueError:
        logging.error("Invalid shib_username: '@' is missing, default username is used")
        creator = "Others"
        username = "Others"
    series_title = "Zoom Recordings " + username
    series_found = False
    for series in series_list["results"]:
        if series["title"] == series_title:
            series_found = True
            id = series["id"]
            logging.info("Series with ID " + id + " and Title " + series_title + " has been found")

    if not series_found:
        id = create_series(creator, series_title, username)

    logging.info("Uploading file " + recording_file + ".mp4 to Opencast")
    try:

        with open(recording_file, 'rb') as fobj:
            data = {"title": title, "creator": username, "isPartOf": id, "flavor": 'presentation/source'}
            body = {'body': fobj}
            response = requests.post(url + '/ingest/addMediaPackage', data=data, files=body,
                                     auth=HTTPDigestAuth(user, password),
                                     headers={'X-Requested-Auth': 'Digest'})
            if response.status_code != 200:
                logging.error("OC Upload failed \n" + response.content)
            else:
                resp_dict = xmltodict.parse(response.content)
                mp_id = resp_dict['wf:workflow']['mp:mediapackage']['@id']
                logging.info("Zoom recording ID : " + rec_id + " has been uploaded to OC with MP-ID : " + mp_id)
    except IOError:
        logging.error("Zoom recording with ID " + rec_id + '.mp4 has not been found')
    if save_local:
        logging.info("Renaming " + recording_file)
        try:
            os.rename(recording_file, rec_id + "_" + mp_id + ".mp4")
        except OSError as err:
            logging.error("Exception while renaming " + id + ".mp4\n{}".format(err))

    else:
        logging.info("Removing " + recording_file)
        try:
            os.remove(recording_file)
        except OSError as err:
            logging.error("Exception while removing " + recording_file + "\n{}".format(err))


def start_consuming_rabbitmsg():
    logging.info("Start consuming")
    credentials = pika.PlainCredentials(rabbit_user, rabbit_password)
    rcv_connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_url, credentials=credentials))
    rcv_channel = rcv_connection.channel()
    queue = rcv_channel.queue_declare(queue="zoomhook")
    msg_count = queue.method.message_count
    logging.info("Received [%s] messages", msg_count)
    while msg_count > 0:
        method, prop, body = rcv_channel.basic_get(queue="zoomhook", auto_ack=True)
        rcv_rabbit_callback(method, prop, body)
        count_queue = rcv_channel.queue_declare(queue="zoomhook", passive=True)
        msg_count = count_queue.method.message_count
    rcv_channel.close()
    rcv_connection.close()


def create_series(shib_name, title, creator):
    logging.info("creating series")
    metadata = [{"label": "Opencast Series DublinCore",
                 "flavor": "dublincore/series",
                 "fields": [{"id": "title",
                             "value": title},
                            {"id": "creator",
                             "value": [creator]}]}]

    acl = [{"allow": True,
            "action": "write",
            "role": "ROLE_USER_" + shib_name.upper().replace('@', '_').replace('-', '_').replace('.', '_')},
           {"allow": True,
            "action": "read",
            "role": "ROLE_USER_" + shib_name.upper().replace('@', '_').replace('-', '_').replace('.', '_')},
           {"allow": True,
            "action": "write",
            "role": "ROLE_GROUP_AAI_MANAGER"},
           {"allow": True,
            "action": "read",
            "role": "ROLE_GROUP_AAI_MANAGER"},
           {"allow": True,
            "action": "write",
            "role": "ROLE_ADMIN"},
           {"allow": True,
            "action": "read",
            "role": "ROLE_ADMIN"}
           ]

    data = {"metadata": json.dumps(metadata),
            "acl": json.dumps(acl)}

    response = requests.post(url + '/api/series', data=data, auth=HTTPDigestAuth(user, password),
                             headers={'X-Requested-Auth': 'Digest'})
    id = json.loads(response.content.decode("utf-8"))["identifier"]
    if response.status_code != 201:
        logging.error("Something went wrong while creating series: Error Code: " + response.status_code)
    else:
        logging.info("Series with id " + id + " with Title : " + title + " has been uploaded")

    return id


if __name__ == '__main__':

    logging.basicConfig(filename='oc_uploader_error.log', level=logging.INFO, format='%(asctime)s %(message)s',
                        datefmt='%d/%m/%Y %I:%M:%S %p')
    try:
        config = configparser.ConfigParser()
        config.read('settings.ini')
        logging.info("Found Settings")
    except FileNotFoundError:
        logging.error("No Settings found")
        sys.exit("No settings found")

    try:
        url = config["Opencast"]["Url"]
        user = config["Opencast"]["User"]
        password = config["Opencast"]["Password"]
        save_local = config.getboolean("Opencast", "Save_records_local")
        rabbit_url = config["Rabbit"]["Url"]
        rabbit_user = config["Rabbit"]["User"]
        rabbit_password = config["Rabbit"]["Password"]
        logging.info("Settings are set")
    except KeyError as err:
        logging.error("Key {0} was not found".format(err))
        sys.exit("Key {0} was not found".format(err))

    start_consuming_rabbitmsg()
