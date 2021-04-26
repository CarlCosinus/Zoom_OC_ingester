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
from requests_toolbelt.multipart import encoder
import os.path

url = ""
user = ""
password = ""
rabbit_url = ""
rabbit_user = ""
rabbit_password = ""
save_local = False
rcv_connection = None
rcv_queue = None

VIEW_PRIORITIES = {
    # if we have this...
    "active_speaker": [
        # then take these in this order...
        "shared_screen",
        "gallery_view",
        "shared_screen_with_gallery_view",
        "shared_screen_with_speaker_view"
    ],
    "shared_screen_with_speaker_view": [
        "shared_screen",
        "shared_screen_with_gallery_view",
        "gallery_view",
    ]
}

FALLBACK_PRIORITIES = [
    "shared_screen",
    "shared_screen_with_gallery_view",
    "gallery_view",
]


def generate_params(data, series_id):
    params = [('title', data['topic']),
              ('creator', data["creator"]),
              ('isPartOf', str(series_id))
              ]
    files = data["recording_files"]
    has_presenter = False
    has_presentation = False
    for presenter_view, presentation_view in VIEW_PRIORITIES.items():
        if has_presenter:
            break
        if has_view(files, presenter_view):
            for file in files:
                if file["recording_type"] == presenter_view:
                    append_files(params, file, 'presenter/source')
                    has_presenter = True
                    break
            for view in presentation_view:
                if has_presentation:
                    break
                if has_view(files, view):
                    for file in files:
                        if file["recording_type"] == view:
                            append_files(params, file, 'presentation/source')
                            has_presentation = True
                            break
                continue

    for view in FALLBACK_PRIORITIES:
        if has_presenter:
            break
        if has_view(files, view):
            for file in files:
                if file["recording_type"] == view:
                    append_files(params, file, 'presenter/source')
                    has_presenter = True
                    break

    return params, has_presenter


def append_files(params, file, flavor):
    filename = str(file['recording_id'] + '.mp4')
    params.append(('flavor', flavor))
    params.append(
        ('body', (filename, open('/srv/opencast/zoom_ingester/spool/' + filename, 'rb'), 'application/octet-stream')))


def has_view(recordings, view):
    for recording in recordings:
        if recording['recording_type'] == view:
            return True
    return False


def rcv_rabbit_callback(dlx_ch, method, properties, body):
    logging.info("Received Rabbit msg")
    data = json.loads(body.decode("utf-8"))
    if "recording_files" not in data:
        logging.error("No recording found")
    files = data["recording_files"]
    dl_url = ''
    id = ''
    recordings = []
    logging.info("Processing recordings from Zoom uuid " + uuid)
    for file in files:
        for key in file.keys():
            if key == "download_url":
                dl_url = file[key]
            elif key == "recording_id":
                id = file[key]
        try:
            if not os.path.exists('/srv/opencast/zoom_ingester/spool/' + id + '.mp4'):
                recording = wget.download(dl_url + '/?access_token=' + data["token"],
                                          '/srv/opencast/zoom_ingester/spool/' + id + '.mp4')
                recordings.append(id + '.mp4')
                logging.info("downloaded zoom recording - id {}".format(id))
            else:
                logging.info("Recording ID " + id + " has already been downloaded - skipping download")
        except Exception as e:
            dlx_ch.basic_publish(exchange='', routing_key='dlx', body=body)
            logging.error("Could not download file {}".format(e))
            return
    if not oc_upload(data, recordings):
        logging.error("Zoom UUID %s could not be processed, forwarding to DLX" % uuid)
        dlx_ch.basic_publish(exchange='', routing_key='dlx', body=body)
        return
    logging.info("Zoom UUID %s has been successfully processed" % uuid)


def oc_upload(data, recordings):
    ack = False
    creator = data["creator"]

    logging.info("Title =====>   " + data["topic"] + "   creator =========>  " + creator)

    session = requests.Session()
    session.auth = HTTPDigestAuth(user, password)
    session.headers.update({'X-Requested-Auth': 'Digest'})

    response = session.get(url + '/admin-ng/series/series.json')

    series_list = json.loads(response.content.decode("utf-8"))
    try:
        response = session.get(url + '/users/' + creator + '.json')
        series_data = response.json()
        username = series_data['user']['name']
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
        id = create_series(creator, series_title, username, session)
        time.sleep(5)

    logging.info("Uploading files ".join(str(recording) for recording in recordings) + " to Opencast")

    try:

        logging.info("Trying to upload")
        params, views_found = generate_params(data, id)
        if not views_found:
            logging.error("No Presenter found")
            return ack
        form = encoder.MultipartEncoder(fields=params)
        headers = {"Content-Type": form.content_type}
        response = session.post(url + '/ingest/addMediaPackage/fast', headers=headers, data=form)
        logging.info("POST Request done")
        if response.status_code != 200:
            logging.error("OC Upload failed \n" + response.content.decode("utf-8"))
        else:
            resp_dict = xmltodict.parse(response.content)
            mp_id = resp_dict['wf:workflow']['mp:mediapackage']['@id']
            logging.info("Zoom recordings ID : %s has been uploaded to OC with MP-ID : %s" % recordings, mp_id)
            ack = True
    except IOError:
        logging.error(
            "Zoom recordings with ID ".join(str(recording) for recording in recordings) + ' has not been found')
        return ack
    except Exception as e:
        logging.error("Error: %s" % e)
        return ack
    if not ack:
        return ack
    if save_local:
        for recording in recordings:
            logging.info("Renaming " + recording)
            try:
                os.rename('/srv/opencast/zoom_ingester/spool/' + recording, recording + "_" + mp_id + ".mp4")
            except OSError as err:
                logging.error("Exception while renaming " + str(recording) + "\n{}".format(err))
    else:
        for recording in recordings:
            logging.info("Removing " + recording)
            try:
                os.remove('/srv/opencast/zoom_ingester/spool/' + recording)
            except OSError as err:
                logging.error("Exception while removing " + str(recording) + "\n{}".format(err))

    return ack


def start_consuming_rabbitmsg():
    global rcv_connection, rcv_queue

    logging.info("Start consuming")
    credentials = pika.PlainCredentials(rabbit_user, rabbit_password)
    rcv_connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_url, credentials=credentials))
    rcv_channel = rcv_connection.channel()
    rcv_channel.exchange_declare(exchange='cycle')
    rcv_queue = rcv_channel.queue_declare(queue="zoomhook_video")
    rcv_channel.queue_bind(exchange='cycle',
                           queue='zoomhook_video')
    delay_channel = rcv_connection.channel()
    delay_channel.confirm_delivery()
    delay_channel.queue_declare(queue="dlx", durable=True, arguments={
        'x-message-ttl': 300000,
        'x-dead-letter-exchange': 'cycle',
        'x-dead-letter-routing-key': 'zoomhook_video'
    })
    msg_count = rcv_queue.method.message_count
    logging.info("Received [%s] messages", msg_count)
    rcv_channel.start_consuming()
    while msg_count > 0:
        method, prop, body = rcv_channel.basic_get(queue='zoomhook_video', auto_ack=True)
        rcv_rabbit_callback(delay_channel, method, prop, body)
        count_queue = rcv_channel.queue_declare(queue="zoomhook_video", passive=True)
        msg_count = count_queue.method.message_count


def create_series(shib_name, title, creator, session):
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

    response = session.post(url + '/api/series', data=data)
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
