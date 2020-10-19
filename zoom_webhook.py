import time

from http.server import HTTPServer
from http.server import BaseHTTPRequestHandler
import pika
import json
from io import BytesIO
from zoomus import ZoomClient
import configparser
import sys
import logging

HOST_NAME = ''
PORT_NUMBER = 8080

MIN_DURATION = 0

API_KEY = ""
API_SECRET = ""
rabbit_url= ""
rabbit_user= ""
rabbit_password= ""


class BadWebhookData(Exception):
    pass


class NoMp4Files(Exception):
    pass


class MyHandler(BaseHTTPRequestHandler):
    def do_HEAD(s):
        s.send_response(200)
        s.send_header("Content-type", "text/html")
        s.end_headers()

    def do_GET(s):

        s.send_response(200)
        s.send_header("Content-type", "text/html")
        s.end_headers()

    def do_POST(s):
        """Respond to Webhook"""
        content_length = int(s.headers['Content-Length'])
        logging.info("Received webhook")
        if content_length < 5:
            s.send_response(400)
            s.end_headers()
            response = BytesIO()
            response.write(b'No data received')
            s.wfile.write(response.getvalue())
            logging.error("No data received" )
            return

        body = json.loads(s.rfile.read(content_length).decode("utf-8"))
        if "payload" not in body:
            print("payload missing")
            s.send_response(400)
            s.end_headers()
            response = BytesIO()
            response.write(b'Missing payload field in webhook body')
            s.wfile.write(response.getvalue())
            logging.error("Missing payload field in webhook body")
            return

        payload = body["payload"]
        try:
            s.validate_payload(payload)
        except BadWebhookData as e:
            logging.error("bad data")
            s.send_response(400)
            s.end_headers()
            response = BytesIO()
            response.write(b'Bad data')
            s.wfile.write(response.getvalue())
            return
        except NoMp4Files as e:
            logging.error("no mp4 found")
            s.send_response(400)
            s.end_headers()
            response = BytesIO()
            response.write(b'Unrecognized payload format')
            s.wfile.write(response.getvalue())
            return

        if payload["object"]["duration"] < MIN_DURATION:
            logging.error("Recording is too short" )
            s.send_response(400)
            s.end_headers()
            response = BytesIO()
            response.write(b'Recording is too short')
            s.wfile.write(response.getvalue())
            return

        token = body["download_token"]
        rabbit_msg , recording_id = s.construct_rabbit_msg(payload,token)

        s.send_rabbit_msg(rabbit_msg)
        logging.info("Rabbit msg w/ rec ID %s has been sent" % recording_id)
        s.send_response(200)
        s.end_headers()
        response = BytesIO()
        response.write(b'Success')
        s.wfile.write(response.getvalue())

    def construct_rabbit_msg(self, payload,token):
        now = time.asctime()
        logging.info("Constructing rabbit msg" )
        zoom_client = ZoomClient(API_KEY, API_SECRET)
        user_list_response = zoom_client.user.get(id=payload["object"]["host_id"])
        user_list = json.loads(user_list_response.content.decode("utf-8"))

        recording_files = []
        for file in payload["object"]["recording_files"]:
            if file["file_type"].lower() == "mp4":
                rec_id = file["id"]
                recording_files.append({
                    "recording_id": file["id"],
                    "recording_start": file["recording_start"],
                    "recording_end": file["recording_end"],
                    "download_url": file["download_url"],
                    "file_type": file["file_type"],
                    "recording_type": file["recording_type"]
                })

        rabbit_msg = {
            "uuid": payload["object"]["uuid"],
            "zoom_series_id": payload["object"]["id"],
            "topic": payload["object"]["topic"],
            "start_time": payload["object"]["start_time"],
            "duration": payload["object"]["duration"],
            "host_id": payload["object"]["host_id"],
            "recording_files": recording_files,
            "token": token,
            "received_time": now,
            "creator": user_list["location"]
        }
        logging.info("Constructed Rabbit Msg")

        return rabbit_msg, rec_id

    def send_rabbit_msg(self,msg):
        credentials = pika.PlainCredentials(rabbit_user,rabbit_password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_url, credentials=credentials))
        channel = connection.channel()
        channel.queue_declare(queue="zoomhook")
        channel.basic_publish(exchange='',
                              routing_key="zoomhook",
                              body=json.dumps(msg))
        connection.close()

    def validate_payload(s,payload):
        required_payload_fields = [
            "object"
        ]
        required_object_fields = [
            "id",  # zoom series id
            "uuid",  # unique id of the meeting instance,
            "host_id",
            "topic",
            "start_time",
            "duration",  # duration in minutes
            "recording_files"
        ]
        required_file_fields = [
            "id",  # unique id for the file
            "recording_start",
            "recording_end",
            "download_url",
            "file_type",
            "recording_type"
        ]

        try:
            for field in required_payload_fields:
                if field not in payload.keys():
                    logging.error("Missing required payload field '{}'. Keys found: {}"
                            .format(field, payload.keys()))
                    raise BadWebhookData(
                        "Missing required payload field '{}'. Keys found: {}"
                            .format(field, payload.keys()))

            obj = payload["object"]
            for field in required_object_fields:
                if field not in obj.keys():
                    logging.error("Missing required object field '{}'. Keys found: {}"
                            .format(field, obj.keys()))
                    raise BadWebhookData(
                        "Missing required object field '{}'. Keys found: {}"
                            .format(field, obj.keys()))

            files = obj["recording_files"]

            # make sure there's some mp4 files in here somewhere
            mp4_files = any(x["file_type"].lower() == "mp4" for x in files)
            if not mp4_files:
                logging.error("No mp4 files in recording data")
                raise NoMp4Files("No mp4 files in recording data")

            for file in files:
                if "file_type" not in file:
                    logging.error("Missing required file field 'file_type'")
                    raise BadWebhookData("Missing required file field 'file_type'")
                if file["file_type"].lower() != "mp4":
                    continue
                for field in required_file_fields:
                    if field not in file.keys():
                        logging.error("Missing required file field '{}'".format(field))
                        raise BadWebhookData(
                            "Missing required file field '{}'".format(field))
                if "status" in file and file["status"].lower() != "completed":
                    logging.error(" File with incomplete status {}".format(file["status"]))
                    raise BadWebhookData(
                        "File with incomplete status {}".format(file["status"])
                    )

        except NoMp4Files:
            # let these bubble up as we handle them differently depending
            # on who the caller is
            raise
        except Exception as e:
            logging.error("Unrecognized payload format. {}".format(e))
            raise BadWebhookData("Unrecognized payload format. {}".format(e))


if __name__ == '__main__':

    logging.basicConfig(filename='webhook_error.log',level=logging.DEBUG, format='%(asctime)s %(message)s',datefmt='%d/%m/%Y %I:%M:%S %p')
    try:
        config = configparser.ConfigParser()
        config.read('settings.ini')
        logging.info("Found Settings")
    except FileNotFoundError:
        logging.error("No settings found ")
        sys.exit("No settings found")

    try:
        API_KEY = config["JWT"]["Key"]
        API_SECRET = config["JWT"]["Secret"]
        PORT_NUMBER = int(config["Webhook"]["Port"])
        HOST_NAME = config["Webhook"]["Url"]
        rabbit_url = config["Rabbit"]["Url"]
        MIN_DURATION = int(config["Webhook"]["Min_Duration"])
        rabbit_user = config["Rabbit"]["User"]
        rabbit_password = config["Rabbit"]["Password"]
        logging.info("Settings are set" )
    except KeyError as err:
        logging.error("Key {0} was not found".format(err))
        sys.exit("Key {0} was not found".format(err))
    except ValueError as err:
        logging.error("Invalid value, integer expected : {0}".format(err))
        sys.exit("Invalid value, integer expected : {0}".format(err))

    server_class = HTTPServer
    httpd = server_class((HOST_NAME, PORT_NUMBER), MyHandler)
    logging.info("Server Starts - %s:%s" % (HOST_NAME, PORT_NUMBER))
    try:
        httpd.serve_forever()

    except KeyboardInterrupt:
        pass
    httpd.server_close()
    logging.info("Server Stops - %s:%s" % (HOST_NAME, PORT_NUMBER))