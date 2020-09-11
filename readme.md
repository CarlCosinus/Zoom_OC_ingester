#Opencast-Zoom-Ingester

**Voraussetzung**

Python Packagete:

`pika`  
`requests`  
`wget`

Für lokalen Test:

`ngrok`

**Installation**

Die oben genannten Python Pakete können alle über *pip* installiert werden  
*ngrok* kann über https://ngrok.com/download runtergeladen werden und muss im terminal in Quellordner ausgeführt werden

**Inbtriebnahme**

Zuerst muss der Webhook in Zoom aktiviert werden. Momentan läuft der Webhook-Server 
lokal auf dem Rechner. Da Zoom aber keine lokale Webseiten akzeptiert musst der Server 
öffentlich gemacht werden. Dazu wird *ngrok* verwendet.

Zuerst muss der python Server gestartet werden. Dazu zum Ordner navigieren:

`python3 -m zoom_webhook 8080`

startet nun den Server auf *localhost:8080*

Anschließend in den *ngrok*-Ordner navigieren.

Mit

`./ngrok http 8080`

wird nun der lokale Server auf Port 8080 weitergeleitet auf einen von ngrok.

Anschließend erhält man die URL auf der Server nun öffentlich erreichbar ist

Sollte dann in etwa so aussehen:

`https://f8d68f6f3073.ngrok.io/`

Danach muss der Webhook noch auf Zoom aktviert werden. Dazu in Zoom in den Marketplace gehen.
Oben rechts wählt man unter *Develop*  *Build App* aus und klickt auf *Webhook Only*.
Einfach alle nötigen Information ausfüllen.   
Eine neue *Event Subscription* wird nun hinzugefügt. Dazu als *Event Type*
 "All Recordings have completed" auswählen. Als URL die von *ngrok* eingeben. 
 Abschließend noch die App aktivieren.
 
 Damit ist der Webhook aktiviert. Abschließend muss noch das Python-Script zum Upload/Download 
 gestartet werden. 
 
 `python3 uploader.py `
 
 Danach kann in Zoom aufgenommen werden.