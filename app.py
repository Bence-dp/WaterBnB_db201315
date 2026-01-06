from flask import Flask, request, jsonify
from flask_pymongo import PyMongo
from datetime import datetime
import paho.mqtt.client as mqtt
import json
import threading
import time

# CONFIG

MONGO_URI = "mongodb+srv://root:toor@cluster0.jqdiovx.mongodb.net/WaterBnB?retryWrites=true&w=majority&tls=true"

MQTT_BROKER = "mqtt.i3s.unice.fr"
MQTT_PORT = 1883
MQTT_TOPIC_STATUS = "uca/iot/master"
MQTT_TOPIC_CMD = "uca/iot/master"

# FLASK
app = Flask(__name__)
app.config["MONGO_URI"] = MONGO_URI
mongo = PyMongo(app)

# MEMOIRE DES PISCINES

piscines = {}

# MQTT CALLBACKS
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"MQTT connecte au broker {MQTT_BROKER}")
        client.subscribe(MQTT_TOPIC_STATUS)
        print(f"Souscription au topic: {MQTT_TOPIC_STATUS}")
    else:
        print(f"Echec connexion MQTT, code: {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        # Extraction de l'identifiant de la piscine
        ident = payload.get("info", {}).get("ident")
        occuped = payload.get("piscine", {}).get("occuped")
        if not ident:
            print("Message sans identifiant piscine")
            return
        if occuped is None:
            print(f"Message sans état 'occuped' pour la piscine {ident}")
            return
        # Mise à jour du statut en mémoire
        piscines[ident] = {
            "occuped": occuped,
            "last_update": datetime.utcnow()
        }
        print(f"Piscine {ident} | occuped={occuped}")
        # Optionnel: stocker l'historique en base
        mongo.db.pool_status.insert_one({
            "pool_id": ident,
            "occuped": occuped,
            "timestamp": datetime.utcnow(),
            "raw_data": payload
        })
    except json.JSONDecodeError as e:
        print(f"Erreur JSON: {e}")
    except Exception as e:
        print(f"Erreur traitement message: {e}")

# MQTT CLIENT
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message


# Fonction de reconnexion automatique MQTT en tâche de fond
def mqtt_connect_loop():
    while True:
        try:
            mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
            mqtt_client.loop_start()
            print("MQTT connecté et loop démarrée")
            break
        except Exception as e:
            print(f"Erreur connexion MQTT: {e}, nouvel essai dans 5s...")
            time.sleep(5)

# Lancer la reconnexion MQTT dans un thread séparé
threading.Thread(target=mqtt_connect_loop, daemon=True).start()

# ROUTES FLASK
@app.route("/")
def index():
    return jsonify({
        "service": "WaterBnB",
        "status": "running",
        "pools_tracked": len(piscines),
        "pools": list(piscines.keys())
    })

@app.route("/open")
def open_pool():
    """
    Route d'autorisation d'acces a une piscine
    Parametres GET:
    - idu: identifiant du client
    - idswp: identifiant de la piscine (ex: P_123456)
    """
    idu = request.args.get("idu")
    idswp = request.args.get("idswp")

    print(f"Demande d'acces: user={idu}, pool={idswp}")

    # Validation des parametres
    if not idu or not idswp:
        return jsonify({
            "error": "Parametres manquants",
            "required": ["idu", "idswp"]
        }), 400

    # Verification utilisateur
    user = mongo.db.users.find_one({"num": idu})
    if not user:
        print(f"Utilisateur inconnu: {idu}")
        send_pool_command(idswp, "RED")
                
        log_access(idu, idswp, "denied", "unknown user")
        return jsonify({
            "status": "denied",
            "reason": "Utilisateur non enregistre"
        }), 403

    # Verification existence piscine
    if idswp not in piscines:
        print(f"Piscine inconnue: {idswp}")
        send_pool_command(idswp, "RED")
                
        log_access(idu, idswp, "denied", "unknown pool")
        return jsonify({
            "status": "denied",
            "reason": "Piscine non trouvee"
        }), 404

    # Verification disponibilite
    pool_data = piscines[idswp]
    

    if pool_data["occuped"]:
        print(f"Piscine occupee: {idswp}")
        send_pool_command(idswp, "RED")
        
        threading.Timer(30.0, lambda: send_pool_command(idswp, "GREEN")).start()
        
        log_access(idu, idswp, "denied", "pool occupied")
        return jsonify({
            "status": "denied",
            "reason": "Piscine deja occupee"
        }), 409

    # Acces autorise
    print(f"Acces autorise: {idu} -> {idswp}")
    send_pool_command(idswp, "YELLOW")
    
    log_access(idu, idswp, "accepted", "access granted")

    return jsonify({
        "status": "accepted",
        "user": idu,
        "pool": idswp,
        "message": "Acces autorise Bonne baignade !"
    }), 200

# FONCTIONS UTILITAIRES
def send_pool_command(pool_id, color):
    """
    Envoie une commande de couleur a une piscine specifique
    """
    topic = f"uca/iot/master"
    
    # Format JSON pour l'ESP32
    command = {
        "cmd": "color",
        "value": color,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    try:
        mqtt_client.publish(topic, json.dumps(command))
        print(f"Commande envoyee: {topic} -> {color}")
    except Exception as e:
        print(f"Erreur envoi MQTT: {e}")

def log_access(user, pool, status, reason):
    """
    Enregistre chaque tentative d'acces dans MongoDB
    """
    try:
        mongo.db.access_logs.insert_one({
            "user": user,
            "pool": pool,
            "status": status,
            "reason": reason,
            "timestamp": datetime.utcnow()
        })
        print(f"Log enregistre: {user} -> {pool} [{status}]")
    except Exception as e:
        print(f"Erreur log MongoDB: {e}")

# ROUTES DEBUG (optionnel)
@app.route("/pools")
def list_pools():
    """Liste toutes les piscines connues"""
    return jsonify({
        "count": len(piscines),
        "pools": {
            k: {
                "occuped": v["occuped"],
                "last_update": v["last_update"].isoformat()
            } for k, v in piscines.items()
        }
    })

@app.route("/users")
def list_users():
    """Liste les utilisateurs (debug)"""
    users = list(mongo.db.users.find({}, {"_id": 0, "login": 1}))
    return jsonify({"users": users})

# MAIN
if __name__ == "__main__":
    print("Demarrage serveur WaterBnB...")
    app.run(host="0.0.0.0", port=5000, debug=False)