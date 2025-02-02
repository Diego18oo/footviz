from flask import Flask, jsonify, render_template
from flask_cors import CORS
import os

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)  # Evita errores CORS

# Ruta para servir el HTML
@app.route('/')
def index():
    return render_template("index.html")  # Asegúrate de que está en "templates/"

# API que devuelve estadísticas en JSON
@app.route('/api/estadisticas', methods=['GET'])
def obtener_estadisticas():
    estadisticas = [
        {"nombre": "Lionel Messi", "goles": 30, "a  sistencias": 12},
        {"nombre": "Cristiano Ronaldo", "goles": 25, "asistencias": 10},    
        {"nombre": "Kylian Mbappé", "goles": 28, "asistencias": 14},
        {"nombre": "Ferran Torres", "goles": 69, "asistencias": 14}

        
    ]
    return jsonify(estadisticas)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))  # Obtiene el puerto de Render
    app.run(host='0.0.0.0', port=port, debug=False)
