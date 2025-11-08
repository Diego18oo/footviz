

import express from 'express'
import bcrypt from 'bcrypt'; 
import jwt from 'jsonwebtoken';
import cookieParser from 'cookie-parser';
import {getTablaLiga, getUltimosPartidos, getMaximosGoleadores, getMejoresValorados, getEstadisticasOfensivas, getStatsJugador, buscarJugadores, getStatsMaximas, getMejoresGoles, getEstadisticasOfensivasEquipo, getXgPorEquipo, getMapaDeDisparosEquipo, getEvolucionEquipos, getPromediosStatsDeUnaLiga, getPartidos, getResultadoPartido, getInfoPrePartido, getPosiblesAlineaciones, getUltimosEnfrentamientos, getEstadisticasEquipo, getComparacionEvolucionEquipos, getComparacionStatsEquipos, getInfoPostPartido, getEstadisticasPartido, getMapaDeDisparosPartido, getMapaDeCalorJugador, getMapaDeDisparosJugador, getPercentilesJugador, getUltimosPartidosJugador, getInfoJugador, getUltimosPartidosPortero, getPercentilesPortero, getEstadisticasPortero, getInfoClub, getUltimosPartidosClub, getAlineacionClub, getPlantillaClub, getTodosLosEquipos, crearUsuario, buscarUsuarioPorEmail, buscarUsuarioPorUsername, getTodosLosPaises, findUserByEmail, getUsuarioData, getEquipoFantasyUsuario, getJugadoresFantasy, crearEquipoFantasyCompleto, getPlantillaFantasy, getDesglosePuntosFantasyJugador, getProximoPartido, realizarFichaje, actualizarPlantilla, getProximosPartidosDeEquipos, getMVPdeLaSemana, getPartidosRandom, getPlantillasDePredicciones, getEstadoDeForma, getPronosticoResultado, getPronosticoPrimerEquipoEnAnotar, getPronosticoPrimerJugadorEnAnotar, guardarPrediccionUsuario, getJornadaFromPartido, getMisPredicciones, getHistorialPredicciones} from './database.js'
 

const app = express()  
app.use(express.static("public"))
app.use(express.json());
app.use(cookieParser());

const JWT_SECRET = process.env.JWT_SECRET || 'tu_super_secreto_de_desarrollo';

function authenticateToken(req, res, next) {
    // 1. Obtener el token de la cookie
    const token = req.cookies.authToken;

    // Si no hay token, el usuario no está autenticado
    if (!token) {
        return res.sendStatus(401); // 401 Unauthorized
    }

    // 2. Verificar la firma del token
    jwt.verify(token, JWT_SECRET, (err, decodedPayload) => {
        if (err) {
            // Si el token es inválido o ha caducado
            return res.sendStatus(403); // 403 Forbidden
        }

        // 3. ¡Token válido! Guardamos el ID del usuario en el objeto `req`
        // para que las rutas posteriores sepan quién es.
        req.userId = decodedPayload.userId;
        next(); // Permite que la petición continúe a la ruta solicitada
    });
}

app.get("/api/fantasy/historial-predicciones", authenticateToken, async (req, res) => {
    try {
        const historial = await getHistorialPredicciones(req.userId);
        res.status(200).json(historial);
    } catch (error) {
        console.error("Error al obtener historial de predicciones:", error);
        res.status(500).json({ error: "Error interno del servidor." });
    }
});

app.get("/api/fantasy/mis-predicciones", authenticateToken, async (req, res) => {
    try {
        const userId = req.userId;
        const { jornada } = req.query; // Esperamos ?jornada=11

        if (!jornada) {
            return res.status(400).json({ error: "Falta el número de jornada." });
        }

        const misPredicciones = await getMisPredicciones(userId, jornada);
        
        res.status(200).json(misPredicciones);

    } catch (error) {
        console.error("Error al obtener mis predicciones:", error);
        res.status(500).json({ error: "Error interno del servidor." });
    }
});

app.post("/api/fantasy/guardar-prediccion", authenticateToken, async (req, res) => {
    try {
        const userId = req.userId;
        const { id_partido, resultado_predicho, primer_equipo_anotar_id, primer_jugador_anotar_id } = req.body;

        if (!id_partido) {
            return res.status(400).json({ error: "Falta el ID del partido." });
        }

        // 1. Obtener la jornada del partido (necesaria para la tabla)
        const partidoInfo = await getJornadaFromPartido(id_partido);
        if (!partidoInfo) {
            return res.status(404).json({ error: "El partido no existe." });
        }

        // 2. Preparar el objeto de datos para la BD
        const prediccion = {
            id_usuario: userId,
            id_partido: id_partido,
            jornada: partidoInfo.jornada,
            resultado_predicho: resultado_predicho,
            primer_equipo_anotar_id: primer_equipo_anotar_id,
            primer_jugador_anotar_id: primer_jugador_anotar_id
        };

        // 3. Llamar a la función de la BD (que usa ON DUPLICATE KEY UPDATE)
        await guardarPrediccionUsuario(prediccion);

        res.status(200).json({ message: "Predicción guardada con éxito." });

    } catch (error) {
        console.error("Error al guardar predicción:", error);
        res.status(500).json({ error: "Error interno del servidor." });
    }
});


app.get("/api/fantasy/predicciones-populares/:id",  async (req, res) => {
    try {
        const { id } = req.params; 
        const resultado = await getPronosticoResultado(id);
        const primerEquipo = await getPronosticoPrimerEquipoEnAnotar(id);
        const primerJugador = await getPronosticoPrimerJugadorEnAnotar(id);


        if (resultado.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json({resultado, primerEquipo, primerJugador});   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/estado-forma/:id",  async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getEstadoDeForma(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/plantillas-partido/:id",  async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getPlantillasDePredicciones(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/fantasy/partidos-prediccion",  async (req, res) => {
    try {
        
        const info = await getPartidosRandom();

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/fantasy/mvp-de-la-jornada",  async (req, res) => {
    try {
        
        const info = await getMVPdeLaSemana();

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/fantasy/proximos-partidos",  async (req, res) => {
    try {
        
        const info = await getProximosPartidosDeEquipos();

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.post("/api/fantasy/actualizar-plantilla", authenticateToken, async (req, res) => {
    try {
        const userId = req.userId; // Gracias al middleware
        const { plantilla } = req.body; // El array de {id_jugador, es_titular, es_capitan}

        // Validación simple
        if (!plantilla || plantilla.length !== 15) {
            return res.status(400).json({ error: "Datos de plantilla inválidos." });
        }

        // 1. Obtener el id_equipo_fantasy del usuario
        // (getEquipoFantasyUsuario solo necesita el userId según tu schema)
        const equipo = await getEquipoFantasyUsuario(userId); 

        if (!equipo) {
            return res.status(404).json({ error: "No se encontró el equipo fantasy del usuario." });
        }
        
        // 2. Llamar a la función de la BD para actualizar
        await actualizarPlantilla(equipo.id_equipo_fantasy, plantilla);

        res.status(200).json({ message: "¡Plantilla actualizada con éxito!" });

    } catch (error) {
        console.error("Error al actualizar plantilla:", error);
        res.status(500).json({ error: "Error interno del servidor." });
    }
});


app.post("/api/fantasy/hacer-fichaje", authenticateToken, async (req, res) => {
    try {
        const userId = req.userId;
        const { jugadorSaleId, jugadorEntraId } = req.body;

        // 1. Obtener el equipo fantasy del usuario
        const equipo = await getEquipoFantasyUsuario(userId);
        if (!equipo) {
            return res.status(404).json({ error: "No se encontró el equipo fantasy." });
        }
        
        // 2. Llamar a la función de la BD que hace la transacción
        await realizarFichaje(
            equipo.id_equipo_fantasy, 
            jugadorSaleId, 
            jugadorEntraId,
            equipo.fichajes_jornada_restantes,
            equipo.id_temporada // Pasamos la temporada del equipo
        );

        res.status(200).json({ message: "Fichaje realizado con éxito." });

    } catch (error) {
        console.error("Error al hacer fichaje:", error);
        // Devolver errores de validación al usuario
        if (error.message.includes("Presupuesto") || error.message.includes("Límite")) {
             return res.status(400).json({ error: error.message });
        }
        res.status(500).json({ error: "Error interno del servidor." });
    }
});

app.get("/api/fantasy/proximo-partido/:id",  async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getProximoPartido(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/fantasy/player-details/:id",  async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getDesglosePuntosFantasyJugador(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/fantasy/mi-equipo", authenticateToken, async (req, res) => {
    // Gracias al middleware, ahora tenemos acceso a req.userId
    const userId = req.userId;

    try {
        // Llama a funciones de database.js pasándoles el userId
        const userInfo = await getUsuarioData(userId);
        const equipoInfo = await getEquipoFantasyUsuario(userId);
        const plantillaConDetalles = await getPlantillaFantasy(userId);
        //const ligasData = await getLigasUsuario(userId);
        // ... obtener el resto de los datos (partidos, etc.)

        // Devuelve los datos específicos de ESE usuario
        res.status(200).json({userInfo, equipoInfo, plantillaConDetalles});
    } catch (error) {
        console.error(`Error al obtener dashboard para usuario ${userId}:`, error);
        res.status(500).json({ error: "Error interno al obtener datos del dashboard" });
    }
});

app.post("/api/fantasy/crear-equipo", authenticateToken, async (req, res) => {
    try {
        // 1. Obtenemos el ID del usuario desde el token verificado
        const userId = req.userId;

        // 2. Obtenemos los datos enviados desde el frontend
        const { nombreEquipo, presupuestoRestante, plantilla } = req.body;

        // 3. Validaciones de backend 
        if (!nombreEquipo || nombreEquipo.trim().length < 3) {
            return res.status(400).json({ error: "El nombre del equipo es muy corto." });
        }
        if (!plantilla || plantilla.length !== 15) {
            return res.status(400).json({ error: "La plantilla debe tener exactamente 15 jugadores." });
        }
        if (presupuestoRestante < 0) {
            return res.status(400).json({ error: "El presupuesto no puede ser negativo." });
        }
        
        // 4. Llamamos a la nueva función en database.js para que haga la magia
        // Le pasamos los datos y el ID del usuario
        const nuevoEquipo = await crearEquipoFantasyCompleto(userId, nombreEquipo, presupuestoRestante, plantilla);

        res.status(201).json({ message: "Equipo creado con éxito", equipoId: nuevoEquipo.id_equipo_fantasy });

    } catch (error) {
        console.error("Error al crear equipo fantasy:", error);
        // Manejamos errores comunes, como "equipo ya existe"
        if (error.code === 'ER_DUP_ENTRY') {
             return res.status(409).json({ error: "Ya has creado un equipo para esta temporada." });
        }
        res.status(500).json({ error: "Error interno del servidor" });
    }
});

app.get("/api/fantasy/available-players", authenticateToken, async (req, res) => {
    const userId = req.userId;
    console.log(`Usuario ${userId} está pidiendo jugadores disponibles.`);

    try {
        
        const playersData = await getJugadoresFantasy();
        
        res.status(200).json(playersData);
    } catch (error) {
        console.error(`Error al obtener jugadores para usuario ${userId}:`, error);
        res.status(500).json({ error: "Error interno al obtener datos" });
    }
});

app.get("/api/fantasy-dashboard", authenticateToken, async (req, res) => {
    // Gracias al middleware, ahora tenemos acceso a req.userId
    const userId = req.userId;
    console.log(`Usuario ${userId} está pidiendo su dashboard.`);

    try {
        // Llama a funciones de database.js pasándoles el userId
        const userData = await getUsuarioData(userId);
        const equipoData = await getEquipoFantasyUsuario(userId);
        const plantillaConDetalles = await getPlantillaFantasy(userId);
        //const ligasData = await getLigasUsuario(userId);
        // ... obtener el resto de los datos (partidos, etc.)

        // Devuelve los datos específicos de ESE usuario
        res.status(200).json({userData, equipoData, plantillaConDetalles});
    } catch (error) {
        console.error(`Error al obtener dashboard para usuario ${userId}:`, error);
        res.status(500).json({ error: "Error interno al obtener datos del dashboard" });
    }
});

app.post("/api/login", async (req, res) => {
    try {
        const { email, password } = req.body;

        // 1. Buscar al usuario en la BD por su email
        const user = await findUserByEmail(email);

        // Si el usuario no existe, las credenciales son inválidas
        if (!user) {
            return res.status(401).json({ error: "Credenciales inválidas" });
        }

        // 2. Comparar la contraseña enviada con el hash guardado en la BD
        const isMatch = await bcrypt.compare(password, user.hashed_password);

        // Si la comparación falla, las credenciales son inválidas
        if (!isMatch) {
            return res.status(401).json({ error: "Credenciales inválidas" });
        }

        // 1. Crear el "payload" (la información dentro de la credencial)
        const payload = { userId: user.id_usuario };

        // 2. Firmar el token con tu secreto y ponerle una caducidad (ej. 1 hora)
        const token = jwt.sign(payload, JWT_SECRET, { expiresIn: '1h' });

        // 3. Enviar el token en una cookie segura
        res.cookie('authToken', token, {
            httpOnly: true, // El navegador no permite a JS leer esta cookie (seguridad XSS)
            secure: process.env.NODE_ENV === 'production', // Solo enviar por HTTPS en producción
            sameSite: 'strict', // Protección CSRF
            maxAge: 3600000 // Tiempo de vida de la cookie en milisegundos (1 hora)
        });

        // 4. Enviar respuesta de éxito (sin el token en el cuerpo)
        res.status(200).json({ message: "Login exitoso", userId: user.id_usuario });

    } catch (error) {
        console.error("Error en el login:", error);
        res.status(500).json({ error: "Error interno del servidor" });
    }
});

app.post("/api/registrar-usuario", async (req, res) => {
    try {
        const { username, correo, contrasenia, fecnac, pais, equipo_favorito } = req.body;

        
        const usuarioExistente = await buscarUsuarioPorUsername(username);
        if (usuarioExistente) {
            return res.status(409).json({ error: "El nombre de usuario ya está en uso." });
        } 
        // RQNF1
        const correoExistente = await buscarUsuarioPorEmail(correo);
        if (correoExistente) {
            return res.status(409).json({ error: "El correo ya está en uso." });
        } 
        
        // Hashea la contraseña ANTES de guardarla
        const saltRounds = 10;
        const hashedPassword = await bcrypt.hash(contrasenia, saltRounds);

        const nuevoUsuario = await crearUsuario({
            username,
            correo,
            hashed_password: hashedPassword, 
            fecha_nacimiento: fecnac,
            pais_id: pais,
            equipo_favorito_id: equipo_favorito
        });

        res.status(201).json({ message: "Usuario creado con éxito", userId: nuevoUsuario.id });

    } catch (error) {
        console.error("Error al registrar usuario:", error);
        res.status(500).json({ error: "Error interno del servidor" });
    }
});


app.get("/api/lista-paises", async (req, res) => {
    try {
        
        const info = await getTodosLosPaises();

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del club" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del club" });
    }
});

app.get("/api/lista-equipos", async (req, res) => {
    try {
        
        const info = await getTodosLosEquipos();

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del club" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del club" });
    }
});



app.get("/api/plantilla-club/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getPlantillaClub(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del club" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del club" });
    }
});

app.get("/api/alineacion-club/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getAlineacionClub(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del club" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del club" });
    }
});

app.get("/api/ultimos-partidos-club/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getUltimosPartidosClub(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del club" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del club" });
    }
});

app.get("/api/info-club/:id", async(req, res) => {
    try {
        const { id } = req.params; 
        const infoClub = await getInfoClub(id);

        if (infoClub.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del club" });
        }

        res.json(infoClub[0]); // devuelves solo el club, no array
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del club" });
    }
})

app.get("/api/estadisticas-portero/:id", async(req, res) => {
    try {
        const { id } = req.params; // ejemplo: /estadisticas-jugador/45
        const estadisticas_jugador = await getEstadisticasPortero(id);

        if (estadisticas_jugador.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(estadisticas_jugador[0]); // devuelves solo el jugador, no array
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
})

app.get("/api/percentiles-portero/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getPercentilesPortero(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/ultimos-partidos-portero/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getUltimosPartidosPortero(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/jugador/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getInfoJugador(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/ultimos-partidos-jugador/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getUltimosPartidosJugador(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/percentiles-jugador/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getPercentilesJugador(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/mapa-calor-jugador/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getMapaDeCalorJugador(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/mapa-disparos-jugador/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getMapaDeDisparosJugador(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del partido" });
    }
});

app.get("/api/mapa-disparos-partido/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getMapaDeDisparosPartido(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del partido" });
    }
});

app.get("/api/estadisticas-partido/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getEstadisticasPartido(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del partido" });
    }
});

app.get("/api/info-postpartido/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getInfoPostPartido(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del partido" });
    }
});
app.get("/api/comparacion-estadisticas-equipos/:id1/:id2", async (req, res) => {
    try {
        const { id1 } = req.params; 
        const { id2 } = req.params; 
        const stats = await getComparacionStatsEquipos(id1, id2);

        if (stats.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion" });
        }
 
        res.json(stats); 
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener la informacion" });
    }
});

app.get("/api/comparacion-evoluciones/:id1/:id2", async (req, res) => {
    try {
        const { id1 } = req.params; 
        const { id2 } = req.params; 
        const evolucion = await getComparacionEvolucionEquipos(id1, id2);

        if (evolucion.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion" });
        }
 
        res.json(evolucion); 
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener la informacion" });
    }
});


app.get("/api/estadisticas-equipo/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getEstadisticasEquipo(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del partido" });
    }
});

app.get("/api/ultimos-enfrentamientos/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getUltimosEnfrentamientos(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del partido" });
    }
});

app.get("/api/alineaciones/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getPosiblesAlineaciones(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del partido" });
    }
});

app.get("/api/info-prepartido/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getInfoPrePartido(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del partido" });
    }
});
 

app.get("/api/resultado-partido/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const resultado = await getResultadoPartido(id);

        if (resultado.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(resultado);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del partido" });
    }
});

app.get("/api/ultimos-partidos/:id1/:id2", async (req, res) => {
    try {
        const { id1 } = req.params; 
        const { id2 } = req.params; 
        const partidos = await getPartidos(id1, id2);

        if (partidos.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion" });
        }
 
        res.json(partidos); 
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener partidos" });
    }
});

app.get("/api/promedios-liga/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const promedios = await getPromediosStatsDeUnaLiga(id);

        if (promedios.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion de la liga seleccionada" });
        }

        res.json(promedios);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas de la liga seleccionada" });
    }
});

app.get("/api/evolucion-equipos/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const evolucion = await getEvolucionEquipos(id);

        if (evolucion.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del equipo" });
        }

        res.json(evolucion);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del equipo" });
    }
});

app.get("/api/mapa-disparos/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const mapaDisparos = await getMapaDeDisparosEquipo(id);

        if (mapaDisparos.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del equipo" });
        }

        res.json(mapaDisparos); 
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del equipo" });
    }
});

app.get("/api/goles-esperados-equipo/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const xgEquipo = await getXgPorEquipo(id);

        if (xgEquipo.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del equipo" });
        }

        res.json(xgEquipo); 
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas de los equipos" });
    }
});


app.get("/api/estadisticas-ofensivas-equipo/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const estadisticas_ofensivas_equipo = await getEstadisticasOfensivasEquipo(id);

        if (estadisticas_ofensivas_equipo.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion de la liga" });
        }

        res.json(estadisticas_ofensivas_equipo); 
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas de los equipos" });
    }
});

app.get("/api/mejores-goles/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const mejores_goles = await getMejoresGoles(id);

        if (mejores_goles.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }
  
        res.json(mejores_goles); 
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/max-stats/:id1/:id2", async (req, res) => {
    try {
        const { id1 } = req.params; 
        const { id2 } = req.params; 
        const max_stats = await getStatsMaximas(id1, id2);

        if (max_stats.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }
 
        res.json(max_stats); 
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/buscar-jugadores", async (req, res) => {
    try {
        const { nombre } = req.query;

        if (!nombre) {
            return res.status(400).json({ error: "Falta el parámetro nombre" });
        }

        const jugadores = await buscarJugadores(nombre);

        res.json(jugadores);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error en la búsqueda de jugadores" });
    }
}); 

app.get("/api/estadisticas-jugador/:id", async(req, res) => {
    try {
        const { id } = req.params; // ejemplo: /estadisticas-jugador/45
        const estadisticas_jugador = await getStatsJugador(id);

        if (estadisticas_jugador.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(estadisticas_jugador[0]); // devuelves solo el jugador, no array
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
})

app.get("/api/estadisticas-ofensivas-premier-league", async(req, res) => {
    const estadisticas_ofensivas = await getEstadisticasOfensivas(1)
    res.json(estadisticas_ofensivas);
})

app.get("/api/estadisticas-ofensivas-la-liga", async(req, res) => {
    const estadisticas_ofensivas = await getEstadisticasOfensivas(2)
    res.json(estadisticas_ofensivas);
})
app.get("/api/estadisticas-ofensivas-serie-a", async(req, res) => {
    const estadisticas_ofensivas = await getEstadisticasOfensivas(3)
    res.json(estadisticas_ofensivas);
})

app.get("/api/estadisticas-ofensivas-bundesliga", async(req, res) => {
    const estadisticas_ofensivas = await getEstadisticasOfensivas(4)
    res.json(estadisticas_ofensivas);
})

app.get("/api/estadisticas-ofensivas-ligue-one", async(req, res) => {
    const estadisticas_ofensivas = await getEstadisticasOfensivas(5)
    res.json(estadisticas_ofensivas);
})
app.get("/api/index", async(req,res) => {
    console.log("Se hizo una solicitud a /index");
    const tabla_liga = await getTablaLiga(1)
    const ultimos_partidos = await getUltimosPartidos(1)
    const maximos_goleadores = await getMaximosGoleadores(1)
    const mejores_valorados = await getMejoresValorados(1)
    res.json({ tabla_liga, ultimos_partidos, maximos_goleadores, mejores_valorados });
    
})

app.get("/api/liga/premier-league", async(req,res) => {
    console.log("Se hizo una solicitud a /premier-league");
    const tabla_liga = await getTablaLiga(1)
    const ultimos_partidos = await getUltimosPartidos(1)
    const maximos_goleadores = await getMaximosGoleadores(1)
    const mejores_valorados = await getMejoresValorados(1)
    res.json({ tabla_liga, ultimos_partidos, maximos_goleadores, mejores_valorados });
    
})



app.get("/api/liga/la-liga", async(req,res) => {
    console.log("Se hizo una solicitud a /la-liga");
    const tabla_liga = await getTablaLiga(2) 
    const ultimos_partidos = await getUltimosPartidos(2)
    const maximos_goleadores = await getMaximosGoleadores(2)
    const mejores_valorados = await getMejoresValorados(2)
    res.json({ tabla_liga, ultimos_partidos, maximos_goleadores, mejores_valorados });
})

app.get("/api/liga/serie-a", async(req,res) => {
    console.log("Se hizo una solicitud a /serie-a");
    const tabla_liga = await getTablaLiga(3)
    const ultimos_partidos = await getUltimosPartidos(3)
    const maximos_goleadores = await getMaximosGoleadores(3)
    const mejores_valorados = await getMejoresValorados(3)
    res.json({ tabla_liga, ultimos_partidos, maximos_goleadores, mejores_valorados });
})

app.get("/api/liga/bundesliga", async(req,res) => {
    console.log("Se hizo una solicitud a /bundesliga");
    const tabla_liga = await getTablaLiga(4)
    const ultimos_partidos = await getUltimosPartidos(4)
    const maximos_goleadores = await getMaximosGoleadores(4)
    const mejores_valorados = await getMejoresValorados(4)
    res.json({ tabla_liga, ultimos_partidos, maximos_goleadores, mejores_valorados });
    
})

app.get("/api/liga/ligue-one", async(req,res) => {
    console.log("Se hizo una solicitud a /ligue-one");
    const tabla_liga = await getTablaLiga(5)
    const ultimos_partidos = await getUltimosPartidos(5)
    const maximos_goleadores = await getMaximosGoleadores(5)
    const mejores_valorados = await getMejoresValorados(5)
    console.log(tabla_liga)
    res.json({ tabla_liga, ultimos_partidos, maximos_goleadores, mejores_valorados });
})
  
 


app.use((err, req, res, next) => {
    console.error(err.stack)
    res.status(500).send("Something broke")
})

app.listen(8080, () => {
    console.log('Server is running on port 8080')
})