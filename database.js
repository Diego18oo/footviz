import mysql from 'mysql2'

import dotenv from 'dotenv'
dotenv.config() 

const pool = mysql.createPool({
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE
}).promise()

export async function getTablaLiga(temporada){
    const [rows] = await pool.query(`
        SELECT
            e.id_equipo, 
            e.nombre AS nombre_equipo,
            e.url_imagen,
            ee.partidos_jugados,
            ee.posicion,
            ee.puntos,
            ee.victorias, 
            ee.empates,
            ee.derrotas, 
            ee.diferencia_de_goles,
            ee.goles_anotados,
            ee.goles_recibidos
        FROM estadisticas_equipo ee 
        JOIN equipo e ON ee.equipo = e.id_equipo
        WHERE ee.temporada = ?
        `, [temporada]) 
    return rows
}

 

export async function getUltimosPartidos(liga){
    const [rows] = await pool.query(`
        
        SELECT 
            p.fecha,
            el.url_imagen AS escudo_local,
            ev.url_imagen AS escudo_visitante,
            ep.goles_local,
            ep.goles_visitante
        FROM partido p
        JOIN temporada t ON p.temporada = t.id_temporada
        JOIN estadisticas_partido ep ON ep.partido = p.id_partido
        JOIN equipo el ON p.equipo_local = el.id_equipo
        JOIN equipo ev ON p.equipo_visitante = ev.id_equipo
        WHERE t.liga = ?  
        ORDER BY p.fecha DESC
        LIMIT 5;

        `, [liga])  
    return rows
}
     

export async function getMaximosGoleadores(temporada) {
    const [rows] = await pool.query(`
        SELECT 
            j.url_imagen as imagen_jugador,
            j.nombre as nombre_jugador,
            SUM(ejp.goles) AS total_goles
        FROM estadistica_jugador_partido ejp
        JOIN partido p ON ejp.partido = p.id_partido
        JOIN jugador j ON ejp.jugador = j.id_jugador
        WHERE p.temporada = ?
        GROUP BY ejp.jugador
        ORDER BY total_goles DESC
        LIMIT 5;
        
        `, [temporada])
    return rows
    
}

export async function getMejoresValorados(temporada) {
    const [rows] = await pool.query(`
        SELECT 
            j.nombre as nombre_jugador,
            j.url_imagen as imagen_jugador,
            ROUND(AVG(ejp.rating),2) AS mejor_valoracion
        FROM estadistica_jugador_partido ejp
        JOIN partido p ON ejp.partido = p.id_partido
        JOIN jugador j ON ejp.jugador = j.id_jugador
        WHERE p.temporada = ?
        GROUP BY ejp.jugador
        ORDER BY mejor_valoracion DESC
        LIMIT 5;
        `, [temporada])
    
    return rows
}

export async function getEstadisticasOfensivas(temporada) {
    const [rows] = await pool.query(`
        SELECT 
            j.url_imagen AS imagen_jugador,
            j.nombre AS nombre_jugador,
            e.nombre AS nombre_equipo,
            e.url_imagen AS imagen_equipo,
            SUM(ejp.minutos) as minutos,
            SUM(ejp.goles) AS total_goles,
            sj.disparos,
            sj.disparos_a_puerta,
            sj.disparos_a_puerta / sj.disparos as porcentaje_disparos_a_puerta,
            sj.disparos / SUM(ejp.minutos) * 100 as disparos_por_90,
            sj.goles_por_disparo,
            sj.distancia_promedio_de_disparos,
            sj.tiros_libres,
            sj.penales,
            SUM(ejp.goles_esperados)
        FROM estadistica_jugador_partido ejp
        JOIN partido p ON ejp.partido = p.id_partido
        JOIN jugador j ON ejp.jugador = j.id_jugador
        JOIN plantilla_equipos pe ON j.id_jugador = pe.jugador
        JOIN equipo e ON pe.equipo = e.id_equipo
        JOIN estadistica_jugador sj ON j.id_jugador = sj.jugador
        WHERE p.temporada = ?
        GROUP BY 
            ejp.jugador, 
            j.url_imagen, 
            j.nombre, 
            e.nombre, 
            e.url_imagen, 
            sj.disparos,
            sj.disparos_a_puerta,
            sj.goles_por_disparo,
            sj.distancia_promedio_de_disparos,
            sj.tiros_libres,
            sj.penales
        ORDER BY total_goles DESC;

        
        `, [temporada])
    return rows
    
}

export async function getStatsJugador(id_jugador) {
    const [rows] = await pool.query(`
        SELECT
            j.id_jugador,
            j.url_imagen AS imagen_jugador,
            j.nombre AS nombre_jugador,
            e.nombre AS nombre_equipo,
            e.url_imagen AS imagen_equipo,
            SUM(ejp.goles) AS total_goles,
            sj.disparos,
            sj.asistencias,
            sj.acciones_creadas,
            sj.pases,
            sj.porcentaje_de_efectividad_de_pases,
            sj.pases_progresivos,
            sj.acarreos_progresivos,
            sj.regates_efectivos,
            sj.toques_de_balon,
            sj.entradas,
            sj.intercepciones,
            sj.bloqueos,
            sj.despejes,
            sj.duelos_aereos_ganados
        FROM estadistica_jugador_partido ejp
        JOIN partido p ON ejp.partido = p.id_partido
        JOIN jugador j ON ejp.jugador = j.id_jugador
        JOIN plantilla_equipos pe ON j.id_jugador = pe.jugador
        JOIN equipo e ON pe.equipo = e.id_equipo
        JOIN estadistica_jugador sj ON j.id_jugador = sj.jugador
        WHERE j.id_jugador = ?
        GROUP BY 
            j.id_jugador,
            ejp.jugador, 
            j.url_imagen, 
            j.nombre, 
            e.nombre, 
            e.url_imagen, 
            sj.disparos,
            sj.asistencias,
            sj.acciones_creadas,
            sj.pases,
            sj.porcentaje_de_efectividad_de_pases,
            sj.pases_progresivos,
            sj.acarreos_progresivos,
            sj.regates_efectivos,
            sj.toques_de_balon,
            sj.entradas,
            sj.intercepciones,
            sj.bloqueos,
            sj.despejes,
            sj.duelos_aereos_ganados
        ORDER BY total_goles DESC;

        `, [id_jugador])
    return rows 
    
}


export async function buscarJugadores(nombre) {
    const [rows] = await pool.query(`
        SELECT 
            j.id_jugador,
            j.nombre AS nombre_jugador,
            e.nombre AS nombre_equipo,
            j.url_imagen AS imagen_jugador,
            e.url_imagen AS imagen_equipo
        FROM jugador j
        JOIN plantilla_equipos pe ON j.id_jugador = pe.jugador
        JOIN equipo e ON pe.equipo = e.id_equipo
        WHERE j.nombre LIKE ?
        LIMIT 10;
    `, [`%${nombre}%`]); // el % permite coincidencias parciales

    return rows;
} 

export async function getStatsMaximas(jugador1, jugador2) {
    const [rows] = await pool.query(`
        SELECT 
            MAX(goles) AS max_goles,
            MAX(disparos) AS max_disparos,
            MAX(asistencias) AS max_asistencias,
            MAX(acciones_creadas) as max_acciones_creadas,
            MAX(pases) AS max_pases,
            MAX(porcentaje_de_efectividad_de_pases) as max_porcentaje_de_efectividad_de_pases,
            MAX(pases_progresivos) as max_pases_progresivos,
            MAX(acarreos_progresivos) as max_acarreos_progresivos,
            MAX(regates_efectivos) as max_regates_efectivos,
            MAX(toques_de_balon) AS max_toques_de_balon,
            MAX(entradas) as max_entradas,
            MAX(intercepciones) as max_intercepciones,
            MAX(bloqueos) as max_bloqueos,
            MAX(despejes) as max_despejes,
            MAX(duelos_aereos_ganados) as max_duelos_aereos_ganados
        FROM estadistica_jugador
        WHERE estadistica_jugador.jugador = ? OR estadistica_jugador.jugador = ?;
    `, [jugador1, jugador2]);
    return rows[0];
    
}


export async function getMejoresGoles(temporada) {
    const [rows] = await pool.query(`
        SELECT 
            m.id_disparo,
            m.xg,
            j.url_imagen,
            j.nombre AS nombre_jugador,
            el.nombre AS equipo_local,
            el.url_imagen AS escudo_local,
            ev.nombre AS equipo_visitante,
            ev.url_imagen AS escudo_visitante,
            ep.goles_local,
            ep.goles_visitante
        FROM mapa_de_disparos m
        JOIN jugador j ON m.jugador = j.id_jugador
        JOIN partido p ON m.partido = p.id_partido
        JOIN equipo el ON p.equipo_local = el.id_equipo
        JOIN equipo ev ON p.equipo_visitante = ev.id_equipo
        JOIN estadisticas_partido ep ON p.id_partido = ep.partido
        WHERE m.xg IS NOT NULL and m.resultado = 'goal' and p.temporada = ?
        ORDER BY m.xg ASC
        LIMIT 5;

        `, [temporada])
    
    return rows
    
}

export async function getEstadisticasOfensivasEquipo(temporada) {
    const [rows] = await pool.query(`
        SELECT 
            e.id_equipo,
            e.url_imagen,
            e.nombre,
            ee.partidos_jugados,
            ee.goles_anotados,
            ee.disparos,
            ee.disparos_a_puerta,
            ee.disparos_a_puerta / ee.disparos * 100 as porcentaje_disparos_a_puerta,
            ee.xg,
            ee.disparos / ee.partidos_jugados as disparos_por_90
        FROM estadisticas_equipo ee 
        JOIN equipo e on ee.equipo = e.id_equipo
        WHERE ee.temporada = ?;
        `, [temporada])
        return rows;
    
} 


export async function getXgPorEquipo(equipo){
    const [rows] = await pool.query(`
        SELECT 
            p.id_partido,
            p.jornada,
            CASE 
                WHEN el.id_equipo = ? THEN ep.xg_local
                WHEN ev.id_equipo = ? THEN ep.xg_visitante
            END AS xg_equipo,
            CASE 
                WHEN el.id_equipo = ? THEN ep.xg_visitante
                WHEN ev.id_equipo = ? THEN ep.xg_local
            END AS xg_rival,
            el.nombre AS equipo_local,
            ev.nombre AS equipo_visitante
        FROM estadisticas_partido ep
        JOIN partido p ON ep.partido = p.id_partido
        JOIN equipo el ON p.equipo_local = el.id_equipo
        JOIN equipo ev ON p.equipo_visitante = ev.id_equipo
        WHERE el.id_equipo = ? OR ev.id_equipo = ?;

        `, [equipo, equipo, equipo, equipo, equipo, equipo])

        return rows;
}

export async function getMapaDeDisparosEquipo(equipo){
    const [rows] = await pool.query(`
        SELECT 
			j.nombre, 
            mp.pitch_x, 
            mp.pitch_y, 
            mp.goal_mouth_y,
            mp.goal_mouth_z,
            mp.xg, 
            mp.resultado, 
            p.id_partido,
            CASE
                WHEN el.id_equipo = ? THEN el.nombre
                WHEN ev.id_equipo = ? THEN ev.nombre
            END AS nombre_equipo 
        FROM mapa_de_disparos mp 
        JOIN partido p on mp.partido = p.id_partido 
        JOIN jugador j on mp.jugador = j.id_jugador
        JOIN equipo el ON p.equipo_local = el.id_equipo 
        JOIN equipo ev ON p.equipo_visitante = ev.id_equipo 
        WHERE (el.id_equipo = ? AND mp.es_local = 1) OR (ev.id_equipo = ? AND mp.es_local= 0);
        `, [equipo, equipo, equipo, equipo])
    return rows;
}

export async function getEvolucionEquipos(equipo) {
    const [rows] = await pool.query(`
        SELECT * FROM evolucion_posiciones where equipo = ?;
        `, [equipo])
    
        return rows;
    
}