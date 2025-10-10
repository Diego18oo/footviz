import mysql from 'mysql2/promise'

import dotenv from 'dotenv'
dotenv.config() 
 
const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME, // <--- El nombre correcto
  port: process.env.DB_PORT,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
};

// Si la variable de entorno del certificado SSL existe (en Vercel),
// añadimos la configuración SSL al objeto.
if (process.env.DB_SSL_CA) {
  dbConfig.ssl = {
    ca: process.env.DB_SSL_CA
  };
}

// Creamos el pool de conexiones con la configuración final
const pool = mysql.createPool(dbConfig);



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
            j.id_jugador,
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
            j.id_jugador, 
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
            j.id_jugador, 
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
        ORDER BY pe.id_plantilla DESC;

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
            j.id_jugador, 
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

export async function getPromediosStatsDeUnaLiga(temporada) {
    
    const [rows] = await pool.query(`
        SELECT 
            e.id_equipo,
            e.url_imagen,
            e.nombre,
            ee.partidos_jugados,
            ee.goles_anotados / ee.partidos_jugados as goles_por_partido,
            ee.disparos_a_puerta / ee.partidos_jugados as disparos_a_puerta_por_partido,
            ee.disparos / ee.partidos_jugados as disparos_por_partido,
            ee.faltas / ee.partidos_jugados as faltas_por_partido,
            ee.tiros_de_esquina / ee.partidos_jugados as tiros_de_esquina_por_partido
        FROM estadisticas_equipo ee 
        JOIN equipo e on ee.equipo = e.id_equipo
        WHERE ee.temporada = ?;
        `, [temporada])
        return rows;    
}

export async function getPartidos(temporada, jornada){
    const [rows] = await pool.query(`
        SELECT 
            p.id_partido,
            p.jornada, 
            p.fecha, 
            el.nombre as equipo_local, 
            ev.nombre as equipo_visitante, 
            el.url_imagen as escudo_local, 
            ev.url_imagen as escudo_visitante
        FROM partido p 
        JOIN equipo el on p.equipo_local = el.id_equipo
        JOIN equipo ev on p.equipo_visitante = ev.id_equipo
        WHERE temporada = ? and jornada = ? ORDER BY fecha ASC;
        `, [temporada, jornada])
    return rows;  
}

export async function getResultadoPartido(partido){
    const [rows] = await pool.query(`
        SELECT
	        goles_local, goles_visitante
        FROM estadisticas_partido WHERE partido = ?;
        `, [partido])
    return rows[0];  
}

export async function getInfoPrePartido(partido) {
    const [rows] = await pool.query(`
        SELECT
            p.id_partido,
            p.jornada,
            p.fecha,
            t.nombre AS nombre_temporada,
            el.id_equipo AS id_local,
            el.nombre AS local_nombre,
            el.url_imagen AS escudo_local,
            ev.id_equipo AS id_visitante,
            ev.nombre AS visitante_nombre,
            ev.url_imagen AS escudo_visitante,
            es.nombre AS estadio_nombre,
            p.momio_local,
            p.momio_empate,
            p.momio_visitante,
            a.nombre AS arbitro_nombre,
            (SELECT COUNT(*) FROM equipo WHERE liga = t.id_temporada) AS total_equipos_liga
        FROM
            partido p
        LEFT JOIN arbitro a ON p.arbitro = a.id_arbitro
        JOIN temporada t ON p.temporada = t.id_temporada
        JOIN estadio es ON p.estadio = es.id_estadio
        JOIN equipo el ON p.equipo_local = el.id_equipo
        JOIN equipo ev ON p.equipo_visitante = ev.id_equipo
        WHERE
            p.id_partido = ?;
        `, [partido])
    return rows[0]; 
    
}

export async function getPosiblesAlineaciones(partido) {
    const [rows] = await pool.query(`
        SELECT 
            formacion_local, 
            formacion_visitante,
            j1.nombre as nombre1, j1.dorsal as dorsal1, j1.url_imagen as img1,
            j2.nombre as nombre2, j2.dorsal as dorsal2 , j2.url_imagen as img2,
            j3.nombre as nombre3, j3.dorsal as dorsal3, j3.url_imagen as img3,
            j4.nombre as nombre4, j4.dorsal as dorsal4, j4.url_imagen as img4,
            j5.nombre as nombre5, j5.dorsal as dorsal5, j5.url_imagen as img5,
            j6.nombre as nombre6, j6.dorsal as dorsal6, j6.url_imagen as img6,
            j7.nombre as nombre7, j7.dorsal as dorsal7, j7.url_imagen as img7,
            j8.nombre as nombre8, j8.dorsal as dorsal8, j8.url_imagen as img8,
            j9.nombre as nombre9, j9.dorsal as drosal9, j9.url_imagen as img9,
            j10.nombre as nombre10, j10.dorsal as dorsal10, j10.url_imagen as img10,
            j11.nombre as nombre11, j11.dorsal as dorsal11, j11.url_imagen as img11,
            j12.nombre as nombre12, j12.dorsal as dorsal12, j12.url_imagen as img12,
            j13.nombre as nombre13, j13.dorsal as dorsal13, j13.url_imagen as img13,
            j14.nombre as nombre14, j14.dorsal as dorsal14, j14.url_imagen as img14,
            j15.nombre as nombre15, j15.dorsal as dorsal15, j15.url_imagen as img15,
            j16.nombre as nombre16, j16.dorsal as dorsal16, j16.url_imagen as img16,
            j17.nombre as nombre17, j17.dorsal as dorsal17, j17.url_imagen as img17,
            j18.nombre as nombre18, j18.dorsal as dorsal18, j18.url_imagen as img18,
            j19.nombre as nombre19, j19.dorsal as dorsal19, j19.url_imagen as img19,
            j20.nombre as nombre20, j20.dorsal as dorsal20, j20.url_imagen as img20,
            j21.nombre as nombre21, j21.dorsal as dorsal21, j21.url_imagen as img21,
            j22.nombre as nombre22, j22.dorsal as dorsal22, j22.url_imagen as img22
            
        FROM alineaciones al
        JOIN jugador j1 on al.jugador1 = j1.id_jugador
        JOIN jugador j2 on al.jugador2 = j2.id_jugador
        JOIN jugador j3 on al.jugador3 = j3.id_jugador
        JOIN jugador j4 on al.jugador4 = j4.id_jugador
        JOIN jugador j5 on al.jugador5 = j5.id_jugador
        JOIN jugador j6 on al.jugador6 = j6.id_jugador
        JOIN jugador j7 on al.jugador7 = j7.id_jugador
        JOIN jugador j8 on al.jugador8 = j8.id_jugador
        JOIN jugador j9 on al.jugador9 = j9.id_jugador
        JOIN jugador j10 on al.jugador10 = j10.id_jugador
        JOIN jugador j11 on al.jugador11 = j11.id_jugador
        JOIN jugador j12 on al.jugador12 = j12.id_jugador
        JOIN jugador j13 on al.jugador13 = j13.id_jugador
        JOIN jugador j14 on al.jugador14 = j14.id_jugador
        JOIN jugador j15 on al.jugador15 = j15.id_jugador
        JOIN jugador j16 on al.jugador16 = j16.id_jugador
        JOIN jugador j17 on al.jugador17 = j17.id_jugador
        JOIN jugador j18 on al.jugador18 = j18.id_jugador
        JOIN jugador j19 on al.jugador19 = j19.id_jugador
        JOIN jugador j20 on al.jugador20 = j20.id_jugador
        JOIN jugador j21 on al.jugador21 = j21.id_jugador
        JOIN jugador j22 on al.jugador22 = j22.id_jugador

        WHERE partido = ?;
        `, [partido])
    return rows[0]; 
     
}

export async function getUltimosEnfrentamientos(partido) {
    const [rows] = await pool.query(`
        SELECT 
            el.nombre as nombre_equipo_local, el.url_imagen as escudo_local,
            ue.goles_local, ue.goles_visitante,
            ev.nombre as nombre_equipo_visitante, ev.url_imagen as escudo_visitante,
            ue.fecha
        FROM ultimos_enfrentamientos ue
        JOIN equipo el on ue.equipo_local = el.id_equipo
        JOIN equipo ev on ue.equipo_visitante = ev.id_equipo
        WHERE partido = ?;
        `, [partido])
    return rows; 
    
}

export async function getEstadisticasEquipo(equipo) {
    const [rows] = await pool.query(`
        SELECT  
            e.nombre as nombre_equipo, e.url_imagen as img_equipo, ent.nombre as nombre_entrenador, ent.url_imagen as img_entrenador,
            ee.posicion, ee.puntos, ee.partidos_jugados, ee.goles_anotados, ee.goles_recibidos, ee.xg, ee.disparos, ee.faltas, ee.tarjetas_amarillas, ee.tarjetas_rojas
        FROM estadisticas_equipo ee 
        JOIN equipo e on ee.equipo = e.id_equipo
        JOIN entrenador ent on e.entrenador = ent.id_entrenador
        WHERE e.id_equipo = ?;
        `, [equipo])
    return rows; 
    
}


export async function getComparacionEvolucionEquipos(equipo1, equipo2) {
    const [rows] = await pool.query(`
        SELECT * FROM evolucion_posiciones where equipo = ? or equipo = ?;
        `, [equipo1, equipo2])
    
        return rows; 
    
} 

export async function getComparacionStatsEquipos(equipo1, equipo2) {
    const [rows] = await pool.query(`
        SELECT
            e.nombre as nombre_equipo, e.url_imagen as escudo_equipo, 
            ee.posicion, ee.puntos, ee.victorias, ee.partidos_jugados, ee.goles_anotados, ee.goles_recibidos, ee.xg, ee.disparos, ee.disparos_a_puerta, ee.faltas, ee.tarjetas_amarillas, ee.tarjetas_rojas, ee.tiros_de_esquina, ee.pases_completados
        FROM estadisticas_equipo ee
        JOIN equipo e ON ee.equipo = e.id_equipo
        WHERE equipo = ? or equipo = ?;
        `, [equipo1, equipo2])
    
        return rows; 
    
} 


export async function getInfoPostPartido(partido) {
    const [rows] = await pool.query(`
        SELECT
            p.id_partido,
            p.jornada,
            p.fecha,
            t.nombre AS nombre_temporada,
            el.id_equipo AS id_local,
            el.nombre AS local_nombre,
            el.url_imagen AS escudo_local,
            ev.id_equipo AS id_visitante,
            ev.nombre AS visitante_nombre,
            ev.url_imagen AS escudo_visitante,
            es.nombre AS estadio_nombre,
            p.momio_local,
            p.momio_empate,
            p.momio_visitante,
            p.momio_ganador, 
            a.nombre AS arbitro_nombre,
            ee.goles_local,
            ee.goles_visitante
            FROM estadisticas_partido ee
        JOIN partido p on ee.partido = p.id_partido
        LEFT JOIN arbitro a ON p.arbitro = a.id_arbitro
        JOIN temporada t ON p.temporada = t.id_temporada
        JOIN estadio es ON p.estadio = es.id_estadio
        JOIN equipo el ON p.equipo_local = el.id_equipo
        JOIN equipo ev ON p.equipo_visitante = ev.id_equipo
        
        WHERE
            p.id_partido = ?;
        `, [partido])
    return rows[0]; 
    
}

export async function getEstadisticasPartido(partido) {
    const [rows] = await pool.query(`
        SELECT 
            p.id_partido,
            ee.goles_local,
            ee.goles_visitante,
            ee.tarjetas_amarillas_local,
            ee.tarjetas_amarillas_visitante,
            ee.tarjetas_rojas_local,
            ee.tarjetas_rojas_visitante,
            ee.posesion_local,
            ee.posesion_visitante,
            ee.pases_local,
            ee.pases_visitante,
            ee.tiros_de_esquina_local,
            ee.tiros_de_esquina_visitante,
            ee.disparos_local,
            ee.disparos_visitante,
            ee.disparos_a_puerta_local,
            ee.disparos_a_puerta_visitante,
            ee.faltas_local,
            ee.faltas_visitante,
            ee.entradas_local,
            ee.entradas_visitante,
            ee.xg_local,
            ee.xg_visitante


        FROM estadisticas_partido ee
        LEFT JOIN partido p on ee.partido = p.id_partido
        WHERE
            p.id_partido = ?;
        `, [partido])
    return rows[0]; 
    
}

export async function getMapaDeDisparosPartido(partido) {
    const [rows] = await pool.query(`
        SELECT 
            j.id_jugador, j.nombre, 
            p.id_partido,
            mp.pitch_x,
            mp.pitch_y,
            mp.xg,
            mp.minuto,
            mp.goal_mouth_y,
            mp.goal_mouth_z,
            mp.es_local,
            mp.resultado,
            mp.parte_del_cuerpo
            
        FROM mapa_de_disparos mp
        JOIN partido p on mp.partido = p.id_partido
        JOIN jugador j on mp.jugador = j.id_jugador
        WHERE
            p.id_partido = ?;
        `, [partido])
    return rows; 
    
}

export async function getMapaDeCalorJugador(jugador) {
    const [rows] = await pool.query(`
        SELECT 
            j.id_jugador,
            j.nombre,
            mp.x,
            mp.y  
        FROM mapa_de_calor mp
        JOIN jugador j on mp.jugador = j.id_jugador
        WHERE j.id_jugador = ?;
        `, [jugador])
    return rows; 
    
}

export async function getMapaDeDisparosJugador(jugador) {
    const [rows] = await pool.query(`
        SELECT 
            j.id_jugador,
            j.nombre,
            md.partido,
            md.pitch_x,
            md.pitch_y,
            md.xg,
            md.minuto,
            md.goal_mouth_y,
            md.goal_mouth_z,
            md.resultado,
            md.parte_del_cuerpo
            
            
        FROM mapa_de_disparos md
        JOIN jugador j on md.jugador = j.id_jugador
        WHERE j.id_jugador = ?;
        `, [jugador])
    return rows; 
    
}


export async function getPercentilesJugador(jugador) {
    const [rows] = await pool.query(`
        WITH EstadisticasPorJugador AS (
            -- Paso 1: Esto no cambia. Sigue siendo la parte pesada de agregación.
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
            JOIN jugador j ON ejp.jugador = j.id_jugador
            JOIN estadistica_jugador sj ON j.id_jugador = sj.jugador
            LEFT JOIN (
                SELECT pe.jugador, MAX(pe.id_plantilla) as max_id FROM plantilla_equipos pe GROUP BY pe.jugador
            ) AS ultima_plantilla ON j.id_jugador = ultima_plantilla.jugador
            LEFT JOIN plantilla_equipos pe ON ultima_plantilla.max_id = pe.id_plantilla
            LEFT JOIN equipo e ON pe.equipo = e.id_equipo
            GROUP BY 
                j.id_jugador, e.nombre, e.url_imagen,
                sj.disparos, sj.asistencias, sj.acciones_creadas, sj.pases, sj.porcentaje_de_efectividad_de_pases,
                sj.pases_progresivos, sj.acarreos_progresivos, sj.regates_efectivos, sj.toques_de_balon,
                sj.entradas, sj.intercepciones, sj.bloqueos, sj.despejes, sj.duelos_aereos_ganados
        ),
        MaximosGlobales AS (
            -- Paso 2: NUEVO. Calculamos TODOS los máximos en una sola pasada.
            -- Esto nos dará una única fila con todos los valores máximos.
            SELECT
                MAX(total_goles) AS max_goles,
                MAX(disparos) AS max_disparos,
                MAX(asistencias) AS max_asistencias,
                MAX(pases) AS max_pases,
                MAX(pases_progresivos) AS max_pases_progresivos,
                MAX(acarreos_progresivos) AS max_acarreos_progresivos,
                MAX(regates_efectivos) AS max_regates_efectivos,
                MAX(toques_de_balon) AS max_toques_de_balon,
                MAX(entradas) AS max_entradas,
                MAX(intercepciones) AS max_intercepciones,
                MAX(bloqueos) AS max_bloqueos,
                MAX(despejes) AS max_despejes,
                MAX(duelos_aereos_ganados) AS max_duelos_aereos
            FROM
                EstadisticasPorJugador
        )
        -- Paso 3: Unimos las estadísticas de TODOS los jugadores con la fila de MÁXIMOS
        -- y luego filtramos por el jugador que queremos.
        SELECT
            epj.id_jugador,
            epj.imagen_jugador,
            epj.nombre_jugador,
            epj.nombre_equipo,
            epj.imagen_equipo,
            
            -- Los cálculos ahora son una simple división contra la tabla de máximos
            (CAST(epj.total_goles AS DECIMAL) * 100.0 / NULLIF(mg.max_goles, 0)) AS percentil_goles,
            (CAST(epj.disparos AS DECIMAL) * 100.0 / NULLIF(mg.max_disparos, 0)) AS percentil_disparos,
            (CAST(epj.asistencias AS DECIMAL) * 100.0 / NULLIF(mg.max_asistencias, 0)) AS percentil_asistencias,
            (CAST(epj.pases AS DECIMAL) * 100.0 / NULLIF(mg.max_pases, 0)) AS percentil_pases,
            (CAST(epj.pases_progresivos AS DECIMAL) * 100.0 / NULLIF(mg.max_pases_progresivos, 0)) AS percentil_pases_progresivos,
            (CAST(epj.acarreos_progresivos AS DECIMAL) * 100.0 / NULLIF(mg.max_acarreos_progresivos, 0)) AS percentil_acarreos_progresivos,
            (CAST(epj.regates_efectivos AS DECIMAL) * 100.0 / NULLIF(mg.max_regates_efectivos, 0)) AS percentil_regates_efectivos,
            (CAST(epj.toques_de_balon AS DECIMAL) * 100.0 / NULLIF(mg.max_toques_de_balon, 0)) AS percentil_toques_de_balon,
            (CAST(epj.entradas AS DECIMAL) * 100.0 / NULLIF(mg.max_entradas, 0)) AS percentil_entradas,
            (CAST(epj.intercepciones AS DECIMAL) * 100.0 / NULLIF(mg.max_intercepciones, 0)) AS percentil_intercepciones,
            (CAST(epj.bloqueos AS DECIMAL) * 100.0 / NULLIF(mg.max_bloqueos, 0)) AS percentil_bloqueos,
            (CAST(epj.despejes AS DECIMAL) * 100.0 / NULLIF(mg.max_despejes, 0)) AS percentil_despejes,
            (CAST(epj.duelos_aereos_ganados AS DECIMAL) * 100.0 / NULLIF(mg.max_duelos_aereos, 0)) AS percentil_duelos_aereos
        FROM
            EstadisticasPorJugador AS epj
        CROSS JOIN -- Unimos cada jugador con la única fila de máximos
            MaximosGlobales AS mg
        WHERE
            epj.id_jugador = ?; -- Filtramos al final
        `, [jugador])
    return rows; 
    
}

export async function getPercentilesPortero(jugador) {
    const [rows] = await pool.query(`
        WITH EstadisticasPorJugador AS (
            -- Paso 1: Esto no cambia. Sigue siendo la parte pesada de agregación.
            SELECT
                j.id_jugador,
                j.url_imagen AS imagen_jugador,
                j.nombre AS nombre_jugador,
                e.nombre AS nombre_equipo,
                e.url_imagen AS imagen_equipo,
                sj.atajadas,
     			sj.porterias_imbatidas,
     			sj.penales_atajados,
     			sj.porcentaje_de_atajadas,
     			sj.acciones_fuera_del_area,
     			sj.centros_interceptados
            FROM estadistica_jugador_portero ejp
            JOIN jugador j ON ejp.jugador = j.id_jugador
            JOIN estadistica_portero sj ON j.id_jugador = sj.jugador
            LEFT JOIN (
                SELECT pe.jugador, MAX(pe.id_plantilla) as max_id FROM plantilla_equipos pe GROUP BY pe.jugador
            ) AS ultima_plantilla ON j.id_jugador = ultima_plantilla.jugador
            LEFT JOIN plantilla_equipos pe ON ultima_plantilla.max_id = pe.id_plantilla
            LEFT JOIN equipo e ON pe.equipo = e.id_equipo
            GROUP BY 
                j.id_jugador, e.nombre, e.url_imagen,sj.atajadas,
     			sj.porterias_imbatidas,
     			sj.penales_atajados,
     			sj.porcentaje_de_atajadas,
     			sj.acciones_fuera_del_area,
     			sj.centros_interceptados
        ),
        MaximosGlobales AS (
            -- Paso 2: NUEVO. Calculamos TODOS los máximos en una sola pasada.
            -- Esto nos dará una única fila con todos los valores máximos.
            SELECT
                MAX(atajadas) AS max_atajadas,
                MAX(porterias_imbatidas) AS max_porterias_imbatidas,
                MAX(penales_atajados) AS max_penales_atajados,
                MAX(porcentaje_de_atajadas) AS max_porcentaje_de_atajadas,
                MAX(acciones_fuera_del_area) AS max_acciones_fuera_del_area,
                MAX(centros_interceptados) AS max_centros_interceptados
            FROM
                EstadisticasPorJugador
        )
        -- Paso 3: Unimos las estadísticas de TODOS los jugadores con la fila de MÁXIMOS
        -- y luego filtramos por el jugador que queremos.
        SELECT
            epj.id_jugador,
            epj.imagen_jugador,
            epj.nombre_jugador,
            epj.nombre_equipo,
            epj.imagen_equipo,
            
            -- Los cálculos ahora son una simple división contra la tabla de máximos
            (CAST(epj.atajadas AS DECIMAL) * 100.0 / NULLIF(mg.max_atajadas, 0)) AS percentil_atajadas,
            (CAST(epj.porterias_imbatidas AS DECIMAL) * 100.0 / NULLIF(mg.max_porterias_imbatidas, 0)) AS percentil_porterias_imbatidas,
            (CAST(epj.penales_atajados AS DECIMAL) * 100.0 / NULLIF(mg.max_penales_atajados, 0)) AS percentil_penales_atajados,
            (CAST(epj.porcentaje_de_atajadas AS DECIMAL) * 100.0 / NULLIF(mg.max_porcentaje_de_atajadas, 0)) AS percentil_porcentaje_de_atajadas,
            (CAST(epj.acciones_fuera_del_area AS DECIMAL) * 100.0 / NULLIF(mg.max_acciones_fuera_del_area, 0)) AS percentil_acciones_fuera_del_area,
            (CAST(epj.centros_interceptados AS DECIMAL) * 100.0 / NULLIF(mg.max_centros_interceptados, 0)) AS percentil_centros_interceptados
        FROM
            EstadisticasPorJugador AS epj
        CROSS JOIN -- Unimos cada jugador con la única fila de máximos
            MaximosGlobales AS mg
        WHERE
            epj.id_jugador = ?;
        `, [jugador])
    return rows; 
    
}

export async function getUltimosPartidosJugador(jugador) {
    const [rows] = await pool.query(`
        SELECT 
            p.fecha,
            ejp.minutos,
            ejp.rating,
            j.id_jugador,
            j.nombre AS nombre_jugador,
            mi_equipo.id_equipo as id_equipo,
            mi_equipo.nombre as nombre_mi_equipo,
            mi_equipo.url_imagen as imagen_equipo,
            p.id_partido,

            -- Información del RIVAL
            rival.nombre AS nombre_rival,
            rival.url_imagen AS imagen_rival,
            rival.id_equipo as id_rival

        FROM 
            estadistica_jugador_partido ejp 
        JOIN 
            jugador j ON ejp.jugador = j.id_jugador
        JOIN 
            partido p ON ejp.partido = p.id_partido

        -- === EL CAMBIO CLAVE ESTÁ AQUÍ ===
        -- Unimos con plantilla_equipos para descubrir para cuál de los dos equipos
        -- (local o visitante) jugó nuestro jugador.
        JOIN 
            plantilla_equipos pe ON pe.jugador = j.id_jugador 
            AND (pe.equipo = p.equipo_local OR pe.equipo = p.equipo_visitante)

        
            
        -- Unimos la tabla equipo para obtener el nombre del equipo del jugador.
        JOIN 
            equipo mi_equipo ON pe.equipo = mi_equipo.id_equipo

        -- Unimos la tabla equipo OTRA VEZ, pero para el RIVAL, usando el CASE.
        JOIN 
            equipo rival ON rival.id_equipo = (
                CASE 
                    WHEN pe.equipo = p.equipo_local THEN p.equipo_visitante
                    ELSE p.equipo_local
                END
            )

        WHERE 
            j.id_jugador = ?

        -- No olvides ordenar para obtener los "últimos" partidos
        ORDER BY 
            p.fecha DESC 
            
        LIMIT 5;
        `, [jugador])
    return rows; 
    
}

export async function getInfoJugador(jugador) {
    const [rows] = await pool.query(`
        SELECT 
            j.id_jugador, j.nombre as nombre_jugador , j.dorsal, j.fec_nac, j.posicion, j.valor_mercado, j.altura, j.pie_preferido, j.url_imagen as img_jugador, p.id_pais, p.nombre as nombre_pais, p.codigo_iso, e.id_equipo, e.nombre as nombre_equipo, e.url_imagen as img_equipo
        FROM plantilla_equipos pe
        JOIN jugador j on pe.jugador = j.id_jugador
        JOIN equipo e on pe.equipo = e.id_equipo
        JOIN pais p on j.pais = p.id_pais
        WHERE j.id_jugador = ?
        ORDER BY pe.id_plantilla DESC;
        `, [jugador])
    return rows[0]; 
    
}

export async function getUltimosPartidosPortero(jugador) {
    const [rows] = await pool.query(`
        SELECT 
            p.fecha,
            ejp.minutos,
            ejp.rating,
            j.id_jugador,
            j.nombre AS nombre_jugador,
            mi_equipo.id_equipo as id_equipo,
            mi_equipo.nombre as nombre_mi_equipo,
            mi_equipo.url_imagen as imagen_equipo,
            p.id_partido,

            -- Información del RIVAL
            rival.nombre AS nombre_rival,
            rival.url_imagen AS imagen_rival,
            rival.id_equipo as id_rival

        FROM 
            estadistica_jugador_portero ejp 
        JOIN 
            jugador j ON ejp.jugador = j.id_jugador
        JOIN 
            partido p ON ejp.partido = p.id_partido

        -- === EL CAMBIO CLAVE ESTÁ AQUÍ ===
        -- Unimos con plantilla_equipos para descubrir para cuál de los dos equipos
        -- (local o visitante) jugó nuestro jugador.
        JOIN 
            plantilla_equipos pe ON pe.jugador = j.id_jugador 
            AND (pe.equipo = p.equipo_local OR pe.equipo = p.equipo_visitante)

        
            
        -- Unimos la tabla equipo para obtener el nombre del equipo del jugador.
        JOIN 
            equipo mi_equipo ON pe.equipo = mi_equipo.id_equipo

        -- Unimos la tabla equipo OTRA VEZ, pero para el RIVAL, usando el CASE.
        JOIN 
            equipo rival ON rival.id_equipo = (
                CASE 
                    WHEN pe.equipo = p.equipo_local THEN p.equipo_visitante
                    ELSE p.equipo_local
                END
            )

        WHERE 
            j.id_jugador = ?

        -- No olvides ordenar para obtener los "últimos" partidos
        ORDER BY 
            p.fecha DESC 
            
        LIMIT 5;
        `, [jugador])
    return rows; 
    
}


export async function getEstadisticasPortero(jugador) {
    const [rows] = await pool.query(`
        SELECT
            j.id_jugador, 
            j.url_imagen AS imagen_jugador,
            j.nombre AS nombre_jugador,
            e.nombre AS nombre_equipo,
            e.url_imagen AS imagen_equipo,
            sj.goles_recibidos,
            sj.goles_recibidos_por_90,
            sj.tiros_a_puerta_recibidos,
            sj.atajadas,
            sj.porcentaje_de_atajadas,
            sj.porterias_imbatidas,
            sj.penales_en_contra,
            sj.penales_atajados,
            sj.pases,
            sj.centros_recibidos,
            sj.centros_interceptados,
            sj.acciones_fuera_del_area
        FROM estadistica_jugador_portero ejp
        JOIN partido p ON ejp.partido = p.id_partido
        JOIN jugador j ON ejp.jugador = j.id_jugador
        JOIN plantilla_equipos pe ON j.id_jugador = pe.jugador
        JOIN equipo e ON pe.equipo = e.id_equipo
        JOIN estadistica_portero sj ON j.id_jugador = sj.jugador
        WHERE j.id_jugador = 1814
        GROUP BY 
            j.id_jugador,
            ejp.jugador, 
            j.url_imagen, 
            j.nombre, 
            e.nombre, 
            e.url_imagen, 
            sj.goles_recibidos,
            sj.goles_recibidos_por_90,
            sj.tiros_a_puerta_recibidos,
            sj.atajadas,
            sj.porcentaje_de_atajadas,
            sj.porterias_imbatidas,
            sj.penales_en_contra,
            sj.penales_atajados,
            sj.pases,
            sj.centros_recibidos,
            sj.centros_interceptados,
            sj.acciones_fuera_del_area
        ORDER BY pe.id_plantilla DESC;

        `, [jugador])
    return rows 
} 

export async function getInfoClub(club) {
    const [rows] = await pool.query(`
        SELECT
            e.id_equipo, e.nombre as nombre_equipo, e.url_imagen as img_equipo, est.id_estadio, est.nombre as nombre_estadio, ent.id_entrenador, ent.nombre as nombre_entrenador, ent.url_imagen as img_entrenador
        FROM equipo e 
        JOIN estadio est on e.estadio = est.id_estadio
        JOIN entrenador ent on e.entrenador = ent.id_entrenador
        WHERE e.id_equipo = ?;

        `, [club])
    return rows;
} 

export async function getUltimosPartidosClub(club) {
    const [rows] = await pool.query(`
        SELECT 
            p.id_partido, p.jornada, p.fecha, el.id_equipo as id_local, el.nombre as nombre_local, el.url_imagen as img_local, ev.id_equipo as id_visitante, ev.nombre as nombre_visitante, ev.url_imagen as img_visitante,
            ep.goles_local, ep.goles_visitante
        FROM estadisticas_partido ep
        JOIN partido p on ep.partido = p.id_partido
        JOIN equipo el on p.equipo_local =  el.id_equipo
        JOIN equipo ev on p.equipo_visitante = ev.id_equipo
        WHERE el.id_equipo = ? or ev.id_equipo = ?
                
        ORDER BY p.fecha DESC
        LIMIT 5 ;



        `, [club, club])
    return rows;
}

export async function getAlineacionClub(club) {
    const [rows] = await pool.query(`
        SELECT
            al.formacion_local,
            j1.id_jugador as id_jugador1, j1.nombre as nombre_jugador1, j1.url_imagen as img_jugador1,
            j2.id_jugador as id_jugador2, j2.nombre as nombre_jugador2, j2.url_imagen as img_jugador2,
            j3.id_jugador as id_jugador3, j3.nombre as nombre_jugador3, j3.url_imagen as img_jugador3,
            j4.id_jugador as id_jugador4, j4.nombre as nombre_jugador4, j4.url_imagen as img_jugador4,
            j5.id_jugador as id_jugador5, j5.nombre as nombre_jugador5, j5.url_imagen as img_jugador5,
            j6.id_jugador as id_jugador6, j6.nombre as nombre_jugador6, j6.url_imagen as img_jugador6,
            j7.id_jugador as id_jugador7, j7.nombre as nombre_jugador7, j7.url_imagen as img_jugador7,
            j8.id_jugador as id_jugador8, j8.nombre as nombre_jugador8, j8.url_imagen as img_jugador8,
            j9.id_jugador as id_jugador9, j9.nombre as nombre_jugador9, j9.url_imagen as img_jugador9,
            j10.id_jugador as id_jugador10, j10.nombre as nombre_jugador10, j10.url_imagen as img_jugador10,
            j11.id_jugador as id_jugador11, j11.nombre as nombre_jugador11, j11.url_imagen as img_jugador11
        FROM alineaciones al

        JOIN jugador j1 on al.jugador1 = j1.id_jugador
        JOIN jugador j2 on al.jugador2 = j2.id_jugador
        JOIN jugador j3 on al.jugador3 = j3.id_jugador
        JOIN jugador j4 on al.jugador4 = j4.id_jugador
        JOIN jugador j5 on al.jugador5 = j5.id_jugador
        JOIN jugador j6 on al.jugador6 = j6.id_jugador
        JOIN jugador j7 on al.jugador7 = j7.id_jugador
        JOIN jugador j8 on al.jugador8 = j8.id_jugador
        JOIN jugador j9 on al.jugador9 = j9.id_jugador
        JOIN jugador j10 on al.jugador10 = j10.id_jugador
        JOIN jugador j11 on al.jugador11 = j11.id_jugador

        JOIN partido p on al.partido = p.id_partido
        JOIN equipo e on p.equipo_local = e.id_equipo
        WHERE e.id_equipo = ?
        ORDER BY p.fecha DESC
        LIMIT 1;




        `, [club])
    return rows;
}


export async function getPlantillaClub(club) {
    const [rows] = await pool.query(`
        
        WITH UltimoEquipoPorJugador AS (
            SELECT
                pe.jugador,
                pe.equipo,
                -- Calculamos el registro más reciente para CADA jugador en TODA la tabla
                ROW_NUMBER() OVER(PARTITION BY pe.jugador ORDER BY pe.id_plantilla DESC) AS rn
            FROM
                plantilla_equipos pe
            -- IMPORTANTE: Quitamos el "WHERE pe.equipo = 15" de aquí
            )
            SELECT
            j.id_jugador,
            j.nombre,
            j.dorsal,
            j.fec_nac,
            j.posicion,
            j.valor_mercado,
            j.url_imagen
            FROM
            jugador j
            JOIN
            UltimoEquipoPorJugador uepj ON j.id_jugador = uepj.jugador
            WHERE
            -- Aplicamos los dos filtros AL FINAL:
            -- 1. Queremos solo el registro más reciente de cada jugador (el que tiene el número 1)
            uepj.rn = 1
            -- 2. Y de esos, solo nos interesan los que actualmente pertenecen al equipo 15
            AND uepj.equipo = ?;



        `, [club])
    return rows;
}
