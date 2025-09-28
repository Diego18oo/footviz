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
