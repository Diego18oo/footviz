
import dotenv from 'dotenv'
dotenv.config()
import mysql from 'mysql2/promise'


  
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
    ca: process.env.DB_SSL_CA,
    rejectUnauthorized: false // <-- AÑADE ESTA LÍNEA
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
        ORDER BY MAX(pe.id_plantilla) DESC;

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
            j1.id_jugador as id1, j1.nombre as nombre1, j1.dorsal as dorsal1, j1.url_imagen as img1,
            j2.id_jugador as id2, j2.nombre as nombre2, j2.dorsal as dorsal2 , j2.url_imagen as img2,
            j3.id_jugador as id3, j3.nombre as nombre3, j3.dorsal as dorsal3, j3.url_imagen as img3,
            j4.id_jugador as id4, j4.nombre as nombre4, j4.dorsal as dorsal4, j4.url_imagen as img4,
            j5.id_jugador as id5, j5.nombre as nombre5, j5.dorsal as dorsal5, j5.url_imagen as img5,
            j6.id_jugador as id6, j6.nombre as nombre6, j6.dorsal as dorsal6, j6.url_imagen as img6,
            j7.id_jugador as id7, j7.nombre as nombre7, j7.dorsal as dorsal7, j7.url_imagen as img7,
            j8.id_jugador as id8, j8.nombre as nombre8, j8.dorsal as dorsal8, j8.url_imagen as img8,
            j9.id_jugador as id9, j9.nombre as nombre9, j9.dorsal as drosal9, j9.url_imagen as img9,
            j10.id_jugador as id10, j10.nombre as nombre10, j10.dorsal as dorsal10, j10.url_imagen as img10,
            j11.id_jugador as id11, j11.nombre as nombre11, j11.dorsal as dorsal11, j11.url_imagen as img11,
            j12.id_jugador as id12, j12.nombre as nombre12, j12.dorsal as dorsal12, j12.url_imagen as img12,
            j13.id_jugador as id13, j13.nombre as nombre13, j13.dorsal as dorsal13, j13.url_imagen as img13,
            j14.id_jugador as id14, j14.nombre as nombre14, j14.dorsal as dorsal14, j14.url_imagen as img14,
            j15.id_jugador as id15, j15.nombre as nombre15, j15.dorsal as dorsal15, j15.url_imagen as img15,
            j16.id_jugador as id16, j16.nombre as nombre16, j16.dorsal as dorsal16, j16.url_imagen as img16,
            j17.id_jugador as id17, j17.nombre as nombre17, j17.dorsal as dorsal17, j17.url_imagen as img17,
            j18.id_jugador as id18, j18.nombre as nombre18, j18.dorsal as dorsal18, j18.url_imagen as img18,
            j19.id_jugador as id19, j19.nombre as nombre19, j19.dorsal as dorsal19, j19.url_imagen as img19,
            j20.id_jugador as id20, j20.nombre as nombre20, j20.dorsal as dorsal20, j20.url_imagen as img20,
            j21.id_jugador as id21, j21.nombre as nombre21, j21.dorsal as dorsal21, j21.url_imagen as img21,
            j22.id_jugador as id22, j22.nombre as nombre22, j22.dorsal as dorsal22, j22.url_imagen as img22
            
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


export async function getTodosLosEquipos() {
    const [rows] = await pool.query(`
    SELECT id_equipo, nombre, url_imagen FROM equipo;
        `)
    return rows;
    
}

export async function getTodosLosPaises() {
    const [rows] = await pool.query(`
    SELECT id_pais, nombre FROM pais;
        `)
    return rows;
    
}


export async function crearUsuario(datos) {
  const { username, correo, hashed_password, fecha_nacimiento, pais_id, equipo_favorito_id } = datos;
  
  const [result] = await pool.query(`
    INSERT INTO usuario (username, email, hashed_password, fecha_nacimiento, pais_id, equipo_favorito_id)
    VALUES (?, ?, ?, ?, ?, ?)
  `, [username, correo, hashed_password, fecha_nacimiento, pais_id, equipo_favorito_id]);

  return { id: result.insertId };
}

// ✅ Función para las validaciones (SELECT)
export async function buscarUsuarioPorUsername(username) {
  const [rows] = await pool.query('SELECT id_usuario FROM usuario WHERE username = ?', [username]);
  return rows[0];
}

// ✅ También necesitarás una para el correo, etc.
export async function buscarUsuarioPorEmail(email) {
    const [rows] = await pool.query('SELECT id_usuario FROM usuario WHERE email = ?', [email]);
    return rows[0];
}


export async function findUserByEmail(email) {
  // Seleccionamos el id y la contraseña hasheada
  const [rows] = await pool.query(
    'SELECT id_usuario, hashed_password FROM usuario WHERE email = ?',
    [email]
  );
  return rows[0]; // Devuelve el usuario si existe, o undefined si no
}


export async function getUsuarioData(id_usuario){
    const [rows] = await pool.query(
    'SELECT * from usuario where id_usuario = ?',
    [id_usuario]
  );
  return rows[0];

}


export async function getEquipoFantasyUsuario(id_usuario){
    const [rows] = await pool.query(
    'SELECT * from equipo_fantasy where id_usuario = ?',
    [id_usuario]
  );
  return rows[0];

}


export async function getJugadoresFantasy(){
    const [rows] = await pool.query(`
        -- Usamos una CTE (Common Table Expression) para encontrar la última asignación de equipo
        WITH LatestTeamAssignment AS (
            SELECT
                pe.jugador,
                pe.equipo,
                pe.temporada,
                -- Asignamos un número de fila a cada registro de equipo para un jugador/temporada,
                -- ordenando por id_plantilla DESC (el más alto obtiene el #1)
                ROW_NUMBER() OVER(PARTITION BY pe.jugador, pe.temporada ORDER BY pe.id_plantilla DESC) as rn
            FROM
                plantilla_equipos pe
            -- No necesitamos filtrar por temporada aquí todavía
        )
        SELECT
            vj.id_jugador,
            j.nombre AS nombre_jugador,
            j.posicion,
            j.url_imagen AS url_imagen_jugador,
            vj.popularidad,
            vj.valor_actual,
            COALESCE(SUM(pjj.puntos_fantasy), 0) AS total_puntos_fantasy,
            -- Columnas del equipo (ahora del equipo más reciente)
            e.id_equipo,
            e.nombre AS nombre_equipo,
            e.url_imagen AS img_equipo -- Añadí la URL de la imagen del equipo también
        FROM
            valor_jugador_fantasy vj
        JOIN
            jugador j ON vj.id_jugador = j.id_jugador
        -- 👇 Unimos con nuestra CTE para obtener SOLO la última asignación de equipo 👇
        JOIN
            LatestTeamAssignment lta ON j.id_jugador = lta.jugador 
        -- 👇 Unimos con la tabla equipo usando el ID de la CTE 👇
        JOIN
            equipo e ON lta.equipo = e.id_equipo
        -- 👇 Mantenemos los LEFT JOINs para los puntos 👇
        LEFT JOIN
            puntos_jugador_jornada pjj ON vj.id_jugador = pjj.id_jugador
        LEFT JOIN
            partido p ON pjj.id_partido = p.id_partido -- Aseguramos que los puntos sean de la misma temporada
        WHERE
            vj.valor_actual > 0
            -- 👇 Filtramos por la temporada activa del fantasy 👇
            -- 👇 ¡LA MAGIA! Solo seleccionamos el registro de equipo más reciente (rn=1) 👇
            AND lta.rn = 1
        GROUP BY
            -- Agrupamos por todas las columnas no agregadas
            vj.id_jugador,
            j.nombre,
            j.posicion,
            j.url_imagen,
            vj.popularidad,
            vj.valor_actual,
            e.id_equipo,
            e.nombre,
            e.url_imagen -- Añadimos la URL del equipo al GROUP BY
        ORDER BY
            total_puntos_fantasy DESC;`
  );
  return rows;

}


export async function crearEquipoFantasyCompleto(id_usuario, nombreEquipo, presupuestoRestante, plantillaJugadores) {
    const conn = await pool.getConnection(); // Obtenemos una conexión del pool
    await conn.beginTransaction(); // Iniciamos la transacción

    try {
        // 1. Insertar el equipo principal en 'equipo_fantasy'
        // (Asumimos que id_temporada = 1, cámbialo si es necesario)
        
        const [equipoResult] = await conn.query(
            `INSERT INTO equipo_fantasy (id_usuario, nombre_equipo, presupuesto_restante)
             VALUES (?, ?, ?)`,
            [id_usuario,  nombreEquipo, presupuestoRestante]
        );

        const newTeamId = equipoResult.insertId; // Obtenemos el ID del equipo que acabamos de crear

        // 2. Obtener los precios de los jugadores (¡seguridad!)
        // No confiamos en el precio del cliente, lo buscamos en la BD
        const placeholders = plantillaJugadores.map(() => '?').join(','); // Crea "(?,?,?,...)"
        const [jugadoresConPrecio] = await conn.query(
            `SELECT id_jugador, valor_actual FROM valor_jugador_fantasy 
             WHERE id_jugador IN (${placeholders})`,
            [...plantillaJugadores]
        );

        if (jugadoresConPrecio.length !== 15) {
            throw new Error("No se encontraron los datos de precio para todos los jugadores.");
        }

        // 3. Preparar el INSERT masivo para 'plantilla_fantasy'
        const plantillaValues = jugadoresConPrecio.map((jugador, index) => {
            const id_jugador = jugador.id_jugador;
            const precio_compra = jugador.valor_actual;
            
            // Lógica para titulares y capitán:
            
            // ✅ Asigna 1 (titular) a los primeros 11 jugadores (índice 0 al 10)
            //    y 0 (suplente) a los últimos 4 (índice 11 al 14).
            const es_titular = (index < 11) ? 1 : 0; 
            
            // ✅ Asigna 1 (capitán) SOLO al primer jugador (índice 0)
            //    y 0 (no capitán) a todos los demás.
            const es_capitan = (index === 0) ? 1 : 0; 

            // El array que se insertará para este jugador
            return [newTeamId, id_jugador, precio_compra, es_titular, es_capitan];
        });

        // 4. Insertar los 15 jugadores en la plantilla
        await conn.query(
            `INSERT INTO plantilla_fantasy (id_equipo_fantasy, id_jugador, precio_compra, es_titular, es_capitan)
             VALUES ?`,
            [plantillaValues] // [ [1, 101, 8.5, 1], [1, 102, 5.5, 1], ... ]
        );

        // 5. ¡Éxito! Confirmamos todos los cambios
        await conn.commit();
        conn.release(); // Devolvemos la conexión al pool
        
        return { id_equipo_fantasy: newTeamId };

    } catch (error) {
        // 6. ¡Error! Deshacemos todos los cambios
        await conn.rollback();
        conn.release(); // Devolvemos la conexión al pool
        console.error("Error en la transacción de crear equipo:", error);
        throw error; // Lanzamos el error para que app.js lo atrape
    }
}



export async function getPlantillaFantasy(id_usuario){
    const [rows] = await pool.query(`
        -- CTE 1: Encuentra la asignación de equipo y temporada MÁS RECIENTE de cada jugador
        WITH LatestPlayerTeam AS (
            SELECT
                pe.jugador,
                pe.equipo,
                pe.temporada,
                -- Asigna el ranking #1 al registro de plantilla más reciente
                ROW_NUMBER() OVER(PARTITION BY pe.jugador ORDER BY pe.id_plantilla DESC) as rn
            FROM
                plantilla_equipos pe
        ),
        -- CTE 2: Encuentra el ID del último partido completado (hasta hoy) para CADA combo de equipo/temporada
        LatestTeamMatch AS (
            SELECT
                team_id,
                temporada_id,
                match_id,
                -- Asigna el ranking #1 al partido más reciente de cada equipo EN ESA TEMPORADA
                ROW_NUMBER() OVER(PARTITION BY team_id, temporada_id ORDER BY match_fecha DESC, match_id DESC) as rn
            FROM (
                -- Partidos como local
                SELECT p.equipo_local as team_id, p.temporada as temporada_id, p.id_partido as match_id, p.fecha as match_fecha
                FROM partido p
                WHERE p.fecha <= CURDATE() -- Solo partidos completados
                UNION ALL
                -- Partidos como visitante
                SELECT p.equipo_visitante as team_id, p.temporada as temporada_id, p.id_partido as match_id, p.fecha as match_fecha
                FROM partido p
                WHERE p.fecha <= CURDATE()
            ) AS all_team_matches
        )
        -- Consulta Principal
        SELECT
            pf.id_jugador,
            pf.precio_compra,
            pf.es_titular,
            pf.es_capitan,
            j.nombre AS nombre_jugador,
            j.posicion,
            j.url_imagen AS url_imagen_jugador,
            e.id_equipo,
            e.nombre AS nombre_equipo,
            e.url_imagen AS img_equipo,
            -- Volvemos a usar COALESCE. Si sale 0, es porque el LEFT JOIN no encontró puntos
            -- para ESE PARTIDO en específico (porque el jugador no jugó)
            COALESCE(pjj.puntos_fantasy, 0) AS puntos_ultima_jornada_equipo,
            ltm.match_id
        FROM
            plantilla_fantasy pf
        -- Uniones básicas para obtener el equipo fantasy del usuario
        JOIN
            equipo_fantasy ef ON pf.id_equipo_fantasy = ef.id_equipo_fantasy
        JOIN
            usuario us ON ef.id_usuario = us.id_usuario
        -- Uniones para obtener los datos del jugador
        JOIN
            jugador j ON pf.id_jugador = j.id_jugador
        -- 1. Unimos con la CTE 1 para encontrar el equipo y temporada MÁS RECIENTE del jugador
        LEFT JOIN
            LatestPlayerTeam lpt ON j.id_jugador = lpt.jugador AND lpt.rn = 1
        -- 2. Unimos para obtener los detalles de ese equipo
        LEFT JOIN
            equipo e ON lpt.equipo = e.id_equipo
        -- 3. Unimos con la CTE 2 para encontrar el ÚLTIMO PARTIDO de ese equipo EN ESA temporada
        LEFT JOIN
            LatestTeamMatch ltm ON lpt.equipo = ltm.team_id AND lpt.temporada = ltm.temporada_id AND ltm.rn = 1
        -- 4. Finalmente, unimos con la tabla de puntos, buscando la coincidencia EXACTA
        --    del jugador Y el ID del último partido del equipo en la temporada correcta
        LEFT JOIN
            puntos_jugador_jornada pjj ON pf.id_jugador = pjj.id_jugador AND ltm.match_id = pjj.id_partido
        WHERE
            us.id_usuario = ?  -- Filtramos por el usuario
        ORDER BY
            FIELD(j.posicion, 'G', 'D', 'M', 'F'), -- Orden G->D->M->F
            pf.es_titular DESC,                   -- Titulares primero
            j.nombre;`,
        [id_usuario]
  );
  return rows;

}


export async function getDesglosePuntosFantasyJugador(id_jugador){
    const [rows] = await pool.query(`
        -- CTE 1: Encuentra el EQUIPO ACTUAL del jugador (el más reciente)
        -- Esto lo usaremos para saber quién es el "rival" en cada partido
        WITH CurrentTeam AS (
            SELECT
                pe.jugador,
                pe.equipo AS id_equipo_actual,
                pe.temporada AS id_temporada_actual,
                e.nombre AS nombre_equipo_actual,
                e.url_imagen AS img_equipo_actual,
                ROW_NUMBER() OVER(PARTITION BY pe.jugador ORDER BY pe.id_plantilla DESC) as rn
            FROM
                plantilla_equipos pe
            JOIN
                equipo e ON pe.equipo = e.id_equipo
            WHERE
                pe.jugador = ? 
        )

        -- Consulta Principal: Obtiene todos los partidos jugados y los datos del rival
        SELECT
            j.id_jugador,
            j.nombre AS nombre_jugador,
            j.posicion,
            j.url_imagen AS url_imagen_jugador,
            
            -- Info estática (se repetirá en cada fila)
            ct.id_equipo_actual,
            ct.nombre_equipo_actual,
            ct.img_equipo_actual,
            vjf.valor_actual,
            vjf.popularidad,
            
            -- Info del partido (cambiará en cada fila)
            p.fecha,
            pjj.jornada,
            pjj.puntos_fantasy,
            
            -- Info del Rival (calculada con el CASE)
            e_rival.id_equipo AS id_rival,
            e_rival.nombre AS nombre_rival,
            e_rival.url_imagen AS img_rival
            
        FROM
            puntos_jugador_jornada pjj
        -- Unimos con partido para obtener la fecha, jornada y los IDs de los equipos
        JOIN
            partido p ON pjj.id_partido = p.id_partido
        -- Unimos con jugador para obtener sus datos
        JOIN
            jugador j ON pjj.id_jugador = j.id_jugador
        -- Unimos con la CTE para saber cuál es el equipo "actual" del jugador
        LEFT JOIN
            CurrentTeam ct ON pjj.id_jugador = ct.jugador AND ct.rn = 1
        -- Unimos con valor_fantasy (usando la temporada del partido)
        LEFT JOIN
            valor_jugador_fantasy vjf ON pjj.id_jugador = vjf.id_jugador 
        -- Unimos la tabla equipo OTRA VEZ para obtener los datos del RIVAL
        LEFT JOIN
            equipo e_rival ON e_rival.id_equipo = (
                -- Este CASE identifica al rival basándose en el equipo actual del jugador
                CASE
                    WHEN p.equipo_local = ct.id_equipo_actual THEN p.equipo_visitante
                    ELSE p.equipo_local
                END
            )
        WHERE
            pjj.id_jugador = ? 
        ORDER BY
            p.fecha DESC;`,
        [id_jugador, id_jugador]
  );
  return rows;

}


export async function getProximoPartido(id_equipo){
    const [rows] = await pool.query(
    `SELECT
        p.id_partido,
        p.jornada,
        p.fecha,
        
        -- Traemos los datos del equipo local
        e_local.id_equipo AS id_local,
        e_local.nombre AS nombre_local,
        e_local.url_imagen AS img_local,
        
        -- Traemos los datos del equipo visitante
        e_visitante.id_equipo AS id_visitante,
        e_visitante.nombre AS nombre_visitante,
        e_visitante.url_imagen AS img_visitante
    FROM
        partido p
    -- Unimos la tabla equipo para el equipo local
    JOIN
        equipo e_local ON p.equipo_local = e_local.id_equipo
    -- Unimos la tabla equipo OTRA VEZ (con un alias) para el visitante
    JOIN
        equipo e_visitante ON p.equipo_visitante = e_visitante.id_equipo
    WHERE
        -- 1. Buscamos el ID de tu equipo tanto en la columna local COMO en la visitante
        (p.equipo_local = ? OR p.equipo_visitante = ?)
        
        -- 2. Filtramos para que solo muestre partidos de hoy en adelante
        AND p.fecha >= CURDATE()
        
    -- 3. Ordenamos por fecha para que el más cercano aparezca primero
    ORDER BY
        p.fecha ASC
        
    -- 4. Tomamos solo el primer resultado
    LIMIT 1;`,
    [id_equipo, id_equipo]
  );
  return rows[0];

}


export async function realizarFichaje(id_equipo_fantasy, jugadorSaleId, jugadorEntraId, fichajesRestantes, id_temporada) {
    const conn = await pool.getConnection();
    await conn.beginTransaction();

    try {
        console.log(jugadorEntraId)
        // 1. Obtener precio y equipo del jugador que ENTRA
        const [jugadorEntraData] = await conn.query(
            `SELECT vj.valor_actual, lta.equipo AS id_equipo
             FROM valor_jugador_fantasy vj
             JOIN (
                 SELECT pe.jugador, pe.equipo, ROW_NUMBER() OVER(PARTITION BY pe.jugador ORDER BY pe.id_plantilla DESC) as rn
                 FROM plantilla_equipos pe WHERE pe.jugador = ?
             ) lta ON vj.id_jugador = lta.jugador AND lta.rn = 1
             WHERE vj.id_jugador = ? `,
            [jugadorEntraId, jugadorEntraId]
        );
        
        if (!jugadorEntraData.length) throw new Error("No se encontró el jugador a comprar.");
        const jugadorEntra = jugadorEntraData[0];

        // 2. Obtener precio del jugador que SALE
        const [jugadorSaleData] = await conn.query(
            `SELECT precio_compra, es_capitan FROM plantilla_fantasy 
             WHERE id_equipo_fantasy = ? AND id_jugador = ?`,
            [id_equipo_fantasy, jugadorSaleId]
        );
        if (!jugadorSaleData.length) throw new Error("No se encontró al jugador a vender en tu plantilla.");
        const jugadorSale = jugadorSaleData[0];

        // 3. Obtener presupuesto actual
        const [equipoData] = await conn.query(
            `SELECT presupuesto_restante FROM equipo_fantasy WHERE id_equipo_fantasy = ? FOR UPDATE`, // Bloquea la fila
            [id_equipo_fantasy]
        );
        let presupuesto = parseFloat(equipoData[0].presupuesto_restante);
        
        // 4. Validar presupuesto
        const nuevoPresupuesto = presupuesto + parseFloat(jugadorSale.precio_compra) - parseFloat(jugadorEntra.valor_actual);
        if (nuevoPresupuesto < 0) {
            throw new Error(`Presupuesto insuficiente. Necesitas ${nuevoPresupuesto * -1}M más.`);
        }

        // 5. Validar límite de equipo (2 jugadores)
        const [conteoEquipo] = await conn.query(
            `SELECT COUNT(pf.id_jugador) AS count
             FROM plantilla_fantasy pf
             JOIN (
                 SELECT pe.jugador, pe.equipo, ROW_NUMBER() OVER(PARTITION BY pe.jugador, pe.temporada ORDER BY pe.id_plantilla DESC) as rn
                 FROM plantilla_equipos pe WHERE pe.temporada = ?
             ) lta ON pf.id_jugador = lta.jugador AND lta.rn = 1
             WHERE pf.id_equipo_fantasy = ? 
               AND lta.equipo = ?
               AND pf.id_jugador != ?`, // No contar al jugador que sale
            [id_temporada, id_equipo_fantasy, jugadorEntra.id_equipo, jugadorSaleId]
        );
        
        if (conteoEquipo[0].count >= 2) {
            throw new Error("Límite de 2 jugadores por equipo alcanzado.");
        }

        // 6. Calcular costo de puntos
        let costoPuntos = 0;
        let fichajesNuevos = fichajesRestantes;
        if (fichajesRestantes > 0) {
            fichajesNuevos -= 1;
        } else {
            costoPuntos = 5; // Penalización de 5 puntos
            // (Aquí también restarías 5 puntos al 'puntos_totales' del equipo)
        }

        // 7. Ejecutar el UPDATE en la plantilla
        await conn.query(
            `UPDATE plantilla_fantasy 
             SET id_jugador = ?, precio_compra = ?, es_capitan = 0
             WHERE id_equipo_fantasy = ? AND id_jugador = ?`,
            [jugadorEntraId, jugadorEntra.valor_actual, id_equipo_fantasy, jugadorSaleId]
        );

        // 8. Actualizar el presupuesto del equipo y fichajes restantes
        await conn.query(
            `UPDATE equipo_fantasy 
             SET presupuesto_restante = ?, fichajes_jornada_restantes = ?
             WHERE id_equipo_fantasy = ?`,
            [nuevoPresupuesto, fichajesNuevos, id_equipo_fantasy]
        );
        
        // 9. (Opcional pero recomendado) Guardar en tabla 'transferencia_fantasy'
        const [jornadaActual] = await conn.query("SELECT MAX(jornada) FROM partido WHERE fecha <= CURDATE()");
        await conn.query(
             `INSERT INTO transferencia_fantasy (id_equipo_fantasy, jornada, jugador_sale_id, jugador_entra_id, costo_puntos)
              VALUES (?, ?, ?, ?, ?)`,
             [id_equipo_fantasy, jornadaActual[0]['MAX(jornada)'], jugadorSaleId, jugadorEntraId, costoPuntos]
         );

        // 10. Confirmar transacción
        await conn.commit();

    } catch (error) {
        await conn.rollback();
        console.error("Error en transacción de fichaje:", error);
        throw error; // Lanza el error para que app.js lo atrape
    } finally {
        conn.release();
    }
}


export async function actualizarPlantilla(id_equipo_fantasy, plantilla) {
    const conn = await pool.getConnection(); // Obtén una conexión del pool
    await conn.beginTransaction(); // Inicia la transacción

    try {
        // Preparamos todas las promesas de actualización
        // (Promise.all ejecuta todas las consultas en paralelo, es más rápido)
        const updatePromises = plantilla.map(player => {
            return conn.query(
                `UPDATE plantilla_fantasy 
                 SET es_titular = ?, es_capitan = ?
                 WHERE id_equipo_fantasy = ? AND id_jugador = ?`,
                [player.es_titular, player.es_capitan, id_equipo_fantasy, player.id_jugador]
            );
        });

        // Ejecutamos todas las promesas de actualización
        await Promise.all(updatePromises);
        
        // Si todas tuvieron éxito, guardamos los cambios
        await conn.commit();
        console.log(`Plantilla ${id_equipo_fantasy} actualizada con éxito.`);

    } catch (error) {
        // Si una sola falla, deshacemos todo
        await conn.rollback();
        console.error("Error en la transacción de actualizar plantilla:", error);
        throw error; // Lanza el error para que app.js lo atrape
    } finally {
        conn.release(); // Siempre libera la conexión de vuelta al pool
    }
}


export async function getProximosPartidosDeEquipos(){
    const [rows] = await pool.query(
    `select p.* , el.nombre as nombre_local, ev.nombre as nombre_visitante, el.url_imagen as escudo_local, ev.url_imagen as escudo_visitante
    from partido p 
    join equipo el on p.equipo_local = el.id_equipo
    join equipo ev on p.equipo_visitante= ev.id_equipo

    where momio_ganador is null and fecha > '2025-11-02' order by fecha asc limit 5; `
  );
  return rows;

}

export async function getMVPdeLaSemana(){
    const [rows] = await pool.query(
    `-- CTE 1: Encuentra la última jornada completada (que ya pasó) para la temporada_id dada
    WITH LatestCompletedGameweek AS (
        SELECT 
            MAX(p.jornada) AS max_jornada
        FROM 
            partido p
        WHERE 
            p.fecha <= '2025-11-06' -- CURDATE() obtiene la fecha de hoy
    ),
    -- CTE 2: Encuentra el equipo más reciente para cada jugador en esa temporada
    LatestPlayerTeam AS (
        SELECT
            pe.jugador,
            pe.equipo,
            ROW_NUMBER() OVER(
                PARTITION BY pe.jugador 
                ORDER BY pe.id_plantilla DESC
            ) as rn
        FROM
            plantilla_equipos pe

    )
    -- Consulta Principal: Obtiene los 5 mejores jugadores de esa jornada
    SELECT
        j.id_jugador,
        j.nombre AS nombre_jugador,
        j.url_imagen AS url_imagen_jugador,
        e.nombre AS nombre_equipo,
        e.url_imagen AS img_equipo,
        pjj.puntos_fantasy
    FROM
        puntos_jugador_jornada pjj
    -- Unir con partido para filtrar por jornada y temporada
    JOIN
        partido p ON pjj.id_partido = p.id_partido
    -- Unir con la CTE 1 para asegurarse de que es la última jornada
    JOIN
        LatestCompletedGameweek lcg ON p.jornada = lcg.max_jornada
    -- Unir con jugador para obtener sus detalles
    JOIN
        jugador j ON pjj.id_jugador = j.id_jugador
    -- Unir con la CTE 2 para obtener el equipo más reciente del jugador
    LEFT JOIN
        LatestPlayerTeam lpt ON j.id_jugador = lpt.jugador AND lpt.rn = 1
    -- Unir con equipo para obtener los detalles del equipo
    LEFT JOIN
        equipo e ON lpt.equipo = e.id_equipo

    ORDER BY
        pjj.puntos_fantasy DESC
    LIMIT 5;`
  );
  return rows;

}