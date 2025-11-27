
import dotenv from 'dotenv'
dotenv.config()
import mysql from 'mysql2/promise'


  
const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME, 
  port: process.env.DB_PORT,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
};


if (process.env.DB_SSL_CA) {
  dbConfig.ssl = {
    ca: process.env.DB_SSL_CA,
    rejectUnauthorized: false 
  };
}

export const pool = mysql.createPool(dbConfig);



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
            COALESCE(sj.disparos_a_puerta / sj.disparos, 0) as porcentaje_disparos_a_puerta,
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
            ee.disparos / ee.partidos_jugados as disparos_por_90,
            ee.goles_de_penal
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
            al.formacion_local, 
            al.formacion_visitante,
            
            j1.id_jugador as id1, j1.nombre as nombre1, j1.dorsal as dorsal1, j1.url_imagen as img1,
            ejp1.rating as rating1, 
            
            j2.id_jugador as id2, j2.nombre as nombre2, j2.dorsal as dorsal2 , j2.url_imagen as img2,
            ejp2.rating as rating2, 
            
            j3.id_jugador as id3, j3.nombre as nombre3, j3.dorsal as dorsal3, j3.url_imagen as img3,
            ejp3.rating as rating3, 
            
            j4.id_jugador as id4, j4.nombre as nombre4, j4.dorsal as dorsal4, j4.url_imagen as img4,
            ejp4.rating as rating4,
            
            j5.id_jugador as id5, j5.nombre as nombre5, j5.dorsal as dorsal5, j5.url_imagen as img5,
            ejp5.rating as rating5, 
            
            j6.id_jugador as id6, j6.nombre as nombre6, j6.dorsal as dorsal6, j6.url_imagen as img6,
            ejp6.rating as rating6, 
            
            j7.id_jugador as id7, j7.nombre as nombre7, j7.dorsal as dorsal7, j7.url_imagen as img7,
            ejp7.rating as rating7, 
            
            j8.id_jugador as id8, j8.nombre as nombre8, j8.dorsal as dorsal8, j8.url_imagen as img8,
            ejp8.rating as rating8, 
            
            j9.id_jugador as id9, j9.nombre as nombre9, j9.dorsal as drosal9, j9.url_imagen as img9,
            ejp9.rating as rating9, 
            
            j10.id_jugador as id10, j10.nombre as nombre10, j10.dorsal as dorsal10, j10.url_imagen as img10,
            ejp10.rating as rating10, 
            
            j11.id_jugador as id11, j11.nombre as nombre11, j11.dorsal as dorsal11, j11.url_imagen as img11,
            ejp11.rating as rating11, 
            
            j12.id_jugador as id12, j12.nombre as nombre12, j12.dorsal as dorsal12, j12.url_imagen as img12,
            ejp12.rating as rating12, 
            
            j13.id_jugador as id13, j13.nombre as nombre13, j13.dorsal as dorsal13, j13.url_imagen as img13,
            ejp13.rating as rating13, 
            
            j14.id_jugador as id14, j14.nombre as nombre14, j14.dorsal as dorsal14, j14.url_imagen as img14,
            ejp14.rating as rating14, 
            
            j15.id_jugador as id15, j15.nombre as nombre15, j15.dorsal as dorsal15, j15.url_imagen as img15,
            ejp15.rating as rating15, 
            
            j16.id_jugador as id16, j16.nombre as nombre16, j16.dorsal as dorsal16, j16.url_imagen as img16,
            ejp16.rating as rating16, 
            
            j17.id_jugador as id17, j17.nombre as nombre17, j17.dorsal as dorsal17, j17.url_imagen as img17,
            ejp17.rating as rating17, 
            
            j18.id_jugador as id18, j18.nombre as nombre18, j18.dorsal as dorsal18, j18.url_imagen as img18,
            ejp18.rating as rating18, 
            
            j19.id_jugador as id19, j19.nombre as nombre19, j19.dorsal as dorsal19, j19.url_imagen as img19,
            ejp19.rating as rating19, 
            
            j20.id_jugador as id20, j20.nombre as nombre20, j20.dorsal as dorsal20, j20.url_imagen as img20,
            ejp20.rating as rating20,
            
            j21.id_jugador as id21, j21.nombre as nombre21, j21.dorsal as dorsal21, j21.url_imagen as img21,
            ejp21.rating as rating21, 
            
            j22.id_jugador as id22, j22.nombre as nombre22, j22.dorsal as dorsal22, j22.url_imagen as img22,
            ejp22.rating as rating22 
                
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

        LEFT JOIN estadistica_jugador_partido ejp1 ON al.partido = ejp1.partido AND al.jugador1 = ejp1.jugador
        LEFT JOIN estadistica_jugador_partido ejp2 ON al.partido = ejp2.partido AND al.jugador2 = ejp2.jugador
        LEFT JOIN estadistica_jugador_partido ejp3 ON al.partido = ejp3.partido AND al.jugador3 = ejp3.jugador
        LEFT JOIN estadistica_jugador_partido ejp4 ON al.partido = ejp4.partido AND al.jugador4 = ejp4.jugador
        LEFT JOIN estadistica_jugador_partido ejp5 ON al.partido = ejp5.partido AND al.jugador5 = ejp5.jugador
        LEFT JOIN estadistica_jugador_partido ejp6 ON al.partido = ejp6.partido AND al.jugador6 = ejp6.jugador
        LEFT JOIN estadistica_jugador_partido ejp7 ON al.partido = ejp7.partido AND al.jugador7 = ejp7.jugador
        LEFT JOIN estadistica_jugador_partido ejp8 ON al.partido = ejp8.partido AND al.jugador8 = ejp8.jugador
        LEFT JOIN estadistica_jugador_partido ejp9 ON al.partido = ejp9.partido AND al.jugador9 = ejp9.jugador
        LEFT JOIN estadistica_jugador_partido ejp10 ON al.partido = ejp10.partido AND al.jugador10 = ejp10.jugador
        LEFT JOIN estadistica_jugador_partido ejp11 ON al.partido = ejp11.partido AND al.jugador11 = ejp11.jugador
        LEFT JOIN estadistica_jugador_partido ejp12 ON al.partido = ejp12.partido AND al.jugador12 = ejp12.jugador
        LEFT JOIN estadistica_jugador_partido ejp13 ON al.partido = ejp13.partido AND al.jugador13 = ejp13.jugador
        LEFT JOIN estadistica_jugador_partido ejp14 ON al.partido = ejp14.partido AND al.jugador14 = ejp14.jugador
        LEFT JOIN estadistica_jugador_partido ejp15 ON al.partido = ejp15.partido AND al.jugador15 = ejp15.jugador
        LEFT JOIN estadistica_jugador_partido ejp16 ON al.partido = ejp16.partido AND al.jugador16 = ejp16.jugador
        LEFT JOIN estadistica_jugador_partido ejp17 ON al.partido = ejp17.partido AND al.jugador17 = ejp17.jugador
        LEFT JOIN estadistica_jugador_partido ejp18 ON al.partido = ejp18.partido AND al.jugador18 = ejp18.jugador
        LEFT JOIN estadistica_jugador_partido ejp19 ON al.partido = ejp19.partido AND al.jugador19 = ejp19.jugador
        LEFT JOIN estadistica_jugador_partido ejp20 ON al.partido = ejp20.partido AND al.jugador20 = ejp20.jugador
        LEFT JOIN estadistica_jugador_partido ejp21 ON al.partido = ejp21.partido AND al.jugador21 = ejp21.jugador
        LEFT JOIN estadistica_jugador_partido ejp22 ON al.partido = ejp22.partido AND al.jugador22 = ejp22.jugador

        WHERE al.partido = ?;
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
        SELECT
            epj.id_jugador,
            epj.imagen_jugador,
            epj.nombre_jugador,
            epj.nombre_equipo,
            epj.imagen_equipo,
            
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
        CROSS JOIN 
            MaximosGlobales AS mg
        WHERE
            epj.id_jugador = ?; 
        `, [jugador])
    return rows; 
    
}

export async function getPercentilesPortero(jugador) {
    const [rows] = await pool.query(`
        WITH EstadisticasPorJugador AS (
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
        SELECT
            epj.id_jugador,
            epj.imagen_jugador,
            epj.nombre_jugador,
            epj.nombre_equipo,
            epj.imagen_equipo,
            
            (CAST(epj.atajadas AS DECIMAL) * 100.0 / NULLIF(mg.max_atajadas, 0)) AS percentil_atajadas,
            (CAST(epj.porterias_imbatidas AS DECIMAL) * 100.0 / NULLIF(mg.max_porterias_imbatidas, 0)) AS percentil_porterias_imbatidas,
            (CAST(epj.penales_atajados AS DECIMAL) * 100.0 / NULLIF(mg.max_penales_atajados, 0)) AS percentil_penales_atajados,
            (CAST(epj.porcentaje_de_atajadas AS DECIMAL) * 100.0 / NULLIF(mg.max_porcentaje_de_atajadas, 0)) AS percentil_porcentaje_de_atajadas,
            (CAST(epj.acciones_fuera_del_area AS DECIMAL) * 100.0 / NULLIF(mg.max_acciones_fuera_del_area, 0)) AS percentil_acciones_fuera_del_area,
            (CAST(epj.centros_interceptados AS DECIMAL) * 100.0 / NULLIF(mg.max_centros_interceptados, 0)) AS percentil_centros_interceptados
        FROM
            EstadisticasPorJugador AS epj
        CROSS JOIN 
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

            rival.nombre AS nombre_rival,
            rival.url_imagen AS imagen_rival,
            rival.id_equipo as id_rival

        FROM 
            estadistica_jugador_partido ejp 
        JOIN 
            jugador j ON ejp.jugador = j.id_jugador
        JOIN 
            partido p ON ejp.partido = p.id_partido

        JOIN 
            plantilla_equipos pe ON pe.jugador = j.id_jugador 
            AND (pe.equipo = p.equipo_local OR pe.equipo = p.equipo_visitante)

        
            
        JOIN 
            equipo mi_equipo ON pe.equipo = mi_equipo.id_equipo

        JOIN 
            equipo rival ON rival.id_equipo = (
                CASE 
                    WHEN pe.equipo = p.equipo_local THEN p.equipo_visitante
                    ELSE p.equipo_local
                END
            )

        WHERE 
            j.id_jugador = ?

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

            rival.nombre AS nombre_rival,
            rival.url_imagen AS imagen_rival,
            rival.id_equipo as id_rival

        FROM 
            estadistica_jugador_portero ejp 
        JOIN 
            jugador j ON ejp.jugador = j.id_jugador
        JOIN 
            partido p ON ejp.partido = p.id_partido

        JOIN 
            plantilla_equipos pe ON pe.jugador = j.id_jugador 
            AND (pe.equipo = p.equipo_local OR pe.equipo = p.equipo_visitante)

        
            
        JOIN 
            equipo mi_equipo ON pe.equipo = mi_equipo.id_equipo

        JOIN 
            equipo rival ON rival.id_equipo = (
                CASE 
                    WHEN pe.equipo = p.equipo_local THEN p.equipo_visitante
                    ELSE p.equipo_local
                END
            )

        WHERE 
            j.id_jugador = ?

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
                ROW_NUMBER() OVER(PARTITION BY pe.jugador ORDER BY pe.id_plantilla DESC) AS rn
            FROM
                plantilla_equipos pe
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
            uepj.rn = 1
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

export async function buscarUsuarioPorUsername(username) {
  const [rows] = await pool.query('SELECT id_usuario FROM usuario WHERE username = ?', [username]);
  return rows[0];
}

export async function buscarUsuarioPorEmail(email) {
    const [rows] = await pool.query('SELECT id_usuario FROM usuario WHERE email = ?', [email]);
    return rows[0];
}


export async function findUserByEmail(email) {
  const [rows] = await pool.query(
    'SELECT id_usuario, hashed_password FROM usuario WHERE email = ?',
    [email]
  );
  return rows[0]; 
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
        WITH LatestTeamAssignment AS (
            SELECT
                pe.jugador,
                pe.equipo,
                pe.temporada,
                ROW_NUMBER() OVER(PARTITION BY pe.jugador, pe.temporada ORDER BY pe.id_plantilla DESC) as rn
            FROM
                plantilla_equipos pe
        )
        SELECT
            vj.id_jugador,
            j.nombre AS nombre_jugador,
            j.posicion,
            j.url_imagen AS url_imagen_jugador,
            vj.popularidad,
            vj.valor_actual,
            COALESCE(SUM(pjj.puntos_fantasy), 0) AS total_puntos_fantasy,
            e.id_equipo,
            e.nombre AS nombre_equipo,
            e.url_imagen AS img_equipo 
        FROM
            valor_jugador_fantasy vj
        JOIN
            jugador j ON vj.id_jugador = j.id_jugador
        JOIN
            LatestTeamAssignment lta ON j.id_jugador = lta.jugador 
        JOIN
            equipo e ON lta.equipo = e.id_equipo
        LEFT JOIN
            puntos_jugador_jornada pjj ON vj.id_jugador = pjj.id_jugador
        LEFT JOIN
            partido p ON pjj.id_partido = p.id_partido 
        WHERE
            vj.valor_actual > 0
            AND lta.rn = 1
        GROUP BY
            vj.id_jugador,
            j.nombre,
            j.posicion,
            j.url_imagen,
            vj.popularidad,
            vj.valor_actual,
            e.id_equipo,
            e.nombre,
            e.url_imagen 
        ORDER BY
            total_puntos_fantasy DESC;`
  );
  return rows;

} 


export async function crearEquipoFantasyCompleto(id_usuario, nombreEquipo, presupuestoRestante, plantillaJugadores, paisId) {
    const conn = await pool.getConnection(); 
    await conn.beginTransaction(); 

    try {
        
        const [equipoResult] = await conn.query(
            `INSERT INTO equipo_fantasy (id_usuario, nombre_equipo, presupuesto_restante)
             VALUES (?, ?, ?)`,
            [id_usuario,  nombreEquipo, presupuestoRestante]
        );

        const newTeamId = equipoResult.insertId; 


        const placeholders = plantillaJugadores.map(() => '?').join(','); 
        const [jugadoresConPrecio] = await conn.query(
            `SELECT id_jugador, valor_actual FROM valor_jugador_fantasy 
             WHERE id_jugador IN (${placeholders})`,
            [...plantillaJugadores]
        );

        

        if (jugadoresConPrecio.length !== 15) {
            throw new Error("No se encontraron los datos de precio para todos los jugadores.");
        }

        const plantillaValues = jugadoresConPrecio.map((jugador, index) => {
            const id_jugador = jugador.id_jugador;
            const precio_compra = jugador.valor_actual;
            

            const es_titular = (index < 11) ? 1 : 0; 

            const es_capitan = (index === 0) ? 1 : 0; 

            return [newTeamId, id_jugador, precio_compra, es_titular, es_capitan];
        });

        await conn.query(
            `INSERT INTO plantilla_fantasy (id_equipo_fantasy, id_jugador, precio_compra, es_titular, es_capitan)
             VALUES ?`,
            [plantillaValues] 
        );

        const [ligaGlobal] = await conn.query(
            `SELECT id_liga_fantasy FROM liga_fantasy WHERE es_publica = 1 AND pais IS NULL LIMIT 1`
        );
        if (ligaGlobal.length > 0) {
            await conn.query(
                `INSERT INTO liga_fantasy_miembros (id_liga_fantasy, id_equipo_fantasy) VALUES (?, ?)`,
                [ligaGlobal[0].id_liga_fantasy, newTeamId]
            );
        } else {
            console.warn("ADVERTENCIA: No se encontró una Liga Global pública.");
        }

        let idLigaLocal;
        const [ligaLocalExistente] = await conn.query(
            `SELECT id_liga_fantasy FROM liga_fantasy WHERE es_publica = 1 AND pais = ? LIMIT 1`,
            [paisId]
        );

        if (ligaLocalExistente.length > 0) {
            idLigaLocal = ligaLocalExistente[0].id_liga_fantasy;
        } else {
            console.log(`No se encontró liga para el país ${paisId}. Creando una nueva...`);
            
            const nombrePais = await getNombrePais(conn, paisId);
            const nombreNuevaLiga = `Liga ${nombrePais}`; 

            const [nuevaLigaResult] = await conn.query(
                `INSERT INTO liga_fantasy (nombre, es_publica, pais) VALUES (?, 1, ?)`,
                [nombreNuevaLiga, paisId]
            );
            idLigaLocal = nuevaLigaResult.insertId; 
        }

        if (idLigaLocal) {
            await conn.query(
                `INSERT INTO liga_fantasy_miembros (id_liga_fantasy, id_equipo_fantasy) VALUES (?, ?)`,
                [idLigaLocal, newTeamId]
            );
        }


        await conn.commit();
        conn.release(); 
        
        return { id_equipo_fantasy: newTeamId };

    } catch (error) {
        await conn.rollback();
        conn.release(); 
        console.error("Error en la transacción de crear equipo:", error);
        throw error; 
    }
}

async function getNombrePais(conn, paisId) {
    const [paisData] = await conn.query(
        `SELECT nombre FROM pais WHERE id_pais = ?`,
        [paisId]
    );
    if (!paisData.length) throw new Error(`País con ID ${paisId} no encontrado.`);
    return paisData[0].nombre;
}

export async function getPlantillaFantasy(id_usuario){
    const [rows] = await pool.query(`
        WITH LatestPlayerTeam AS (
            SELECT
                pe.jugador,
                pe.equipo,
                pe.temporada,
                ROW_NUMBER() OVER(PARTITION BY pe.jugador ORDER BY pe.id_plantilla DESC) as rn
            FROM
                plantilla_equipos pe
        ),
        LatestTeamMatch AS (
            SELECT
                team_id,
                temporada_id,
                match_id,
                ROW_NUMBER() OVER(PARTITION BY team_id, temporada_id ORDER BY match_fecha DESC, match_id DESC) as rn
            FROM (
                SELECT p.equipo_local as team_id, p.temporada as temporada_id, p.id_partido as match_id, p.fecha as match_fecha
                FROM partido p
                WHERE p.fecha <= CURDATE()  
                UNION ALL
                SELECT p.equipo_visitante as team_id, p.temporada as temporada_id, p.id_partido as match_id, p.fecha as match_fecha
                FROM partido p
                WHERE p.fecha <= CURDATE() 
            ) AS all_team_matches
        )
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
            COALESCE(pjj.puntos_fantasy, 0) AS puntos_ultima_jornada_equipo,
            ltm.match_id
        FROM
            plantilla_fantasy pf
        JOIN
            equipo_fantasy ef ON pf.id_equipo_fantasy = ef.id_equipo_fantasy
        JOIN
            usuario us ON ef.id_usuario = us.id_usuario
        JOIN
            jugador j ON pf.id_jugador = j.id_jugador
        LEFT JOIN
            LatestPlayerTeam lpt ON j.id_jugador = lpt.jugador AND lpt.rn = 1
        LEFT JOIN
            equipo e ON lpt.equipo = e.id_equipo
        LEFT JOIN
            LatestTeamMatch ltm ON lpt.equipo = ltm.team_id AND lpt.temporada = ltm.temporada_id AND ltm.rn = 1
        LEFT JOIN
            puntos_jugador_jornada pjj ON pf.id_jugador = pjj.id_jugador AND ltm.match_id = pjj.id_partido
        WHERE
            us.id_usuario = ?  
        ORDER BY
            FIELD(j.posicion, 'G', 'D', 'M', 'F'), 
            pf.es_titular DESC,                   
            j.nombre;`,
        [id_usuario]
  );
  return rows;

}


export async function getDesglosePuntosFantasyJugador(id_jugador){
    const [rows] = await pool.query(`
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

        SELECT
            j.id_jugador,
            j.nombre AS nombre_jugador,
            j.posicion,
            j.url_imagen AS url_imagen_jugador,
            
            ct.id_equipo_actual,
            ct.nombre_equipo_actual,
            ct.img_equipo_actual,
            vjf.valor_actual,
            vjf.popularidad,
            
            p.fecha,
            pjj.jornada,
            pjj.puntos_fantasy,
            
            e_rival.id_equipo AS id_rival,
            e_rival.nombre AS nombre_rival,
            e_rival.url_imagen AS img_rival
            
        FROM
            puntos_jugador_jornada pjj
        JOIN
            partido p ON pjj.id_partido = p.id_partido
        JOIN
            jugador j ON pjj.id_jugador = j.id_jugador
        LEFT JOIN
            CurrentTeam ct ON pjj.id_jugador = ct.jugador AND ct.rn = 1
        LEFT JOIN
            valor_jugador_fantasy vjf ON pjj.id_jugador = vjf.id_jugador 
        LEFT JOIN
            equipo e_rival ON e_rival.id_equipo = (
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
        
        e_local.id_equipo AS id_local,
        e_local.nombre AS nombre_local,
        e_local.url_imagen AS img_local,
        
        e_visitante.id_equipo AS id_visitante,
        e_visitante.nombre AS nombre_visitante,
        e_visitante.url_imagen AS img_visitante
    FROM
        partido p
    JOIN
        equipo e_local ON p.equipo_local = e_local.id_equipo
    JOIN
        equipo e_visitante ON p.equipo_visitante = e_visitante.id_equipo
    WHERE
        (p.equipo_local = ? OR p.equipo_visitante = ?)
        
        AND p.fecha >= CURDATE()
        
    ORDER BY
        p.fecha ASC
        
    LIMIT 1;`,
    [id_equipo, id_equipo]
  );
  return rows[0];

}


export async function realizarFichaje( id_usuario, jugadorSaleId, jugadorEntraId, idFichaActiva) {
    const conn = await pool.getConnection();
    await conn.beginTransaction();

    try {
        // 1. Obtener datos del equipo
        const [equipos] = await conn.query(
            `SELECT id_equipo_fantasy, presupuesto_restante, fichajes_jornada_restantes, puntos_totales 
             FROM equipo_fantasy WHERE id_usuario = ? FOR UPDATE`,
            [id_usuario]
        );

        if (!equipos.length) throw new Error("Equipo no encontrado");
        const equipo = equipos[0];

        // 2. Obtener precios
        const [jugadores] = await conn.query(
            `SELECT id_jugador, valor_actual FROM valor_jugador_fantasy 
             WHERE id_jugador IN (?, ?)`,
            [jugadorSaleId, jugadorEntraId]
        );
        
        const sale = jugadores.find(j => j.id_jugador === jugadorSaleId);
        const entra = jugadores.find(j => j.id_jugador === jugadorEntraId);
        if (!sale || !entra) throw new Error("Jugadores no encontrados");

        // 3. Validar Presupuesto (ID 7 = Sin Límites ignora esto)
        const nuevoPresupuesto = parseFloat(equipo.presupuesto_restante) + parseFloat(sale.valor_actual) - parseFloat(entra.valor_actual);
        if (nuevoPresupuesto < 0 && idFichaActiva != 7) {
            throw new Error("Presupuesto insuficiente.");
        }

        // --- 👇 LÓGICA DE FICHAS Y PENALIZACIÓN 👇 ---
        let nuevosFichajesRestantes = equipo.fichajes_jornada_restantes - 1; // Por defecto resta 1
        let costoPuntos = 0;
        let registrarFicha = false;

        // CASO: FICHAJE EXTRA (ID 1)
        if (idFichaActiva == 1) {
            // Verificar si ya se usó
            const [check] = await conn.query(
                `SELECT id_ficha_usuario FROM ficha_usuario WHERE id_equipo_fantasy = ? AND id_ficha = 1`,
                [equipo.id_equipo_fantasy]
            );
            if (check.length > 0) throw new Error("Ya utilizaste tu Fichaje Extra esta temporada.");

            // EFECTO: No restamos fichajes (se mantiene igual que antes)
            nuevosFichajesRestantes = equipo.fichajes_jornada_restantes; 
            costoPuntos = 0; // Sin penalización
            registrarFicha = true; // Hay que marcarla como usada
        }

        // CASO: COMODÍN (ID 6) o SIN LÍMITES (ID 7)
        else if (idFichaActiva == 6 || idFichaActiva == 7) {
            // Verificar si ya se usó (opcional, o confiar en el frontend/app.js)
             const [check] = await conn.query(
                `SELECT id_ficha_usuario FROM ficha_usuario WHERE id_equipo_fantasy = ? AND id_ficha = ?`,
                [equipo.id_equipo_fantasy, idFichaActiva]
            );
            const yaEstabaActiva = check.length > 0;
            
            nuevosFichajesRestantes = equipo.fichajes_jornada_restantes; 
            costoPuntos = 0;
            
            // 3. Solo registramos la ficha si es la PRIMERA vez que la activa
            registrarFicha = !yaEstabaActiva;
        }
        
        // CASO NORMAL (Sin ficha de transferencia activa)
        else {
            // Si nos quedamos en negativos, cobramos penalización
            if (nuevosFichajesRestantes < 0) {
                costoPuntos = 5;
            }
        }

        const nuevosPuntosTotales = equipo.puntos_totales - costoPuntos;

        // --- 4. REGISTRAR USO DE FICHA (Si aplica) ---
        if (registrarFicha) {
            // Obtenemos la jornada actual para el registro
            const [jornadaRows] = await conn.query(`SELECT MIN(jornada) as proxima FROM partido WHERE fecha > NOW()`);
            const jornadaActual = jornadaRows[0].proxima || 1;

            await conn.query(
                `INSERT INTO ficha_usuario (id_equipo_fantasy, id_ficha, jornada_uso) VALUES (?, ?, ?)`,
                [equipo.id_equipo_fantasy, idFichaActiva, jornadaActual]
            );
        }

        // --- 5. EJECUTAR CAMBIOS (PLANTILLA Y EQUIPO) ---
        await conn.query(
            `UPDATE plantilla_fantasy SET id_jugador = ?, precio_compra = ? 
             WHERE id_equipo_fantasy = ? AND id_jugador = ?`,
            [jugadorEntraId, entra.valor_actual, equipo.id_equipo_fantasy, jugadorSaleId]
        );

        await conn.query(
            `UPDATE equipo_fantasy SET presupuesto_restante = ?, fichajes_jornada_restantes = ?, puntos_totales = ?
             WHERE id_equipo_fantasy = ?`,
            [nuevoPresupuesto, nuevosFichajesRestantes, nuevosPuntosTotales, equipo.id_equipo_fantasy]
        );

        await conn.commit();
        
        return { 
            penalizacion_aplicada: costoPuntos > 0,
            puntos_restados: costoPuntos,
            nuevo_presupuesto: nuevoPresupuesto
        };

    } catch (error) {
        await conn.rollback();
        console.error("Error en transacción hacerFichaje:", error);
        throw error;
    } finally {
        conn.release();
    }
}


export async function actualizarPlantilla(id_equipo_fantasy, plantilla) {
    const conn = await pool.getConnection(); 
    await conn.beginTransaction(); 

    try {
        for (const p of plantilla) {
            await conn.query(
                `UPDATE plantilla_fantasy 
                 SET es_titular = ?, es_capitan = ? 
                 WHERE id_equipo_fantasy = ? AND id_jugador = ?`,
                [p.es_titular, p.es_capitan, id_equipo_fantasy, p.id_jugador] 
            );
        }
        await conn.commit();

    } catch (error) {
        await conn.rollback();
        console.error("Error en la transacción de actualizar plantilla:", error);
        throw error; 
    } finally {
        conn.release(); 
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
    `
    WITH LatestCompletedGameweek AS (
        SELECT 
            MAX(p.jornada) AS max_jornada
        FROM 
            partido p
        WHERE 
            p.fecha <= CURDATE() - 1
    ),
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
    SELECT
        j.id_jugador,
        j.nombre AS nombre_jugador,
        j.url_imagen AS url_imagen_jugador,
        e.nombre AS nombre_equipo,
        e.url_imagen AS img_equipo,
        pjj.puntos_fantasy
    FROM
        puntos_jugador_jornada pjj
    JOIN
        partido p ON pjj.id_partido = p.id_partido
    JOIN
        LatestCompletedGameweek lcg ON p.jornada = lcg.max_jornada
    JOIN
        jugador j ON pjj.id_jugador = j.id_jugador
    LEFT JOIN
        LatestPlayerTeam lpt ON j.id_jugador = lpt.jugador AND lpt.rn = 1
    LEFT JOIN
        equipo e ON lpt.equipo = e.id_equipo

    ORDER BY
        pjj.puntos_fantasy DESC
    LIMIT 5;`
  );
  return rows;

}

export async function getPartidosRandom(){
    const [rows] = await pool.query(
    `WITH NextJornadas AS (
    SELECT
        p.temporada,
        MIN(p.jornada) AS jornada
    FROM 
        partido p
    WHERE 
        p.temporada IN (1, 2, 3, 4, 5) 
        AND p.fecha >= CURDATE() 
    GROUP BY 
        p.temporada
),

RankedMatches AS (
    SELECT
        p.id_partido,
        p.temporada,
        p.jornada,
        p.fecha,
        p.equipo_local,
        p.equipo_visitante,
        

        ROW_NUMBER() OVER (
            PARTITION BY p.temporada 
            ORDER BY RAND(DAYOFYEAR(CURDATE()) + p.jornada)
        ) as rn
    FROM 
        partido p
    JOIN 
        NextJornadas nj ON p.temporada = nj.temporada AND p.jornada = nj.jornada
)

SELECT
    rm.id_partido,
    rm.jornada,
    rm.fecha,
    rm.temporada,
    el.id_equipo as id_local,
    el.nombre AS nombre_local,
    el.url_imagen AS escudo_local,
    ev.id_equipo as id_visitante,
    ev.nombre AS nombre_visitante,
    ev.url_imagen AS escudo_visitante
FROM 
    RankedMatches rm
JOIN 
    equipo el ON rm.equipo_local = el.id_equipo
JOIN 
    equipo ev ON rm.equipo_visitante = ev.id_equipo
WHERE 
    rm.rn = 1;`
  );
  return rows;

}


export async function getPlantillasDePredicciones(idPartido){
    const [rows] = await pool.query(
        `SELECT
    j.id_jugador,
    j.nombre AS nombre_jugador,
    j.posicion,
    j.url_imagen AS img_jugador,
    e.id_equipo,
    e.nombre AS nombre_equipo,
    e.url_imagen AS img_equipo,
    
    CASE 
        WHEN pe.equipo = p.equipo_local THEN 'local'
        ELSE 'visitante'
    END AS tipo_equipo 
FROM
    partido p
JOIN
    plantilla_equipos pe ON p.temporada = pe.temporada 
    AND (pe.equipo = p.equipo_local OR pe.equipo = p.equipo_visitante)
JOIN
    jugador j ON pe.jugador = j.id_jugador
JOIN
    equipo e ON pe.equipo = e.id_equipo
WHERE
    p.id_partido = ? 
ORDER BY
    tipo_equipo,
    FIELD(j.posicion, 'G', 'D', 'M', 'F');`,[idPartido]
    );
  return rows;

}

export async function getEstadoDeForma(idEquipo){
    const [rows] = await pool.query(
        `(
        SELECT
            p.fecha,
            (CASE
                WHEN ep.goles_local > ep.goles_visitante THEN 'V' 
                WHEN ep.goles_local = ep.goles_visitante THEN 'E' 
                ELSE 'D' 
            END) AS resultado
        FROM
            partido p
        JOIN
            estadisticas_partido ep ON p.id_partido = ep.partido
        WHERE
            p.equipo_local = ? 
            AND p.fecha < CURDATE() 
    )
    UNION ALL
    (
        SELECT
            p.fecha,
            (CASE
                WHEN ep.goles_visitante > ep.goles_local THEN 'V' 
                WHEN ep.goles_local = ep.goles_visitante THEN 'E' 
                ELSE 'D' 
            END) AS resultado
        FROM
            partido p
        JOIN
            estadisticas_partido ep ON p.id_partido = ep.partido
        WHERE
            p.equipo_visitante = ? 
            AND p.fecha < CURDATE() 
    )
    ORDER BY
        fecha DESC
    LIMIT 5;`,[idEquipo, idEquipo]
    );
  return rows;

}

export async function getPronosticoResultado(idPartido){
    const [rows] = await pool.query(
        `SELECT
    resultado_predicho,
    COUNT(*) AS total_votos
FROM
    prediccion_usuario
WHERE
    id_partido = ? AND resultado_predicho IS NOT NULL
GROUP BY
    resultado_predicho
ORDER BY
    total_votos DESC;`,[idPartido]
    );
  return rows;

}

export async function getPronosticoPrimerEquipoEnAnotar(idPartido){
    const [rows] = await pool.query(
        `SELECT
            e.nombre AS nombre_equipo,
            e.url_imagen AS img_equipo,
            COUNT(*) AS total_votos
        FROM
            prediccion_usuario pu
        JOIN
            equipo e ON pu.primer_equipo_anotar_id = e.id_equipo
        WHERE
            pu.id_partido = ? AND pu.primer_equipo_anotar_id IS NOT NULL
        GROUP BY
            e.id_equipo, e.nombre, e.url_imagen
        ORDER BY
            total_votos DESC
        LIMIT 1; `,[idPartido]
    );
  return rows;

}

export async function getPronosticoPrimerJugadorEnAnotar(idPartido){
    const [rows] = await pool.query(
        `SELECT
            j.nombre AS nombre_jugador,
            j.url_imagen AS img_jugador,
            COUNT(*) AS total_votos
        FROM
            prediccion_usuario pu
        JOIN
            jugador j ON pu.primer_jugador_anotar_id = j.id_jugador
        WHERE
            pu.id_partido = ? AND pu.primer_jugador_anotar_id IS NOT NULL
        GROUP BY
            j.id_jugador, j.nombre, j.url_imagen
        ORDER BY
            total_votos DESC
        LIMIT 1; `,[idPartido]
    );
  return rows;

}

export async function getJornadaFromPartido(id_partido) {
    const [rows] = await pool.query(
        `SELECT jornada FROM partido WHERE id_partido = ?`,
        [id_partido]
    );
    return rows[0];
}

export async function guardarPrediccionUsuario(prediccion) {
    const { 
        id_usuario, 
        id_partido, 
        jornada, 
        resultado_predicho, 
        primer_equipo_anotar_id, 
        primer_jugador_anotar_id 
    } = prediccion;

    const sql = `
        INSERT INTO prediccion_usuario 
            (id_usuario, id_partido, jornada, resultado_predicho, primer_equipo_anotar_id, primer_jugador_anotar_id)
        VALUES 
            (?, ?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
            resultado_predicho = VALUES(resultado_predicho),
            primer_equipo_anotar_id = VALUES(primer_equipo_anotar_id),
            primer_jugador_anotar_id = VALUES(primer_jugador_anotar_id)
    `;

    await pool.query(sql, [
        id_usuario, 
        id_partido, 
        jornada, 
        resultado_predicho, 
        primer_equipo_anotar_id, 
        primer_jugador_anotar_id
    ]);
}

export async function getMisPredicciones(id_usuario, jornada) {
    const [rows] = await pool.query(
        `SELECT
            id_partido,
            resultado_predicho,
            primer_equipo_anotar_id,
            primer_jugador_anotar_id
         FROM 
            prediccion_usuario
         WHERE 
            id_usuario = ? AND jornada = ?`,
        [id_usuario, jornada]
    );
    return rows; 
}

export async function getHistorialPredicciones(id_usuario) {
    const [rows] = await pool.query(
        `SELECT 
            jornada, 
            SUM(puntos_obtenidos) as puntos_jornada
         FROM 
            prediccion_usuario
         WHERE 
            id_usuario = ? AND puntos_obtenidos IS NOT NULL
         GROUP BY 
            jornada
         ORDER BY 
            jornada ASC`, 
        [id_usuario]
    );
    return rows;
}


export async function getMisLigas(id_usuario, pais_id, id_equipo_fantasy) {
    const sql = `
        (SELECT 
            lf.id_liga_fantasy, lf.nombre, 'publica' as tipo, 
            (lf.id_creador = ?) AS es_admin, lf.codigo_invitacion
         FROM liga_fantasy lf
         WHERE lf.es_publica = 1 AND lf.pais IS NULL)
        
        UNION
        
        (SELECT 
            lf.id_liga_fantasy, lf.nombre, 'publica' as tipo, 
            (lf.id_creador = ?) AS es_admin, lf.codigo_invitacion
         FROM liga_fantasy lf
         WHERE lf.es_publica = 1 AND lf.pais = ?)
        
        UNION
        
        (SELECT 
            lf.id_liga_fantasy, lf.nombre, 'privada' as tipo, 
            (lf.id_creador = ?) AS es_admin, lf.codigo_invitacion
         FROM liga_fantasy lf
         JOIN liga_fantasy_miembros lfm ON lf.id_liga_fantasy = lfm.id_liga_fantasy
         WHERE lfm.id_equipo_fantasy = ? AND lf.es_publica = 0)
    `;
    
    const [rows] = await pool.query(sql, [id_usuario, id_usuario, pais_id, id_usuario, id_equipo_fantasy]);
    return rows;
}

export async function getLeagueRanking(id_liga_fantasy) {
    const [rows] = await pool.query(
        `SELECT
            u.id_usuario,
            u.username,
            ef.nombre_equipo,
            ef.puntos_totales
         FROM
            liga_fantasy_miembros lfm
         JOIN
            equipo_fantasy ef ON lfm.id_equipo_fantasy = ef.id_equipo_fantasy
         JOIN
            usuario u ON ef.id_usuario = u.id_usuario
         WHERE
            lfm.id_liga_fantasy = ?
         ORDER BY
            ef.puntos_totales DESC, ef.nombre_equipo ASC`,
        [id_liga_fantasy]
    );
    return rows;
}


export async function crearLiga(nombre, id_creador, codigo_invitacion, id_equipo_fantasy) {
    const conn = await pool.getConnection();
    await conn.beginTransaction();
    
    try {
        const [resultLiga] = await conn.query(
            `INSERT INTO liga_fantasy (nombre, id_creador, es_publica, codigo_invitacion) 
             VALUES (?, ?, 0, ?)`,
            [nombre, id_creador, codigo_invitacion]
        );
        
        const newLeagueId = resultLiga.insertId;

        await conn.query(
            `INSERT INTO liga_fantasy_miembros (id_liga_fantasy, id_equipo_fantasy) 
             VALUES (?, ?)`,
            [newLeagueId, id_equipo_fantasy]
        );
        
        await conn.commit();
        return resultLiga; 

    } catch (error) {
        await conn.rollback();
        console.error("Error en la transacción de crearLiga:", error);
        throw error; 
    } finally {
        conn.release();
    }
}

export async function unirseLigaPorCodigo(codigo, id_equipo_fantasy) {
    const conn = await pool.getConnection();
    await conn.beginTransaction();

    try {
        const [ligas] = await conn.query(
            `SELECT id_liga_fantasy FROM liga_fantasy 
             WHERE codigo_invitacion = ? AND es_publica = 0 FOR UPDATE`,
            [codigo]
        );

        if (ligas.length === 0) {
            throw new Error("Código no válido");
        }
        const idLiga = ligas[0].id_liga_fantasy;

        const [conteo] = await conn.query(
            `SELECT COUNT(*) as memberCount FROM liga_fantasy_miembros 
             WHERE id_liga_fantasy = ? FOR UPDATE`,
            [idLiga]
        );

        if (conteo[0].memberCount >= 100) {
            throw new Error("La liga está llena. No se pueden unir más miembros.");
        }


        await conn.query(
            `INSERT INTO liga_fantasy_miembros (id_liga_fantasy, id_equipo_fantasy) 
             VALUES (?, ?)`,
            [idLiga, id_equipo_fantasy]
        );
        
        await conn.commit();

    } catch (error) {
        await conn.rollback();
        console.error("Error en la transacción de unirseLigaPorCodigo:", error);
        throw error; 
    } finally {
        conn.release();
    }
}

export async function getPublicLeagueIDs(paisId) {
    const [leagues] = await pool.query(
        `SELECT id_liga_fantasy, nombre, pais
         FROM liga_fantasy
         WHERE es_publica = 1 AND (pais IS NULL OR pais = ?)`,
        [paisId]
    );
    
    const globalLeague = leagues.find(l => l.pais === null);
    const localLeague = leagues.find(l => l.pais == paisId); 
    
    return {
        globalLeagueId: globalLeague?.id_liga_fantasy,
        localLeagueId: localLeague?.id_liga_fantasy,
        localLeagueName: localLeague?.nombre || 'Liga Local' 
    };
}

export async function getLeagueRankingTop10(id_liga_fantasy) {
    if (!id_liga_fantasy) {
        return []; 
    }
    
    const [rows] = await pool.query(
        `SELECT
            u.id_usuario,
            u.username,
            ef.nombre_equipo,
            ef.puntos_totales
         FROM
            liga_fantasy_miembros lfm
         JOIN
            equipo_fantasy ef ON lfm.id_equipo_fantasy = ef.id_equipo_fantasy
         JOIN
            usuario u ON ef.id_usuario = u.id_usuario
         WHERE
            lfm.id_liga_fantasy = ?
         ORDER BY
            ef.puntos_totales DESC, ef.nombre_equipo ASC
         LIMIT 5`, 
        [id_liga_fantasy]
    );
    return rows;
}

export async function getEventoActivo() {
    const [rows] = await pool.query(
        `SELECT id_evento, nombre, regla_clave, descripcion
         FROM eventos
         WHERE CURDATE() BETWEEN DATE(fecha_inicio) AND DATE(fecha_fin)
         LIMIT 1`
    );
    return rows[0]; 
}

export async function getEventoRanking(id_evento) {
    const [rows] = await pool.query(
        `SELECT
            u.username,
            ee.nombre_equipo,
            ee.puntos_totales
         FROM equipo_evento ee
         JOIN usuario u ON ee.id_usuario = u.id_usuario
         WHERE ee.id_evento = ?
         ORDER BY ee.puntos_totales DESC
         LIMIT 10`, 
        [id_evento]
    );
    return rows;
}

export async function getEquipoEventoUsuario(id_usuario, id_evento) {
    const [rows] = await pool.query(
        `SELECT id_equipo_evento FROM equipo_evento
         WHERE id_usuario = ? AND id_evento = ?`,
        [id_usuario, id_evento]
    );
    return rows[0];
}

export async function getAvailableEventPlayers(regla) {
    let whereClause = ''; 

    const id_temporada = 1; 

    switch (regla) {
        case 'U23':
            whereClause = ' j.fec_nac >= DATE_SUB(CURDATE(), INTERVAL 23 YEAR)';
            break;
        case 'GIGANTES':
            whereClause = ' j.altura >= 190'; 
            break;
        case 'SOLO_POR':
            whereClause = ' j.posicion = "G"';
            break;
       
        default:
            console.error(`Regla de evento no reconocida: ${regla}`);
            throw new Error(`Regla de evento no reconocida: ${regla}`);
    }

    const sql = `
        SELECT 
            j.id_jugador, j.nombre AS nombre_jugador, j.posicion, j.url_imagen AS url_imagen_jugador,
            e.id_equipo, e.nombre AS nombre_equipo, e.url_imagen AS img_equipo,
            vjf.valor_actual, vjf.popularidad,
            COALESCE(sjf.total_puntos_fantasy, 0) AS total_puntos_fantasy
        FROM jugador j
        JOIN valor_jugador_fantasy vjf ON j.id_jugador = vjf.id_jugador
        LEFT JOIN (
            SELECT pe.jugador, pe.equipo, ROW_NUMBER() OVER(PARTITION BY pe.jugador ORDER BY pe.id_plantilla DESC) as rn
            FROM plantilla_equipos pe 
        ) lta ON j.id_jugador = lta.jugador AND lta.rn = 1
        LEFT JOIN equipo e ON lta.equipo = e.id_equipo
        LEFT JOIN (
            SELECT pjj.id_jugador, SUM(pjj.puntos_fantasy) as total_puntos_fantasy
            FROM puntos_jugador_jornada pjj
            JOIN partido p ON pjj.id_partido = p.id_partido
            
            GROUP BY pjj.id_jugador
        ) sjf ON j.id_jugador = sjf.id_jugador
        WHERE 
            
            ${whereClause} 
    `;
    
    const [rows] = await pool.query(sql);
    return rows;
}


export async function crearEquipoEventoCompleto(id_usuario, id_evento, nombreEquipo, plantillaIds, reglaClave) {
    const conn = await pool.getConnection();
    await conn.beginTransaction();

    try {
        let validationWhere = '';
        switch (reglaClave) {
            case 'U23':
                validationWhere = 'AND j.fec_nac >= DATE_SUB(CURDATE(), INTERVAL 23 YEAR)';
                break;
            case 'GIGANTES':
                validationWhere = 'AND j.altura >= 190';
                break;
            case 'SOLO_POR':
                validationWhere = 'AND j.posicion = "G"';
                break;
            default:
                throw new Error("Regla de evento no reconocida.");
        }
        
        const placeholders = plantillaIds.map(() => '?').join(',');
        const [validationRows] = await conn.query(
            `SELECT COUNT(j.id_jugador) as validCount FROM jugador j
             WHERE j.id_jugador IN (${placeholders}) ${validationWhere}`,
            [...plantillaIds]
        );

        if (validationRows[0].validCount !== plantillaIds.length || plantillaIds.length !== 11) {
            throw new Error(`La plantilla no cumple con la regla del evento "${reglaClave}". (${validationRows[0].validCount}/11 válidos)`);
        }

        const [equipoResult] = await conn.query(
            `INSERT INTO equipo_evento (id_usuario, id_evento, nombre_equipo) VALUES (?, ?, ?)`,
            [id_usuario, id_evento, nombreEquipo]
        );
        const newEventTeamId = equipoResult.insertId;

        const plantillaValues = plantillaIds.map(id_jugador => [newEventTeamId, id_jugador]);
        await conn.query(
            `INSERT INTO plantilla_evento (id_equipo_evento, id_jugador) VALUES ?`,
            [plantillaValues]
        );

        await conn.commit();
        conn.release();
        return { id_equipo_evento: newEventTeamId };

    } catch (error) {
        await conn.rollback();
        conn.release();
        console.error("Error en transacción crearEquipoEvento:", error);
        throw error; 
    }
}

export async function getEquipoEventoUsuarioID(id_usuario, id_evento) {
    const [rows] = await pool.query(
        `SELECT id_equipo_evento FROM equipo_evento
         WHERE id_usuario = ? AND id_evento = ?`,
        [id_usuario, id_evento]
    );
    return rows[0]; 
}

export async function getPlantillaEvento(id_equipo_evento) {
    const id_temporada = 1; 
    
    const [rows] = await pool.query(
        `SELECT 
            j.id_jugador, 
            j.nombre AS nombre_jugador, 
            j.posicion, 
            j.url_imagen AS url_imagen_jugador,
            e.nombre AS nombre_equipo,
            1 AS es_titular,
            0 AS es_capitan 
         FROM plantilla_evento pe
         JOIN jugador j ON pe.id_jugador = j.id_jugador
         LEFT JOIN (
             SELECT pe_sub.jugador, pe_sub.equipo, 
                    ROW_NUMBER() OVER(PARTITION BY pe_sub.jugador ORDER BY pe_sub.id_plantilla DESC) as rn
             FROM plantilla_equipos pe_sub WHERE pe_sub.temporada = ?
         ) lta ON j.id_jugador = lta.jugador AND lta.rn = 1
         LEFT JOIN equipo e ON lta.equipo = e.id_equipo
         WHERE pe.id_equipo_evento = ?
         ORDER BY  FIELD(j.posicion, 'G', 'D', 'M', 'F')`,
         [id_temporada, id_equipo_evento]
    );
    return rows; 
}

export async function getLogrosBase(id_usuario) {
    const [totalUsersRows] = await pool.query('SELECT COUNT(id_usuario) AS total FROM equipo_fantasy');
    const totalUsers = totalUsersRows[0].total > 0 ? totalUsersRows[0].total : 1;

    const sql = `
        SELECT
            l.id_logro,
            l.nombre,
            l.descripcion,
            
            (CASE WHEN lu_user.id_usuario IS NOT NULL THEN 1 ELSE 0 END) AS unlocked,
            
            (SELECT COUNT(id_usuario) FROM logro_usuario lu_all WHERE lu_all.id_logro = l.id_logro) * 100 / 24 AS rarity

            
            
        FROM 
            logro l
        LEFT JOIN 
            logro_usuario lu_user ON l.id_logro = lu_user.id_logro AND lu_user.id_usuario = ?
        ORDER BY
            unlocked DESC, rarity ASC;
    `;
    
    const [rows] = await pool.query(sql, [ id_usuario]);
    return rows;
}


export async function otorgarLogro(id_usuario, id_logro) {
    try {
        const [result] = await pool.query(
            'INSERT IGNORE INTO logro_usuario (id_logro, id_usuario, fecha_desbloqueo) VALUES (?, ?, NOW())',
            [id_logro, id_usuario]
        );
        
        if (result.affectedRows > 0) {
            console.log(`¡LOGRO DESBLOQUEADO! Usuario ${id_usuario} obtuvo el logro ${id_logro}`);
        }
        return result.affectedRows > 0; 
        
    } catch (error) {
        console.error("Error al otorgar logro:", error);
    }
}

export async function getStatsParaPoisson(id_partido) {
    const [partidoRows] = await pool.query(
        `SELECT equipo_local, equipo_visitante 
         FROM partido 
         WHERE id_partido = ?`,
        [id_partido]
    );
    if (!partidoRows.length) {
        throw new Error('Partido no encontrado');
    }
    const { equipo_local, equipo_visitante } = partidoRows[0];

    const sql = `
    WITH
    StatsEquipos AS (
        SELECT
            p.equipo_local AS id_equipo,
            SUM(ep.goles_local) AS goles_anotados_casa,
            SUM(ep.goles_visitante) AS goles_recibidos_casa,
            COUNT(p.id_partido) AS partidos_casa,
            0 AS goles_anotados_visita,
            0 AS goles_recibidos_visita,
            0 AS partidos_visita
        FROM partido p
        JOIN estadisticas_partido ep ON p.id_partido = ep.partido
        WHERE  p.fecha < CURDATE()
        GROUP BY p.equipo_local

        UNION ALL

        SELECT
            p.equipo_visitante AS id_equipo,
            0 AS goles_anotados_casa,
            0 AS goles_recibidos_casa,
            0 AS partidos_casa,
            SUM(ep.goles_visitante) AS goles_anotados_visita,
            SUM(ep.goles_local) AS goles_recibidos_visita,
            COUNT(p.id_partido) AS partidos_visita
        FROM partido p
        JOIN estadisticas_partido ep ON p.id_partido = ep.partido
        WHERE p.fecha < CURDATE()
        GROUP BY p.equipo_visitante
    ),
    StatsAgregadas AS (
        SELECT
            id_equipo,
            SUM(goles_anotados_casa) AS total_goles_casa,
            SUM(goles_recibidos_casa) AS total_recibidos_casa,
            SUM(partidos_casa) AS total_partidos_casa,
            SUM(goles_anotados_visita) AS total_goles_visita,
            SUM(goles_recibidos_visita) AS total_recibidos_visita,
            SUM(partidos_visita) AS total_partidos_visita
        FROM StatsEquipos
        GROUP BY id_equipo
    ),
    PromediosLiga AS (
        SELECT
            AVG(ep.goles_local) AS avg_goles_casa,
            AVG(ep.goles_visitante) AS avg_goles_visita
        FROM estadisticas_partido ep
        JOIN partido p ON ep.partido = p.id_partido
        WHERE p.fecha < CURDATE()
    )
    SELECT
        (SELECT total_goles_casa / total_partidos_casa FROM StatsAgregadas WHERE id_equipo = ?) AS poder_ataque_local,
        (SELECT total_recibidos_casa / total_partidos_casa FROM StatsAgregadas WHERE id_equipo = ?) AS poder_defensa_local,
        
        (SELECT total_goles_visita / total_partidos_visita FROM StatsAgregadas WHERE id_equipo = ?) AS poder_ataque_visitante,
        (SELECT total_recibidos_visita / total_partidos_visita FROM StatsAgregadas WHERE id_equipo = ?) AS poder_defensa_visitante,
        
        pl.avg_goles_casa AS avg_liga_casa,
        pl.avg_goles_visita AS avg_liga_visita
    FROM
        PromediosLiga pl;
    `;

    const [rows] = await pool.query(sql, [
        equipo_local,  
        equipo_local,  
        equipo_visitante,
        equipo_visitante  
    ]);
    return rows[0];
}


export async function getPuzzleDelUsuario(id_usuario) {
    const [userData] = await pool.query(
        `SELECT es.id_estadio, es.url_imagen AS estadio_url_completa
         FROM usuario u
         JOIN equipo e ON u.equipo_favorito_id = e.id_equipo
         JOIN estadio es on e.estadio = es.id_estadio
         WHERE u.id_usuario = ?`,
        [id_usuario]
    );

    if (!userData.length || !userData[0].id_estadio) {
        return { piezas: [], estadio_url_completa: null };
    }
    
    const { id_estadio, estadio_url_completa } = userData[0];

    const [piezas] = await pool.query(
            `SELECT
                pz.id_pieza,
                pz.nombre,
                pz.url_imagen,
                (CASE WHEN pzu.id_usuario IS NOT NULL THEN 1 ELSE 0 END) AS desbloqueado
            FROM 
                pieza_rompecabezas pz
            LEFT JOIN 
                pieza_rompecabezas_usuario pzu ON pz.id_pieza = pzu.id_pieza AND pzu.id_usuario = ?
            WHERE 
                pz.id_estadio = ?
            ORDER BY 
                pz.nombre ASC 
        `,
        [id_usuario, id_estadio]
    );

    return {
        piezas: piezas, 
        estadio_url_completa: estadio_url_completa 
    };
}


export async function getUltimaJornadaCompletada(temporada_id) {
    const [rows] = await pool.query(
        `SELECT MAX(jornada) as ultima_jornada
         FROM partido
         WHERE fecha < CURDATE() AND temporada = ?`,
        [temporada_id]
    );
    return rows[0]?.ultima_jornada || 1; 
}


export async function getFichasEquipo(id_equipo_fantasy) {
    const [rows] = await pool.query(`
        SELECT 
            f.id_ficha, 
            f.nombre, 
            f.descripcion,
            -- Si 'jornada_uso' no es nulo, significa que ya se usó
            fu.jornada_uso 
        FROM 
            ficha f
        LEFT JOIN 
            ficha_usuario fu ON f.id_ficha = fu.id_ficha AND fu.id_equipo_fantasy = ?
        ORDER BY 
            f.id_ficha ASC
    `, [id_equipo_fantasy]);

    return rows.map(ficha => ({
        ...ficha,
        usada: ficha.jornada_uso !== null 
    }));
}

export async function registrarUsoFicha(id_equipo_fantasy, id_ficha) {
    
    const [jornadaRows] = await pool.query(
        `SELECT MIN(jornada) as proxima_jornada FROM partido WHERE fecha > NOW()`
    );
    const jornadaActual = jornadaRows[0].proxima_jornada || 1; 

    await pool.query(`
        INSERT INTO ficha_usuario (id_equipo_fantasy, id_ficha, jornada_uso)
        VALUES (?, ?, ?)
    `, [id_equipo_fantasy, id_ficha, jornadaActual]);
}