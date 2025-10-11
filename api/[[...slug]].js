// api/[[...slug]].js

import * as db from '../database.js'; // Importamos TODAS las funciones de database.js

export default async function handler(req, res) {
  // req.query.slug será un array con las partes de la URL.
  // Ej: /api/liga/premierleague -> slug = ['liga', 'premierleague']
  // Ej: /api/estadisticas-jugador/45 -> slug = ['estadisticas-jugador', '45']
  console.log("Función [[...slug]] ejecutada.");
  console.log("URL solicitada:", req.url);
  console.log("Valor de req.query.slug:", req.query.slug);
  const { slug } = req.query;

  try {
    // --- NUESTRO ROUTER MANUAL ---
    // Comprobamos el contenido del 'slug' para decidir qué hacer.
    if (slug && slug[0] === 'buscar-jugadores' && slug.length === 1) {
      
      const nombre = 1;

      if (!nombre) return res.status(404).json({ error: 'Liga no encontrada' });

      const data = await db.buscarJugadores(nombre);
      

      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'estadisticas-ofensivas-premier-league' && slug.length === 1) {
      
      const ligaId = 1;

      if (!ligaId) return res.status(404).json({ error: 'Liga no encontrada' });

      const data = await db.getEstadisticasOfensivas(ligaId);
      

      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'estadisticas-ofensivas-la-liga' && slug.length === 1) {
      
      const ligaId = 2;

      if (!ligaId) return res.status(404).json({ error: 'Liga no encontrada' });

      const data = await db.getEstadisticasOfensivas(ligaId);
      

      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'estadisticas-ofensivas-serie-a' && slug.length === 1) {
      
      const ligaId = 3;

      if (!ligaId) return res.status(404).json({ error: 'Liga no encontrada' });

      const data = await db.getEstadisticasOfensivas(ligaId);
      

      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'estadisticas-ofensivas-bundesliga' && slug.length === 1) {
      
      const ligaId = 4;

      if (!ligaId) return res.status(404).json({ error: 'Liga no encontrada' });

      const data = await db.getEstadisticasOfensivas(ligaId);
      

      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'estadisticas-ofensivas-ligue-one' && slug.length === 1) {
      
      const ligaId = 5;

      if (!ligaId) return res.status(404).json({ error: 'Liga no encontrada' });

      const data = await db.getEstadisticasOfensivas(ligaId);
      

      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'index' && slug.length === 1) {
      
      const ligaId = 1;

      if (!ligaId) return res.status(404).json({ error: 'Liga no encontrada' });

      const tabla_liga = await db.getTablaLiga(ligaId);
      const ultimos_partidos = await db.getUltimosPartidos(ligaId);
      const maximos_goleadores = await db.getMaximosGoleadores(ligaId);
      const mejores_valorados = await db.getMejoresValorados(ligaId);

      return res.status(200).json({ tabla_liga, ultimos_partidos, maximos_goleadores, mejores_valorados });
    }


    // Caso 1: Ruta para las ligas -> /api/liga/[nombre_liga]
    if (slug && slug[0] === 'liga' && slug.length === 2) {
      const nombreLiga = slug[1];
      const ligasMap = { 'premier-league': 1, 'la-liga': 2, 'serie-a': 3, 'bundesliga': 4, 'ligue-one': 5 };
      const ligaId = ligasMap[nombreLiga];

      if (!ligaId) return res.status(404).json({ error: 'Liga no encontrada' });

      const tabla_liga = await db.getTablaLiga(ligaId);
      const ultimos_partidos = await db.getUltimosPartidos(ligaId);
      const maximos_goleadores = await db.getMaximosGoleadores(ligaId);
      const mejores_valorados = await db.getMejoresValorados(ligaId);

      return res.status(200).json({ tabla_liga, ultimos_partidos, maximos_goleadores, mejores_valorados });
    }

    // Caso 2: Ruta para stats de jugador -> /api/estadisticas-jugador/[id]
    if (slug && slug[0] === 'estadisticas-jugador' && slug.length === 2) {
      const jugadorId = slug[1];
      const data = await db.getStatsJugador(jugadorId);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Jugador no encontrado" });
      
      return res.status(200).json(data[0]);
    }

    if (slug && slug[0] === 'mejores-goles' && slug.length === 2) {
      const ligaID = slug[1];
      const data = await db.getMejoresGoles(ligaID);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }
    
    if (slug && slug[0] === 'estadisticas-ofensivas-equipo' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getEstadisticasOfensivasEquipo(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'goles-esperados-equipo' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getXgPorEquipo(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'mapa-disparos' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getMapaDeDisparosEquipo(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'evolucion-equipos' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getEvolucionEquipos(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'promedios-liga' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getPromediosStatsDeUnaLiga(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }


    if (slug && slug[0] === 'resultado-partido' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getResultadoPartido(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'info-prepartido' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getInfoPrePartido(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }
    

    if (slug && slug[0] === 'alineaciones' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getPosiblesAlineaciones(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }


    if (slug && slug[0] === 'ultimos-enfrentamientos' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getUltimosEnfrentamientos(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }


    if (slug && slug[0] === 'estadisticas-equipo' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getEstadisticasEquipo(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'info-postpartido' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getInfoPostPartido(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }


    if (slug && slug[0] === 'estadisticas-partido' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getEstadisticasPartido(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'mapa-disparos-partido' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getMapaDeDisparosPartido(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'mapa-disparos-jugador' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getMapaDeDisparosJugador(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'mapa-calor-jugador' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getMapaDeCalorJugador(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'percentiles-jugador' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getPercentilesJugador(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }


    if (slug && slug[0] === 'ultimos-partidos-jugador' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getUltimosPartidosJugador(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'jugador' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getInfoJugador(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'ultimos-partidos-portero' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getUltimosPartidosPortero(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }


    if (slug && slug[0] === 'percentiles-portero' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getPercentilesPortero(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'estadisticas-portero' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getEstadisticasPortero(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data[0]);
    }

    if (slug && slug[0] === 'info-club' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getInfoClub(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data[0]);
    }

    if (slug && slug[0] === 'ultimos-partidos-club' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getUltimosPartidosClub(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'alineacion-club' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getAlineacionClub(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'plantilla-club' && slug.length === 2) {
      const id = slug[1];
      const data = await db.getPlantillaClub(id);
      
      if (!data || data.length === 0) return res.status(404).json({ error: "Data no encontrado" });
      
      return res.status(200).json(data);
    }


    // Caso 3: Ruta para max-stats -> /api/max-stats/[id1]/[id2]
    if (slug && slug[0] === 'max-stats' && slug.length === 3) {
      const id1 = slug[1];
      const id2 = slug[2];
      const data = await db.getStatsMaximas(id1, id2);

      return res.status(200).json(data);
    }


    if (slug && slug[0] === 'ultimos-partidos' && slug.length === 3) {
      const id1 = slug[1];
      const id2 = slug[2];
      const data = await db.getPartidos(id1, id2);

      return res.status(200).json(data);
    }

    if (slug && slug[0] === 'comparacion-evoluciones' && slug.length === 3) {
      const id1 = slug[1];
      const id2 = slug[2];
      const data = await db.getComparacionEvolucionEquipos(id1, id2);

      return res.status(200).json(data);
    }


    if (slug && slug[0] === 'comparacion-estadisticas-equipos' && slug.length === 3) {
      const id1 = slug[1];
      const id2 = slug[2];
      const data = await db.getComparacionStatsEquipos(id1, id2);

      return res.status(200).json(data);
    }

    // --- AÑADE AQUÍ MÁS 'if' PARA CADA UNO DE TUS OTROS ENDPOINTS ---

    

    // Si ninguna ruta coincide, devolvemos un error 404
    return res.status(404).json({ error: 'Endpoint no encontrado' });

  } catch (error) {
    console.error('Error en el router de la API:', error);
    res.status(500).json({ error: 'Error interno del servidor' });
  }
}