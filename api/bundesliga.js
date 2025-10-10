// Después - En api/premier-league.js
import { getTablaLiga,getUltimosPartidos,getMaximosGoleadores,getMejoresValorados } from '../database.js'; // Importamos la función

export default async function handler(req, res) {
  try {
    const tabla_liga = await getTablaLiga(4)
    const ultimos_partidos = await getUltimosPartidos(4)
    const maximos_goleadores = await getMaximosGoleadores(4)
    const mejores_valorados = await getMejoresValorados(4)
    res.status(200).json({ tabla_liga, ultimos_partidos, maximos_goleadores, mejores_valorados });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Error al obtener los datos' });
  }
}