// Después - En api/premier-league.js
import { getTablaLiga,getUltimosPartidos,getMaximosGoleadores,getMejoresValorados } from '../database.js'; // Importamos la función

export default async function handler(req, res) {
  try {
    const tabla_liga = await getTablaLiga(3)
    const ultimos_partidos = await getUltimosPartidos(3)
    const maximos_goleadores = await getMaximosGoleadores(3)
    const mejores_valorados = await getMejoresValorados(3)
    res.status(200).json({ tabla_liga, ultimos_partidos, maximos_goleadores, mejores_valorados });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Error al obtener los datos' });
  }
}