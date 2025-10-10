// api/max-stats/[id1]/[id2].js

// ¡OJO! La ruta al import cambia de nuevo porque estamos más anidados
import { getComparacionStatsEquipos } from '../../../database.js'; 

export default async function handler(req, res) {
  try {
    // Obtenemos AMBOS parámetros del objeto req.query
    const { id1, id2 } = req.query;

    const max_stats = await getComparacionStatsEquipos(id1, id2);

    if (!max_stats || max_stats.length === 0) {
        return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
    }

    res.status(200).json(max_stats); 

  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
  }
}