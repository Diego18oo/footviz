import { getEstadisticasOfensivas } from '../database.js'; 

export default async function handler(req, res) {
  try {
    const data = await getEstadisticasOfensivas(2);
    res.status(200).json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Error al obtener los datos' });
  }
} 