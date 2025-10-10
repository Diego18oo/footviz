import { buscarJugadores } from '../database.js'; 

export default async function handler(req, res) {
  try {
    const { nombre } = req.query;

    if (!nombre) {
        return res.status(400).json({ error: "Falta el parámetro nombre" });
    }
    const data = await buscarJugadores(nombre);
    res.status(200).json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Error al obtener los datos' });
  }
} 