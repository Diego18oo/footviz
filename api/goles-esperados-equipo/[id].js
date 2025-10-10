import { getXgPorEquipo } from '../../database.js'; 

export default async function handler(req, res) {
  try {
    const { id } = req.query;
    const data = await getXgPorEquipo(id);
    res.status(200).json(data);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Error al obtener los datos' });
  }
}  