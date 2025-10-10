import { getInfoClub } from '../../database.js'; 

export default async function handler(req, res) {
  try {
    const { id } = req.query;
    const data = await getInfoClub(id);
    res.status(200).json(data[0]);
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Error al obtener los datos' });
  }
}  