// api/image-proxy.js

export default async function handler(req, res) {
  // Obtenemos la URL de la imagen del parámetro "url" de la petición
  const imageUrl = req.query.url;

  if (!imageUrl) {
    return res.status(400).json({ error: 'Image URL is required' });
  }

  try {
    // Hacemos la petición a la URL de la imagen externa (SofaScore)
    const imageResponse = await fetch(imageUrl);

    // Si SofaScore no nos da una respuesta exitosa, devolvemos el error
    if (!imageResponse.ok) {
      return res.status(imageResponse.status).json({ error: 'Failed to fetch image' });
    }

    // Obtenemos el tipo de contenido (ej. 'image/png') de la respuesta original
    const contentType = imageResponse.headers.get('content-type');
    
    // Le decimos al navegador del usuario qué tipo de imagen le estamos enviando
    res.setHeader('Content-Type', contentType);
    
    // Devolvemos el contenido de la imagen
    const imageBuffer = await imageResponse.arrayBuffer();
    res.send(Buffer.from(imageBuffer));

  } catch (error) {
    console.error('Image proxy error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
}