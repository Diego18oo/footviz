// api/[[...slug]].js

export default function handler(req, res) {
  console.log("¡LA SUPER-FUNCIÓN [[...SLUG]].JS SE EJECUTÓ!");
  console.log("El objeto req.query es:", req.query);

  res.status(200).json({
    message: '¡La Super-Función [[...slug]].js está funcionando!',
    slugRecibido: req.query.slug || 'slug no definido'
  });
}