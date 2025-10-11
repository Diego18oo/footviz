// api/[...slug].js
export default function handler(req, res) {
  res.status(200).json({ 
    message: 'El archivo [...slug].js SÍ está funcionando.',
    query: req.query 
  });
}