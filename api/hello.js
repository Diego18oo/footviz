// api/hello.js
export default function handler(req, res) {
  console.log("¡La función de prueba 'hello' se ha ejecutado!");
  res.status(200).json({ message: 'Hola desde la función de prueba!' });
}