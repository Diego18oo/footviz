// poblar_piezas.js
import { pool } from './database.js'; // 👈 Asegúrate que la ruta a tu database.js sea correcta

// Función para insertar la transformación en la URL de Cloudinary
function getUrlPieza(urlOriginal, gravedad) {
    // Definimos la transformación
    const transform = "w_0.5,h_0.5,c_crop,g_"; // g_ se completará
    
    // Partimos la URL en el 'upload/'
    const partes = urlOriginal.split('/upload/');
    if (partes.length !== 2) {
        console.error("URL de Cloudinary no válida:", urlOriginal);
        return null;
    }
    
    // Reconstruimos la URL con la transformación
    // partes[0] = https://res.cloudinary.com/donni11al/image
    // partes[1] = v1760204039/footviz/stadiums/stadium_Camp_Nou.png
    return `${partes[0]}/upload/${transform}${gravedad}/${partes[1]}`;
}

async function poblarTablaPiezas() {
    const conn = await pool.getConnection();
    try {
        console.log("Iniciando script para poblar piezas de rompecabezas...");

        // 1. Obtener todos los estadios
        const [estadios] = await conn.query("SELECT id_estadio, url_imagen, nombre FROM estadio WHERE url_imagen IS NOT NULL AND url_imagen != 'URL_DE_IMAGEN_POR_DEFECTO.PNG'");
        console.log(`Se encontraron ${estadios.length} estadios con imágenes válidas.`);

        for (const estadio of estadios) {
            console.log(`Procesando estadio: ${estadio.nombre} (ID: ${estadio.id_estadio})`);
            
            // 2. Definir las 4 piezas
            const piezas = [
                { nombre: "Pieza 1 (Sup-Izq)", url: getUrlPieza(estadio.url_imagen, "north_west") },
                { nombre: "Pieza 2 (Sup-Der)", url: getUrlPieza(estadio.url_imagen, "north_east") },
                { nombre: "Pieza 3 (Inf-Izq)", url: getUrlPieza(estadio.url_imagen, "south_west") },
                { nombre: "Pieza 4 (Inf-Der)", url: getUrlPieza(estadio.url_imagen, "south_east") }
            ];

            // 3. Insertar las 4 piezas para este estadio
            for (const pieza of piezas) {
                if (pieza.url) {
                    const sql = `
                        INSERT INTO pieza_rompecabezas (nombre, url_imagen, id_estadio)
                        VALUES (?, ?, ?)
                        ON DUPLICATE KEY UPDATE 
                            nombre = VALUES(nombre), 
                            url_imagen = VALUES(url_imagen)
                    `; // ON DUPLICATE previene errores si lo corres 2 veces
                    await conn.query(sql, [pieza.nombre, pieza.url, estadio.id_estadio]);
                }
            }
            console.log(`-> 4 piezas insertadas/actualizadas para el estadio ${estadio.id_estadio}`);
        }

        console.log("\n¡Script completado! Tabla 'pieza_rompecabezas' poblada.");

    } catch (error) {
        console.error("Error en el script:", error);
    } finally {
        conn.release();
        await pool.end(); // Cierra todas las conexiones del pool
    }
}

// Ejecutar el script
poblarTablaPiezas();