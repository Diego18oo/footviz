import express from 'express'
import {getTablaLiga, getUltimosPartidos, getMaximosGoleadores, getMejoresValorados, getEstadisticasOfensivas, getStatsJugador, buscarJugadores, getStatsMaximas, getMejoresGoles, getEstadisticasOfensivasEquipo, getXgPorEquipo, getMapaDeDisparosEquipo, getEvolucionEquipos, getPromediosStatsDeUnaLiga, getPartidos, getResultadoPartido, getInfoPrePartido, getPosiblesAlineaciones, getUltimosEnfrentamientos, getEstadisticasEquipo, getComparacionEvolucionEquipos, getComparacionStatsEquipos, getInfoPostPartido, getEstadisticasPartido, getMapaDeDisparosPartido, getMapaDeCalorJugador, getMapaDeDisparosJugador, getPercentilesJugador, getUltimosPartidosJugador, getInfoJugador, getUltimosPartidosPortero, getPercentilesPortero, getEstadisticasPortero, getInfoClub, getUltimosPartidosClub, getAlineacionClub, getPlantillaClub} from './database.js'
 

const app = express() 
app.use(express.static("public"))


app.get('/api/image-proxy', async (req, res) => {
  const imageUrl = req.query.url;

  if (!imageUrl) {
    return res.status(400).json({ error: 'Image URL is required' });
  }

  try {
    const imageResponse = await fetch(imageUrl, {
        headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://www.sofascore.com/' // Esta cabecera es muy efectiva
        }
    });

    if (!imageResponse.ok) {
      return res.status(imageResponse.status).json({ error: 'Failed to fetch image' });
    }

    const contentType = imageResponse.headers.get('content-type');
    const imageBuffer = await imageResponse.arrayBuffer();

    res.setHeader('Content-Type', contentType);
    res.send(Buffer.from(imageBuffer));

  } catch (error) {
    console.error('Local image proxy error:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
});


app.get("/api/plantilla-club/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getPlantillaClub(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del club" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del club" });
    }
});

app.get("/api/alineacion-club/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getAlineacionClub(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del club" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del club" });
    }
});

app.get("/api/ultimos-partidos-club/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getUltimosPartidosClub(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del club" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del club" });
    }
});

app.get("/api/info-club/:id", async(req, res) => {
    try {
        const { id } = req.params; 
        const infoClub = await getInfoClub(id);

        if (infoClub.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del club" });
        }

        res.json(infoClub[0]); // devuelves solo el club, no array
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del club" });
    }
})

app.get("/api/estadisticas-portero/:id", async(req, res) => {
    try {
        const { id } = req.params; // ejemplo: /estadisticas-jugador/45
        const estadisticas_jugador = await getEstadisticasPortero(id);

        if (estadisticas_jugador.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(estadisticas_jugador[0]); // devuelves solo el jugador, no array
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
})

app.get("/api/percentiles-portero/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getPercentilesPortero(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/ultimos-partidos-portero/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getUltimosPartidosPortero(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/jugador/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getInfoJugador(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/ultimos-partidos-jugador/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getUltimosPartidosJugador(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(info);   
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/percentiles-jugador/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getPercentilesJugador(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/mapa-calor-jugador/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getMapaDeCalorJugador(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/mapa-disparos-jugador/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getMapaDeDisparosJugador(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del partido" });
    }
});

app.get("/api/mapa-disparos-partido/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getMapaDeDisparosPartido(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del partido" });
    }
});

app.get("/api/estadisticas-partido/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getEstadisticasPartido(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del partido" });
    }
});

app.get("/api/info-postpartido/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getInfoPostPartido(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del partido" });
    }
});
app.get("/api/comparacion-estadisticas-equipos/:id1/:id2", async (req, res) => {
    try {
        const { id1 } = req.params; 
        const { id2 } = req.params; 
        const stats = await getComparacionStatsEquipos(id1, id2);

        if (stats.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion" });
        }
 
        res.json(stats); 
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener la informacion" });
    }
});

app.get("/api/comparacion-evoluciones/:id1/:id2", async (req, res) => {
    try {
        const { id1 } = req.params; 
        const { id2 } = req.params; 
        const evolucion = await getComparacionEvolucionEquipos(id1, id2);

        if (evolucion.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion" });
        }
 
        res.json(evolucion); 
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener la informacion" });
    }
});


app.get("/api/estadisticas-equipo/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getEstadisticasEquipo(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del partido" });
    }
});

app.get("/api/ultimos-enfrentamientos/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getUltimosEnfrentamientos(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del partido" });
    }
});

app.get("/api/alineaciones/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getPosiblesAlineaciones(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del partido" });
    }
});

app.get("/api/info-prepartido/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const info = await getInfoPrePartido(id);

        if (info.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(info);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del partido" });
    }
});


app.get("/api/resultado-partido/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const resultado = await getResultadoPartido(id);

        if (resultado.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del partido" });
        }

        res.json(resultado);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del partido" });
    }
});

app.get("/api/ultimos-partidos/:id1/:id2", async (req, res) => {
    try {
        const { id1 } = req.params; 
        const { id2 } = req.params; 
        const partidos = await getPartidos(id1, id2);

        if (partidos.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion" });
        }
 
        res.json(partidos); 
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener partidos" });
    }
});

app.get("/api/promedios-liga/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const promedios = await getPromediosStatsDeUnaLiga(id);

        if (promedios.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion de la liga seleccionada" });
        }

        res.json(promedios);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas de la liga seleccionada" });
    }
});

app.get("/api/evolucion-equipos/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const evolucion = await getEvolucionEquipos(id);

        if (evolucion.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del equipo" });
        }

        res.json(evolucion);  
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del equipo" });
    }
});

app.get("/api/mapa-disparos/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const mapaDisparos = await getMapaDeDisparosEquipo(id);

        if (mapaDisparos.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del equipo" });
        }

        res.json(mapaDisparos); 
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del equipo" });
    }
});

app.get("/api/goles-esperados-equipo/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const xgEquipo = await getXgPorEquipo(id);

        if (xgEquipo.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del equipo" });
        }

        res.json(xgEquipo); 
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas de los equipos" });
    }
});


app.get("/api/estadisticas-ofensivas-equipo/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const estadisticas_ofensivas_equipo = await getEstadisticasOfensivasEquipo(id);

        if (estadisticas_ofensivas_equipo.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion de la liga" });
        }

        res.json(estadisticas_ofensivas_equipo); 
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas de los equipos" });
    }
});

app.get("/api/mejores-goles/:id", async (req, res) => {
    try {
        const { id } = req.params; 
        const mejores_goles = await getMejoresGoles(id);

        if (mejores_goles.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }
  
        res.json(mejores_goles); 
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/max-stats/:id1/:id2", async (req, res) => {
    try {
        const { id1 } = req.params; 
        const { id2 } = req.params; 
        const max_stats = await getStatsMaximas(id1, id2);

        if (max_stats.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }
 
        res.json(max_stats); 
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
});

app.get("/api/buscar-jugadores", async (req, res) => {
    try {
        const { nombre } = req.query;

        if (!nombre) {
            return res.status(400).json({ error: "Falta el parámetro nombre" });
        }

        const jugadores = await buscarJugadores(nombre);

        res.json(jugadores);
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error en la búsqueda de jugadores" });
    }
}); 

app.get("/api/estadisticas-jugador/:id", async(req, res) => {
    try {
        const { id } = req.params; // ejemplo: /estadisticas-jugador/45
        const estadisticas_jugador = await getStatsJugador(id);

        if (estadisticas_jugador.length === 0) {
            return res.status(404).json({ error: "No hay suficiente informacion del jugador" });
        }

        res.json(estadisticas_jugador[0]); // devuelves solo el jugador, no array
    } catch (error) {
        console.error(error);
        res.status(500).json({ error: "Error al obtener estadísticas del jugador" });
    }
})

app.get("/api/estadisticas-ofensivas-premier-league", async(req, res) => {
    const estadisticas_ofensivas = await getEstadisticasOfensivas(1)
    res.json(estadisticas_ofensivas);
})

app.get("/api/estadisticas-ofensivas-la-liga", async(req, res) => {
    const estadisticas_ofensivas = await getEstadisticasOfensivas(2)
    res.json(estadisticas_ofensivas);
})
app.get("/api/estadisticas-ofensivas-serie-a", async(req, res) => {
    const estadisticas_ofensivas = await getEstadisticasOfensivas(3)
    res.json(estadisticas_ofensivas);
})

app.get("/api/estadisticas-ofensivas-bundesliga", async(req, res) => {
    const estadisticas_ofensivas = await getEstadisticasOfensivas(4)
    res.json(estadisticas_ofensivas);
})

app.get("/api/estadisticas-ofensivas-ligue-one", async(req, res) => {
    const estadisticas_ofensivas = await getEstadisticasOfensivas(5)
    res.json(estadisticas_ofensivas);
})
app.get("/api/index", async(req,res) => {
    console.log("Se hizo una solicitud a /index");
    const tabla_liga = await getTablaLiga(1)
    const ultimos_partidos = await getUltimosPartidos(1)
    const maximos_goleadores = await getMaximosGoleadores(1)
    const mejores_valorados = await getMejoresValorados(1)
    res.json({ tabla_liga, ultimos_partidos, maximos_goleadores, mejores_valorados });
    
})

app.get("/api/liga/premier-league", async(req,res) => {
    console.log("Se hizo una solicitud a /premier-league");
    const tabla_liga = await getTablaLiga(1)
    const ultimos_partidos = await getUltimosPartidos(1)
    const maximos_goleadores = await getMaximosGoleadores(1)
    const mejores_valorados = await getMejoresValorados(1)
    res.json({ tabla_liga, ultimos_partidos, maximos_goleadores, mejores_valorados });
    
})



app.get("/api/liga/la-liga", async(req,res) => {
    console.log("Se hizo una solicitud a /la-liga");
    const tabla_liga = await getTablaLiga(2) 
    const ultimos_partidos = await getUltimosPartidos(2)
    const maximos_goleadores = await getMaximosGoleadores(2)
    const mejores_valorados = await getMejoresValorados(2)
    res.json({ tabla_liga, ultimos_partidos, maximos_goleadores, mejores_valorados });
})

app.get("/api/liga/serie-a", async(req,res) => {
    console.log("Se hizo una solicitud a /serie-a");
    const tabla_liga = await getTablaLiga(3)
    const ultimos_partidos = await getUltimosPartidos(3)
    const maximos_goleadores = await getMaximosGoleadores(3)
    const mejores_valorados = await getMejoresValorados(3)
    res.json({ tabla_liga, ultimos_partidos, maximos_goleadores, mejores_valorados });
})

app.get("/api/liga/bundesliga", async(req,res) => {
    console.log("Se hizo una solicitud a /bundesliga");
    const tabla_liga = await getTablaLiga(4)
    const ultimos_partidos = await getUltimosPartidos(4)
    const maximos_goleadores = await getMaximosGoleadores(4)
    const mejores_valorados = await getMejoresValorados(4)
    res.json({ tabla_liga, ultimos_partidos, maximos_goleadores, mejores_valorados });
    
})

app.get("/api/liga/ligue-one", async(req,res) => {
    console.log("Se hizo una solicitud a /ligue-one");
    const tabla_liga = await getTablaLiga(5)
    const ultimos_partidos = await getUltimosPartidos(5)
    const maximos_goleadores = await getMaximosGoleadores(5)
    const mejores_valorados = await getMejoresValorados(5)
    console.log(tabla_liga)
    res.json({ tabla_liga, ultimos_partidos, maximos_goleadores, mejores_valorados });
})
  
 


app.use((err, req, res, next) => {
    console.error(err.stack)
    res.status(500).send("Something broke")
})

app.listen(8080, () => {
    console.log('Server is running on port 8080')
})