/**
 * Genera l'HTML completo con MapLibre GL JS per la WebView.
 * La mappa è centrata su Piazza Maggiore, Bologna.
 * Comunicazione bidirezionale con React Native via postMessage.
 */
export function buildMapHtml() {
  return `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
  <script src="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js"></script>
  <link href="https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.css" rel="stylesheet">
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    html, body, #map { width: 100%; height: 100%; overflow: hidden; }

    .marker {
      width: 22px;
      height: 22px;
      border-radius: 50%;
      border: 2.5px solid white;
      box-shadow: 0 2px 6px rgba(0, 0, 0, 0.35);
      cursor: pointer;
      transition: transform 0.15s;
    }
    .marker:active {
      transform: scale(1.3);
    }

    /* Attribuzione compatta */
    .maplibregl-ctrl-attrib {
      font-size: 10px !important;
    }
  </style>
</head>
<body>
  <div id="map"></div>
  <script>
    // ---------------------------------------------------------------
    // Inizializza la mappa — centro: Piazza Maggiore, Bologna
    // ---------------------------------------------------------------
    const map = new maplibregl.Map({
      container: 'map',
      style: {
        version: 8,
        sources: {
          osm: {
            type: 'raster',
            tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'],
            tileSize: 256,
            attribution: '© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
          }
        },
        layers: [{ id: 'osm', type: 'raster', source: 'osm' }]
      },
      center: [11.3428, 44.4936],   // [lon, lat] Piazza Maggiore
      zoom: 14,
      attributionControl: { compact: true }
    });

    // ---------------------------------------------------------------
    // Gestione marker
    // ---------------------------------------------------------------
    const activeMarkers = [];

    function getColor(pct) {
      if (pct < 50) return '#4CAF50';   // verde  — meno del 50% occupato
      if (pct < 80) return '#FF9800';   // arancio — 50–80% occupato
      return '#F44336';                  // rosso  — oltre 80% occupato
    }

    function updateMarkers(parcheggi) {
      // Rimuovi tutti i marker precedenti
      activeMarkers.forEach(m => m.remove());
      activeMarkers.length = 0;

      parcheggi.forEach(function(p) {
        if (!p.coordinate) return;
        var lat = p.coordinate.lat;
        var lon = p.coordinate.lon;

        // Elemento DOM del marker
        var el = document.createElement('div');
        el.className = 'marker';
        el.style.backgroundColor = getColor(p.occupazione_pct);
        el.setAttribute('title', p.nome);

        // Al click invia il parcheggio a React Native
        el.addEventListener('click', function(e) {
          e.stopPropagation();
          try {
            window.ReactNativeWebView.postMessage(JSON.stringify(p));
          } catch (err) {
            console.warn('postMessage fallito:', err);
          }
        });

        var marker = new maplibregl.Marker({ element: el })
          .setLngLat([lon, lat])
          .addTo(map);

        activeMarkers.push(marker);
      });
    }

    // ---------------------------------------------------------------
    // Ricezione messaggi da React Native
    // ---------------------------------------------------------------
    function handleMessage(event) {
      try {
        var data = JSON.parse(event.data);
        if (data.type === 'UPDATE_MARKERS') {
          updateMarkers(data.parcheggi);
        }
      } catch (e) {
        console.warn('Messaggio non valido:', e);
      }
    }

    // Android usa document, iOS usa window
    window.addEventListener('message', handleMessage);
    document.addEventListener('message', handleMessage);
  </script>
</body>
</html>`;
}
