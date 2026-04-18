/**
 * Bologna Parking — Fase 2: Frontend React Native + Expo
 *
 * Mappa MapLibre GL JS (via WebView) con marker colorati dei parcheggi.
 * Dati in tempo reale dall'endpoint locale FastAPI.
 */

import React, { useState, useRef, useEffect, useCallback } from 'react';
import {
  StyleSheet,
  View,
  Text,
  TouchableOpacity,
  ActivityIndicator,
  SafeAreaView,
} from 'react-native';
import { StatusBar } from 'expo-status-bar';
import { WebView } from 'react-native-webview';
import Constants from 'expo-constants';

import { buildMapHtml } from './src/mapHtml';
import ParcheggioCard from './src/ParcheggioCard';

// ---------------------------------------------------------------------------
// Rilevamento automatico IP del backend
// Expo Go inserisce l'IP del PC in hostUri → lo usiamo per raggiungere
// il server FastAPI dalla LAN (localhost non funziona sul telefono fisico).
// ---------------------------------------------------------------------------
function getApiBase() {
  const hostUri = Constants.expoConfig?.hostUri ?? '';
  if (hostUri) {
    const ip = hostUri.split(':')[0];
    if (ip && ip !== 'localhost' && ip !== '127.0.0.1') {
      return `http://${ip}:8000`;
    }
  }
  // Fallback: emulatore Android (10.0.2.2 = host machine)
  return 'http://10.0.2.2:8000';
}

const API_BASE = getApiBase();
const REFRESH_INTERVAL_MS = 60_000; // aggiorna ogni 60 s

// ---------------------------------------------------------------------------
// App principale
// ---------------------------------------------------------------------------
export default function App() {
  const [parcheggi, setParcheggi] = useState([]);
  const [selected, setSelected] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [lastUpdate, setLastUpdate] = useState(null);

  const webviewRef = useRef(null);
  // Ref per accedere ai dati correnti dentro callback senza dipendenze stale
  const parcheggiRef = useRef([]);
  const webviewReady = useRef(false);

  // Invia i marker alla mappa WebView
  const sendMarkersToMap = useCallback((data) => {
    if (webviewRef.current && webviewReady.current) {
      webviewRef.current.postMessage(
        JSON.stringify({ type: 'UPDATE_MARKERS', parcheggi: data })
      );
    }
  }, []);

  // Fetch dati dall'API
  const fetchData = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/parcheggi/disponibilita?limit=100`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();

      parcheggiRef.current = data;
      setParcheggi(data);
      setError(null);
      setLastUpdate(new Date());
      sendMarkersToMap(data);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [sendMarkersToMap]);

  // Fetch iniziale + polling ogni minuto
  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, REFRESH_INTERVAL_MS);
    return () => clearInterval(interval);
  }, [fetchData]);

  // Quando la WebView ha terminato il caricamento invia i dati
  const onWebViewLoad = useCallback(() => {
    webviewReady.current = true;
    if (parcheggiRef.current.length > 0 && webviewRef.current) {
      webviewRef.current.postMessage(
        JSON.stringify({ type: 'UPDATE_MARKERS', parcheggi: parcheggiRef.current })
      );
    }
  }, []);

  // Ricezione messaggio dalla WebView (marker toccato)
  const onMessage = useCallback((event) => {
    try {
      const p = JSON.parse(event.nativeEvent.data);
      setSelected(p);
    } catch {}
  }, []);

  const formatTime = (date) =>
    date.toLocaleTimeString('it-IT', { hour: '2-digit', minute: '2-digit' });

  return (
    <SafeAreaView style={styles.container}>
      <StatusBar style="dark" />

      {/* ── Header ─────────────────────────────────────────────────── */}
      <View style={styles.header}>
        <Text style={styles.title}>Bologna Parking</Text>
        <TouchableOpacity
          onPress={fetchData}
          style={styles.refreshBtn}
          disabled={loading}
          hitSlop={8}
        >
          {loading ? (
            <ActivityIndicator size="small" color="#E91E63" />
          ) : (
            <Text style={styles.refreshIcon}>↻</Text>
          )}
        </TouchableOpacity>
      </View>

      {/* ── Barra di stato / errore ─────────────────────────────────── */}
      {error ? (
        <View style={styles.errorBar}>
          <Text style={styles.errorText}>⚠ {error}</Text>
          <Text style={styles.errorHint}>Backend su: {API_BASE}</Text>
        </View>
      ) : lastUpdate ? (
        <View style={styles.statusBar}>
          <View style={styles.legendRow}>
            <LegendDot color="#4CAF50" label="libero" />
            <LegendDot color="#FF9800" label="medio" />
            <LegendDot color="#F44336" label="pieno" />
          </View>
          <Text style={styles.updateText}>
            {parcheggi.length} parcheggi · {formatTime(lastUpdate)}
          </Text>
        </View>
      ) : null}

      {/* ── Mappa MapLibre (WebView) ────────────────────────────────── */}
      <WebView
        ref={webviewRef}
        source={{ html: buildMapHtml() }}
        style={styles.map}
        onMessage={onMessage}
        onLoadEnd={onWebViewLoad}
        javaScriptEnabled
        domStorageEnabled
        mixedContentMode="always"
        allowFileAccess
        originWhitelist={['*']}
      />

      {/* ── Overlay caricamento iniziale ────────────────────────────── */}
      {loading && parcheggi.length === 0 && (
        <View style={styles.loadingOverlay}>
          <ActivityIndicator size="large" color="#E91E63" />
          <Text style={styles.loadingText}>Caricamento parcheggi…</Text>
        </View>
      )}

      {/* ── Card dettaglio parcheggio selezionato ───────────────────── */}
      {selected && (
        <ParcheggioCard
          parcheggio={selected}
          onClose={() => setSelected(null)}
        />
      )}
    </SafeAreaView>
  );
}

// ---------------------------------------------------------------------------
// Componente legenda
// ---------------------------------------------------------------------------
function LegendDot({ color, label }) {
  return (
    <View style={styles.legendItem}>
      <View style={[styles.dot, { backgroundColor: color }]} />
      <Text style={styles.legendLabel}>{label}</Text>
    </View>
  );
}

// ---------------------------------------------------------------------------
// Stili
// ---------------------------------------------------------------------------
const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: '#fff',
  },

  // Header
  header: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'space-between',
    paddingHorizontal: 16,
    paddingVertical: 10,
    backgroundColor: '#fff',
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: '#ddd',
  },
  title: {
    fontSize: 18,
    fontWeight: '700',
    color: '#1a1a1a',
  },
  refreshBtn: {
    width: 36,
    height: 36,
    justifyContent: 'center',
    alignItems: 'center',
  },
  refreshIcon: {
    fontSize: 24,
    color: '#E91E63',
  },

  // Status bar
  statusBar: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    paddingHorizontal: 12,
    paddingVertical: 6,
    backgroundColor: '#fafafa',
    borderBottomWidth: StyleSheet.hairlineWidth,
    borderBottomColor: '#eee',
  },
  legendRow: {
    flexDirection: 'row',
    gap: 10,
  },
  legendItem: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 4,
  },
  dot: {
    width: 10,
    height: 10,
    borderRadius: 5,
  },
  legendLabel: {
    fontSize: 11,
    color: '#666',
  },
  updateText: {
    fontSize: 11,
    color: '#aaa',
  },

  // Error bar
  errorBar: {
    backgroundColor: '#ffebee',
    paddingHorizontal: 16,
    paddingVertical: 8,
  },
  errorText: {
    color: '#c62828',
    fontSize: 13,
    fontWeight: '600',
  },
  errorHint: {
    color: '#e57373',
    fontSize: 11,
    marginTop: 2,
  },

  // Map
  map: {
    flex: 1,
  },

  // Loading overlay (sopra la mappa, sotto l'header)
  loadingOverlay: {
    ...StyleSheet.absoluteFillObject,
    top: 90,
    backgroundColor: 'rgba(255,255,255,0.88)',
    justifyContent: 'center',
    alignItems: 'center',
  },
  loadingText: {
    marginTop: 14,
    fontSize: 15,
    color: '#555',
  },
});
