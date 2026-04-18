import React from 'react';
import { View, Text, TouchableOpacity, StyleSheet } from 'react-native';

function getColor(pct) {
  if (pct < 50) return '#4CAF50';
  if (pct < 80) return '#FF9800';
  return '#F44336';
}

function getLabel(pct) {
  if (pct < 50) return 'DISPONIBILE';
  if (pct < 80) return 'AFFOLLATO';
  return 'QUASI PIENO';
}

/**
 * Card in basso che mostra i dettagli del parcheggio selezionato.
 * Props:
 *   parcheggio — oggetto ParcheggioDisponibilita dall'API
 *   onClose    — callback per chiudere la card
 */
export default function ParcheggioCard({ parcheggio: p, onClose }) {
  const color = getColor(p.occupazione_pct);
  const pct = Math.min(Math.round(p.occupazione_pct), 100);

  return (
    <View style={styles.card}>
      {/* Handle visivo */}
      <View style={styles.handle} />

      {/* Header: badge stato + bottone chiudi */}
      <View style={styles.header}>
        <View style={[styles.badge, { backgroundColor: color }]}>
          <Text style={styles.badgeText}>{getLabel(p.occupazione_pct)}</Text>
        </View>
        <TouchableOpacity onPress={onClose} style={styles.closeBtn} hitSlop={12}>
          <Text style={styles.closeIcon}>✕</Text>
        </TouchableOpacity>
      </View>

      {/* Nome parcheggio */}
      <Text style={styles.name} numberOfLines={2}>
        {p.nome}
      </Text>

      {/* Statistiche: posti liberi / totali / occupazione% */}
      <View style={styles.statsRow}>
        <Stat value={p.posti_liberi} label="posti liberi" color={color} />
        <View style={styles.divider} />
        <Stat value={p.posti_totali} label="totali" />
        <View style={styles.divider} />
        <Stat value={`${pct}%`} label="occupazione" color={color} />
      </View>

      {/* Barra di occupazione */}
      <View style={styles.progressBg}>
        <View
          style={[
            styles.progressFill,
            { width: `${pct}%`, backgroundColor: color },
          ]}
        />
      </View>
    </View>
  );
}

function Stat({ value, label, color }) {
  return (
    <View style={styles.stat}>
      <Text style={[styles.statValue, color ? { color } : null]}>{value}</Text>
      <Text style={styles.statLabel}>{label}</Text>
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    position: 'absolute',
    bottom: 0,
    left: 0,
    right: 0,
    backgroundColor: '#fff',
    borderTopLeftRadius: 20,
    borderTopRightRadius: 20,
    paddingHorizontal: 20,
    paddingBottom: 28,
    paddingTop: 8,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: -3 },
    shadowOpacity: 0.12,
    shadowRadius: 10,
    elevation: 12,
  },
  handle: {
    width: 44,
    height: 4,
    backgroundColor: '#e0e0e0',
    borderRadius: 2,
    alignSelf: 'center',
    marginBottom: 14,
  },
  header: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: 10,
  },
  badge: {
    paddingHorizontal: 12,
    paddingVertical: 4,
    borderRadius: 14,
  },
  badgeText: {
    color: '#fff',
    fontSize: 11,
    fontWeight: '700',
    letterSpacing: 0.6,
  },
  closeBtn: {
    padding: 4,
  },
  closeIcon: {
    fontSize: 17,
    color: '#aaa',
  },
  name: {
    fontSize: 19,
    fontWeight: '700',
    color: '#111',
    marginBottom: 18,
    lineHeight: 25,
  },
  statsRow: {
    flexDirection: 'row',
    justifyContent: 'space-around',
    alignItems: 'center',
    marginBottom: 16,
  },
  stat: {
    flex: 1,
    alignItems: 'center',
  },
  statValue: {
    fontSize: 28,
    fontWeight: '800',
    color: '#1a1a1a',
  },
  statLabel: {
    fontSize: 11,
    color: '#999',
    marginTop: 2,
  },
  divider: {
    width: 1,
    height: 44,
    backgroundColor: '#f0f0f0',
  },
  progressBg: {
    height: 8,
    backgroundColor: '#f0f0f0',
    borderRadius: 4,
    overflow: 'hidden',
  },
  progressFill: {
    height: '100%',
    borderRadius: 4,
  },
});
