const API_URL = 'http://127.0.0.1:8000';

export async function getPastRankings(week: number) {
  const res = await fetch(`${API_URL}/rankings/past/${week}`);
  if (!res.ok) return [];
  return res.json();
}

export async function getFutureRankings(week: number) {
  const res = await fetch(`${API_URL}/rankings/future/${week}`);
  if (!res.ok) return [];
  return res.json();
}

export async function getAllPlayers() {
  const res = await fetch(`${API_URL}/players/all`);
  if (!res.ok) return [];
  return res.json();
}

export async function predictPlayer(playerName: string, week: number) {
  const res = await fetch(`${API_URL}/predict`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ player_name: playerName, week }),
  });
  if (!res.ok) throw new Error('Prediction failed');
  return res.json();
}