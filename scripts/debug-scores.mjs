import { readFileSync } from 'fs';

// Load 1-week optimal schedules
const content = readFileSync('results/6teams-1week.txt', 'utf8');
const lines = content.trim().split('\n').filter(l => l && !l.startsWith('#'));
console.log('1-week paths:', lines.length);

// Check score distribution of week1 schedules
const N_TEAMS = 6;
const N_SLOTS = 9;
const matchupToTeams = new Uint8Array(15 * 2);
let idx = 0;
for (let ti1 = 0; ti1 < N_TEAMS - 1; ti1++) {
  for (let ti2 = ti1 + 1; ti2 < N_TEAMS; ti2++) {
    matchupToTeams[idx * 2] = ti1;
    matchupToTeams[idx * 2 + 1] = ti2;
    idx++;
  }
}

function scoreSchedule(schedule) {
  const slots = [];
  for (let ti = 0; ti < N_TEAMS; ti++) slots.push([]);
  for (let s = 0; s < N_SLOTS; s++) {
    const m = schedule[s];
    slots[matchupToTeams[m * 2]].push(s);
    slots[matchupToTeams[m * 2 + 1]].push(s);
  }
  let doubleByes = 0, fiveSlotTeams = 0;
  for (let ti = 0; ti < N_TEAMS; ti++) {
    const ts = slots[ti];
    if (ts.length !== 3) continue;
    if (ts[2] - ts[0] + 1 === 5) fiveSlotTeams++;
    for (let i = 0; i < 2; i++) if (ts[i + 1] - ts[i] === 3) doubleByes++;
  }
  return [doubleByes, fiveSlotTeams];
}

const scores = {};
for (const line of lines) {
  const sched = line.split(',').map(Number);
  const [db, fs] = scoreSchedule(sched);
  const key = `${db},${fs}`;
  scores[key] = (scores[key] || 0) + 1;
}
console.log('Score distribution of 1-week schedules:');
for (const [k, v] of Object.entries(scores)) console.log(`  [${k}]:`, v);
