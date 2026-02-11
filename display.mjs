#!/usr/bin/env node
// Display schedules from saved .txt files

import { resolve } from 'path';
import { readFileSync } from 'fs';
import { COMPLETE_MARKER, INCOMPLETE_MARKER } from './lib/tree-format.mjs';

// Parse command-line args
const args = process.argv.slice(2);
let file = null, limit = 10, showMatchups = false, showGrid = true, scheduleNumber = null;

for (let i = 0; i < args.length; i++) {
	const arg = args[i];
	if (arg.startsWith('--limit=')) limit = parseInt(arg.slice(8));
	else if (arg === '--limit') limit = parseInt(args[++i]);
	else if (arg.startsWith('--schedule=')) scheduleNumber = parseInt(arg.slice(11));
	else if (arg === '--schedule') scheduleNumber = parseInt(args[++i]);
	else if (arg === '--matchups') showMatchups = true;
	else if (arg === '--no-grid') showGrid = false;
	else if (!arg.startsWith('--')) file = arg;
}

if (!file) {
	console.log('Usage: node display.mjs <file> [--limit N] [--schedule N] [--matchups] [--no-grid]');
	console.log('  --limit N    Show at most N schedules (default: 10, 0 for all)');
	console.log('  --schedule N Show a specific schedule number');
	console.log('  --matchups   Show matchup indices instead of team letters');
	console.log('  --no-grid    Hide the slot grid, show only summary');
	process.exit(1);
}

if (scheduleNumber !== null && (!Number.isInteger(scheduleNumber) || scheduleNumber < 1)) {
	console.error(`Invalid --schedule value: ${scheduleNumber}`);
	process.exit(1);
}

// Read file and parse shared header
const content = readFileSync(resolve(file), 'utf-8');
const lines = content.split('\n');

const header = lines[0];
const headerMatch = header.match(/^#\s*teams=(\d+)\s+weeks=(\d+)(?:\s+count=(\d+))?/);
if (!headerMatch) {
	console.error('Invalid file format: missing header');
	process.exit(1);
}
const N_TEAMS = parseInt(headerMatch[1]);
const N_WEEKS = parseInt(headerMatch[2]);
const HEADER_COUNT = headerMatch[3] ? parseInt(headerMatch[3]) : null;
const inputFormat = lines.some(line => /^Schedule\s+\d+\/\d+\s+::\s+Score:/.test(line)) ? 'best' : 'results';

const TEAMS = 'ABCDEFGHIJKLMNOP'.slice(0, N_TEAMS).split('');
const N_SLOTS = (N_TEAMS * 3) / 2;

// Precompute matchup decoding
const matchupToTeams = [];
const matchupIndexByToken = new Map();
for (let ti1 = 0; ti1 < N_TEAMS - 1; ti1++) {
	for (let ti2 = ti1 + 1; ti2 < N_TEAMS; ti2++) {
		const idx = matchupToTeams.length;
		matchupToTeams.push([ti1, ti2]);
		matchupIndexByToken.set(`${TEAMS[ti1]}v${TEAMS[ti2]}`, idx);
	}
}

function matchupToString(m) {
	const [t1, t2] = matchupToTeams[m];
	return `${TEAMS[t1]}v${TEAMS[t2]}`;
}

function isTreeNode(item) {
	return Array.isArray(item) && item.length === 2 && Array.isArray(item[0]) && Array.isArray(item[1]);
}

function isLeafNode(item) {
	return Array.isArray(item) && item.length === 1 && Array.isArray(item[0]);
}

function flattenTree(roots) {
	const results = [];
	const stack = [];

	for (let i = roots.length - 1; i >= 0; i--) {
		stack.push({ node: roots[i], path: [] });
	}

	while (stack.length > 0) {
		const { node, path } = stack.pop();
		if (isLeafNode(node)) {
			results.push(path.concat([node[0]]));
		} else if (isTreeNode(node)) {
			const [schedule, children] = node;
			const newPath = path.concat([schedule]);
			for (let i = children.length - 1; i >= 0; i--) {
				stack.push({ node: children[i], path: newPath });
			}
		}
	}

	return results;
}

function parseResultsFormat(fileLines) {
	let data = [];
	const stack = [{ children: data, depth: -1 }];

	for (let i = 1; i < fileLines.length; i++) {
		const line = fileLines[i];
		if (!line.trim()) continue;

		let depth = 0;
		while (depth < line.length && line[depth] === '\t') depth++;

		const trimmed = line.trim();
		if (trimmed === INCOMPLETE_MARKER || trimmed === COMPLETE_MARKER) continue;

		const schedule = trimmed.split(',').map(Number);
		while (stack.length > 1 && stack[stack.length - 1].depth >= depth) {
			stack.pop();
		}
		const parent = stack[stack.length - 1];
		const node = [schedule, []];
		parent.children.push(node);
		stack.push({ children: node[1], depth });
	}

	function cleanTree(nodes) {
		return nodes.map(node => {
			if (node[1].length === 0) {
				return [node[0]];
			}
			return [node[0], cleanTree(node[1])];
		});
	}

	data = cleanTree(data);
	const flattened = flattenTree(data);
	return flattened.map((weeks, idx) => ({
		scheduleNum: idx + 1,
		totalSchedules: HEADER_COUNT,
		weeks,
	}));
}

function parseBestFormat(fileLines) {
	const schedules = [];
	let current = null;
	for (const line of fileLines) {
		const scheduleMatch = line.match(/^Schedule\s+(\d+)\/(\d+)\s+::\s+Score:/);
		if (scheduleMatch) {
			if (current && current.weeks.length > 0) schedules.push(current);
			current = {
				scheduleNum: parseInt(scheduleMatch[1]),
				totalSchedules: parseInt(scheduleMatch[2]),
				weeks: [],
			};
			continue;
		}
		const weekMatch = line.match(/^Week\s+\d+:\s+(.+)$/);
		if (weekMatch && current) {
			const tokens = weekMatch[1].trim().split(/\s+/).filter(Boolean);
			const week = tokens.map(token => {
				const idx = matchupIndexByToken.get(token);
				if (idx === undefined) {
					throw new Error(`Unknown matchup token "${token}" in best file`);
				}
				return idx;
			});
			current.weeks.push(week);
		}
	}
	if (current && current.weeks.length > 0) schedules.push(current);

	return schedules.filter(item => item.weeks.length === N_WEEKS && item.weeks.every(week => week.length === N_SLOTS));
}

function formatCountLabel(count, singular, plural) {
	if (count <= 0) return '-';
	if (count === 1) return singular;
	return `${count} ${plural}`;
}

function formatTimingLabel(hasEarly, hasLate) {
	if (hasEarly && hasLate) return 'early+late';
	if (hasEarly) return 'early';
	if (hasLate) return 'late';
	return '-';
}

function buildWeekStats(matchups) {
	const grid = [];
	const slotsByTeam = [];
	for (let ti = 0; ti < N_TEAMS; ti++) {
		grid.push(new Array(N_SLOTS).fill('.'));
		slotsByTeam.push([]);
	}

	for (let slot = 0; slot < matchups.length; slot++) {
		const [t1, t2] = matchupToTeams[matchups[slot]];
		grid[t1][slot] = TEAMS[t2];
		grid[t2][slot] = TEAMS[t1];
		slotsByTeam[t1].push(slot);
		slotsByTeam[t2].push(slot);
	}

	const spans = new Array(N_TEAMS).fill(0);
	const doubleHeaders = new Array(N_TEAMS).fill(0);
	const doubleByes = new Array(N_TEAMS).fill(0);
	const early = new Array(N_TEAMS).fill(0);
	const late = new Array(N_TEAMS).fill(0);
	const thirdVs2nd = new Array(N_TEAMS).fill(0);
	const earlyCutoff = Math.min(2, N_SLOTS);
	const lateStart = Math.max(0, N_SLOTS - 2);

	for (let ti = 0; ti < N_TEAMS; ti++) {
		const slots = slotsByTeam[ti];
		if (slots.length > 0) {
			spans[ti] = slots[slots.length - 1] - slots[0] + 1;
		}
		for (let i = 0; i < slots.length - 1; i++) {
			const gap = slots[i + 1] - slots[i];
			if (gap === 1) doubleHeaders[ti]++;
			if (gap === 3) doubleByes[ti]++;
		}
		early[ti] = slots.some(slot => slot < earlyCutoff) ? 1 : 0;
		late[ti] = slots.some(slot => slot >= lateStart) ? 1 : 0;
		if (slots.length >= 3) {
			const my3rdSlot = slots[2];
			const matchupIdx = matchups[my3rdSlot];
			const [t1, t2] = matchupToTeams[matchupIdx];
			const opponent = t1 === ti ? t2 : t1;
			const opponentSlots = slotsByTeam[opponent];
			if (opponentSlots.length >= 2 && opponentSlots[1] === my3rdSlot) {
				thirdVs2nd[ti] = 1;
			}
		}
	}

	return { grid, spans, doubleHeaders, doubleByes, early, late, thirdVs2nd };
}

function formatWeek(matchups, weekNum1Based, weekStats) {
	const lines = [];
	const header = `Week ${weekNum1Based}:`;

	if (showMatchups) {
		lines.push(`${header} [${matchups.join(',')}]`);
	} else {
		lines.push(`${header} ${matchups.map(matchupToString).join(' ')}`);
	}

	if (showGrid) {
		for (let ti = 0; ti < N_TEAMS; ti++) {
			const spanLabel = `${weekStats.spans[ti]} slots`;
			const doubleHeaderLabel = formatCountLabel(weekStats.doubleHeaders[ti], 'double-header', 'double-headers').padEnd(14);
			const doubleByeLabel = formatCountLabel(weekStats.doubleByes[ti], 'double-bye', 'double-byes').padEnd(12);
			const thirdVs2ndLabel = (weekStats.thirdVs2nd[ti] === 1 ? '3v2' : '-').padEnd(3);
			const timingLabel = formatTimingLabel(weekStats.early[ti] === 1, weekStats.late[ti] === 1);
			lines.push(`  ${TEAMS[ti]}: ${weekStats.grid[ti].join(' ')}  ${spanLabel.padStart(7)}  ${doubleHeaderLabel}  ${doubleByeLabel}  ${thirdVs2ndLabel}  ${timingLabel}`);
		}
	}

	return lines.join('\n');
}

function formatOverall(weeks, allWeekStats) {
	const matchupCounts = Array.from({ length: N_TEAMS }, () => new Array(N_TEAMS).fill(0));
	const totalSlots = new Array(N_TEAMS).fill(0);
	const totalDoubleHeaders = new Array(N_TEAMS).fill(0);
	const totalDoubleByes = new Array(N_TEAMS).fill(0);
	const totalEarly = new Array(N_TEAMS).fill(0);
	const totalLate = new Array(N_TEAMS).fill(0);
	const totalThirdVs2nd = new Array(N_TEAMS).fill(0);

	for (let w = 0; w < weeks.length; w++) {
		for (const matchupIdx of weeks[w]) {
			const [t1, t2] = matchupToTeams[matchupIdx];
			matchupCounts[t1][t2]++;
			matchupCounts[t2][t1]++;
		}
		const stats = allWeekStats[w];
		for (let ti = 0; ti < N_TEAMS; ti++) {
			totalSlots[ti] += stats.spans[ti];
			totalDoubleHeaders[ti] += stats.doubleHeaders[ti];
			totalDoubleByes[ti] += stats.doubleByes[ti];
			totalEarly[ti] += stats.early[ti];
			totalLate[ti] += stats.late[ti];
			totalThirdVs2nd[ti] += stats.thirdVs2nd[ti];
		}
	}

	const lines = [];
	lines.push('Overall:');
	lines.push(`  ${TEAMS.join(' ')}`);
	for (let ti = 0; ti < N_TEAMS; ti++) {
		const matchupRow = matchupCounts[ti].map((count, otherTi) => (ti === otherTi ? '·' : count)).join(' ');
		lines.push(
			`${TEAMS[ti]} ${matchupRow}  ${totalSlots[ti]}×slots  ${totalDoubleHeaders[ti]}×double-headers  ${totalDoubleByes[ti]}×double-byes  ${totalThirdVs2nd[ti]}×3v2  ${totalEarly[ti]}×early  ${totalLate[ti]}×late`
		);
	}
	return lines.join('\n');
}

const schedules = inputFormat === 'best' ? parseBestFormat(lines) : parseResultsFormat(lines);
if (schedules.length === 0) {
	console.error(`No schedules found in file (${inputFormat} format)`);
	process.exit(1);
}

// Display header info
console.log(`File: ${file}`);
console.log(`Format: ${inputFormat}`);
console.log(`Teams: ${N_TEAMS} (${TEAMS.join('')}), Slots: ${N_SLOTS}`);
console.log(`Schedules in file: ${schedules.length}`);
console.log();

const schedulesToDisplay = (() => {
	if (scheduleNumber !== null) {
		const found = schedules.find(item => item.scheduleNum === scheduleNumber);
		if (!found) {
			console.error(`Schedule ${scheduleNumber} not found in ${file}`);
			process.exit(1);
		}
		return [found];
	}
	const toShow = limit === 0 ? schedules.length : Math.min(limit, schedules.length);
	return schedules.slice(0, toShow);
})();

for (const schedule of schedulesToDisplay) {
	const weeks = schedule.weeks;
	const scheduleLabel = schedule.totalSchedules
		? `Schedule ${schedule.scheduleNum}/${schedule.totalSchedules}`
		: `Schedule ${schedule.scheduleNum}`;
	console.log(`=== ${scheduleLabel} ===`);
	for (const week of weeks) {
		console.log(week.map(matchupToString).join(' '));
	}
	console.log();

	const allWeekStats = [];
	for (let w = 0; w < weeks.length; w++) {
		const weekStats = buildWeekStats(weeks[w]);
		allWeekStats.push(weekStats);
		console.log(formatWeek(weeks[w], w + 1, weekStats));
		console.log();
	}

	if (showGrid) {
		console.log(formatOverall(weeks, allWeekStats));
		console.log();
	}
}

if (scheduleNumber === null && schedulesToDisplay.length < schedules.length) {
	console.log(`... and ${schedules.length - schedulesToDisplay.length} more. Use --limit=0 to show all.`);
}
