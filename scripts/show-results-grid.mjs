#!/usr/bin/env node
// Show counts per week depth across results files
// Usage: node scripts/show-results-grid.mjs [6|8|all] [--results <dir>] [--no-drops]

import { readdirSync, createReadStream } from 'fs';
import { createInterface } from 'readline';
import { isCompleteMarker, isIncompleteMarker, readHeaderFromFile } from '../lib/tree-format.mjs';

const args = process.argv.slice(2);
const resultsDir = getArgValue(args, '--results', 'results');
const showDrops = !args.includes('--no-drops');
const teamArg = args.find(arg => !arg.startsWith('--')) || 'all';

const teamsList = teamArg === 'all'
	? [6, 8]
	: teamArg.split(',').map(value => parseInt(value.trim(), 10)).filter(Number.isFinite);

if (teamsList.length === 0) {
	console.error('Usage: node scripts/show-results-grid.mjs [6|8|all] [--results <dir>] [--no-drops]');
	process.exit(1);
}

function getArgValue(list, name, fallback) {
	const idx = list.indexOf(name);
	if (idx === -1) return fallback;
	const value = list[idx + 1];
	return value && !value.startsWith('--') ? value : fallback;
}

async function countNodesPerDepth(filePath) {
	const header = await readHeaderFromFile(filePath);
	const counts = new Array(header.weeks).fill(0);

	const rl = createInterface({
		input: createReadStream(filePath),
		crlfDelay: Infinity
	});

	for await (const line of rl) {
		if (line.startsWith('#') || !line.trim()) continue;
		let depth = 0;
		while (line[depth] === '\t') depth++;
		const content = line.slice(depth).trim();
		if (content === '' || isIncompleteMarker(content) || isCompleteMarker(content)) continue;
		if (depth < counts.length) counts[depth]++;
	}

	return { header, counts };
}

function formatCount(value) {
	return value.toLocaleString();
}

function buildTable(rows, maxWeeks, showDropMarks) {
	const headerCells = [''].concat(
		Array.from({ length: maxWeeks }, (_, idx) => `week ${idx + 1}`)
	);

	const tableRows = [
		headerCells
	];

	const maxSoFar = new Array(maxWeeks).fill(0);

	for (const row of rows) {
		const cells = new Array(maxWeeks).fill('');
		for (let i = 0; i < row.counts.length; i++) {
			let value = formatCount(row.counts[i]);
			if (showDropMarks && row.counts[i] < maxSoFar[i]) {
				const delta = maxSoFar[i] - row.counts[i];
				value = `${value} (↓${formatCount(delta)})`;
			}
			cells[i] = value;
			if (row.counts[i] > maxSoFar[i]) maxSoFar[i] = row.counts[i];
		}
		tableRows.push([row.label].concat(cells));
	}

	const colWidths = [];
	for (const row of tableRows) {
		row.forEach((cell, idx) => {
			const width = cell.length;
			colWidths[idx] = Math.max(colWidths[idx] || 0, width);
		});
	}

	return tableRows
		.map(row => row.map((cell, idx) => cell.padEnd(colWidths[idx])).join('  ').trimEnd())
		.join('\n');
}

async function loadTeamRows(teams) {
	const pattern = new RegExp(`^${teams}teams-(\\d+)weeks?\\.txt$`);
	const files = readdirSync(resultsDir).filter(file => pattern.test(file));

	const rows = [];
	for (const file of files) {
		const filePath = `${resultsDir}/${file}`;
		const { header, counts } = await countNodesPerDepth(filePath);
		if (header.teams !== teams) continue;
		const label = header.partial ? `week ${header.weeks} (partial)` : `week ${header.weeks}`;
		rows.push({ filePath, weeks: header.weeks, label, counts });
	}

	rows.sort((a, b) => a.weeks - b.weeks);
	return rows;
}

for (const teams of teamsList) {
	const rows = await loadTeamRows(teams);
	if (rows.length === 0) {
		console.log(`${teams} teams: no results found in ${resultsDir}`);
		if (teams !== teamsList[teamsList.length - 1]) console.log('');
		continue;
	}

	const maxWeeks = Math.max(...rows.map(row => row.weeks));
	console.log(`${teams} teams`);
	console.log(buildTable(rows, maxWeeks, showDrops));
	if (showDrops) {
		console.log('Counts with ↓ indicate a decrease from earlier files');
	}
	if (teams !== teamsList[teamsList.length - 1]) console.log('');
}
