#!/usr/bin/env node
// Merge multiple result tree files with the same teams/weeks header.
// Usage: node scripts/merge-results.mjs <outputFile> <input1> <input2> [...]

import { createReadStream, createWriteStream } from 'fs';
import { createInterface } from 'readline';
import { isCompleteMarker, isIncompleteMarker, readHeaderFromFile } from '../lib/tree-format.mjs';

const args = process.argv.slice(2);
const outputFile = args[0];
const inputFiles = args.slice(1);

if (!outputFile || inputFiles.length < 2) {
	console.error('Usage: node scripts/merge-results.mjs <outputFile> <input1> <input2> [...]');
	process.exit(1);
}

const headers = [];
for (const file of inputFiles) {
	const header = await readHeaderFromFile(file);
	if (!header.teams || !header.weeks) {
		console.error(`Error: ${file} does not have a valid header`);
		process.exit(1);
	}
	headers.push({ file, ...header });
}

const { teams, weeks } = headers[0];
for (const header of headers.slice(1)) {
	if (header.teams !== teams || header.weeks !== weeks) {
		console.error('Error: input files must have the same teams/weeks');
		console.error(`  ${headers[0].file}: teams=${teams}, weeks=${weeks}`);
		console.error(`  ${header.file}: teams=${header.teams}, weeks=${header.weeks}`);
		process.exit(1);
	}
}

const mergedCount = headers.reduce((sum, h) => sum + (h.count || 0), 0);

async function scanFileForMarkers(file, onMarker) {
	const rl = createInterface({ input: createReadStream(file), crlfDelay: Infinity });
	const stack = [];

	for await (const line of rl) {
		if (line.startsWith('#') || !line.trim()) continue;

		let depth = 0;
		while (line[depth] === '\t') depth++;
		const content = line.slice(depth).trim();

		while (stack.length > depth) {
			stack.pop();
		}

		if (isIncompleteMarker(content) || isCompleteMarker(content)) {
			if (stack.length > 0) {
				const parentKey = stack.map(s => s.join(',')).join('|');
				onMarker(content, parentKey);
			}
			continue;
		}

		const parts = content.split(',');
		const schedule = [];
		let invalid = false;
		for (const part of parts) {
			const token = part.trim();
			if (token === '') {
				invalid = true;
				break;
			}
			const value = parseInt(token, 10);
			if (!Number.isFinite(value)) {
				invalid = true;
				break;
			}
			schedule.push(value);
		}
		if (invalid) continue;
		stack.push(schedule);
	}
}

const completeParentKeys = new Set();
for (const file of inputFiles) {
	await scanFileForMarkers(file, (content, parentKey) => {
		if (isCompleteMarker(content)) {
			completeParentKeys.add(parentKey);
		}
	});
}

let hasIncomplete = false;
for (const file of inputFiles) {
	await scanFileForMarkers(file, (content, parentKey) => {
		if (isIncompleteMarker(content) && !completeParentKeys.has(parentKey)) {
			hasIncomplete = true;
		}
	});
}

const outStream = createWriteStream(outputFile);
outStream.write(`# teams=${teams} weeks=${weeks} count=${mergedCount}${hasIncomplete ? ' (partial)' : ''}\n`);

for (const file of inputFiles) {
	const rl = createInterface({ input: createReadStream(file), crlfDelay: Infinity });
	let lineNum = 0;
	const stack = [];

	for await (const line of rl) {
		lineNum++;
		if (lineNum === 1) continue; // skip header
		if (!line.trim()) continue;

		let depth = 0;
		while (line[depth] === '\t') depth++;
		const content = line.slice(depth).trim();

		while (stack.length > depth) {
			stack.pop();
		}

		if (isIncompleteMarker(content) || isCompleteMarker(content)) {
			if (isIncompleteMarker(content) && stack.length > 0) {
				const parentKey = stack.map(s => s.join(',')).join('|');
				if (completeParentKeys.has(parentKey)) {
					continue;
				}
			}
			outStream.write(`${line}\n`);
			continue;
		}

		const parts = content.split(',');
		const schedule = [];
		let invalid = false;
		for (const part of parts) {
			const token = part.trim();
			if (token === '') {
				invalid = true;
				break;
			}
			const value = parseInt(token, 10);
			if (!Number.isFinite(value)) {
				invalid = true;
				break;
			}
			schedule.push(value);
		}
		outStream.write(`${line}\n`);
		if (invalid) continue;
		stack.push(schedule);
	}
}

await new Promise((resolve, reject) => {
	outStream.end(resolve);
	outStream.on('error', reject);
});
