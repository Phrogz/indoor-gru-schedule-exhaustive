// Analyze parent/child counts in streaming tree files

import { createReadStream } from 'fs';
import { createInterface } from 'readline';
import { isCompleteMarker, isIncompleteMarker, readHeaderFromFile } from './tree-format.mjs';

function parseSchedule(content) {
	const parts = content.split(',');
	const schedule = [];
	for (const part of parts) {
		const token = part.trim();
		if (token === '') return null;
		const value = parseInt(token, 10);
		if (!Number.isFinite(value)) return null;
		schedule.push(value);
	}
	return schedule;
}

function parentKeyFromStack(stack, parentDepth) {
	if (stack.length < parentDepth + 1) return null;
	return stack.slice(0, parentDepth + 1).map(s => s.join(',')).join('|');
}

/**
 * Collect child counts per parent path, merging across file chunks.
 * Marks parents as partial if any incomplete marker appears under them,
 * unless a complete marker is present for that parent.
 * @param {string} filepath
 * @param {number} parentDepth
 * @param {object} [options]
 * @param {boolean} [options.includePartials=false]
 * @returns {Promise<Map<string, {childCount: number, partial: boolean}>>}
 */
export async function collectParentStats(filepath, parentDepth, options = {}) {
	const { includePartials = false } = options;
	const header = await readHeaderFromFile(filepath);
	const leafDepth = header.weeks - 1;

	const rl = createInterface({
		input: createReadStream(filepath),
		crlfDelay: Infinity
	});

	const childDepth = parentDepth + 1;
	const stats = new Map();
	const stack = [];

	for await (const line of rl) {
		if (line.startsWith('#') || !line.trim()) continue;

		let depth = 0;
		while (line[depth] === '\t') depth++;

		const content = line.slice(depth).trim();

		// Pop stack to current depth
		while (stack.length > depth) {
			stack.pop();
		}

		// Marker line
		if (isIncompleteMarker(content) || isCompleteMarker(content) || content === '') {
			if (depth === leafDepth) {
				const parentKey = parentKeyFromStack(stack, parentDepth);
				if (parentKey) {
					const entry = stats.get(parentKey) || { childCount: 0, partial: false, complete: false };
					if (isCompleteMarker(content)) {
						entry.complete = true;
						entry.partial = false;
					} else if (isIncompleteMarker(content) && !entry.complete) {
						entry.partial = true;
					}
					stats.set(parentKey, entry);
				}
			}
			continue;
		}

		const schedule = parseSchedule(content);
		if (!schedule) continue;
		stack.push(schedule);

		if (depth === childDepth) {
			const parentKey = parentKeyFromStack(stack, parentDepth);
			if (parentKey) {
			const entry = stats.get(parentKey) || { childCount: 0, partial: false, complete: false };
				entry.childCount++;
				stats.set(parentKey, entry);
			}
		}
	}

	if (includePartials) return stats;

	const filtered = new Map();
	for (const [key, value] of stats) {
		if (!value.partial) filtered.set(key, value);
	}
	return filtered;
}
