// Streaming tree format for tournament schedules
// Format: tab-indented depth-first traversal
//
// Header (fixed width for in-place updates):
//   # teams=8 weeks=2 count=0000000000000
//
// Body: one schedule per line, tabs indicate depth (week number)
//   0,1,7,2,8,14,19,23,24,25,26,27
//   	13,16,20,17,21,5,12,3,4,9,10,22
//   	13,16,20,17,21,5,12,4,3,10,9,22
//   0,1,7,2,8,14,19,24,23,26,25,27
//   	...

import { createReadStream, createWriteStream, openSync, writeSync, closeSync } from 'fs';
import { createInterface } from 'readline';

// Fixed width for count field (13 digits = up to 9.9 trillion)
const COUNT_WIDTH = 13;
// Space reserved after count for " (partial)" marker (10 chars)
const PARTIAL_RESERVE = 10;
export const INCOMPLETE_MARKER = '…';
export const COMPLETE_MARKER = '✅';

export function isIncompleteMarker(content) {
	return content === INCOMPLETE_MARKER;
}

export function isCompleteMarker(content) {
	return content === COMPLETE_MARKER;
}

/**
 * Write schedules in streaming tree format.
 * Tracks previous path to emit only changed suffix with proper indentation.
 * Writes directly to final file with fixed-width header for in-place count updates.
 */
export class TreeWriter {
	#stream;
	#previousPath = [];
	#count = 0;
	#closed = false;
	#headerInfo = null;  // Store header info for finalization
	#countOffset = 0;    // Byte offset where count field starts

	/**
	 * @param {string} filePath - Output file path (writes directly, no temp file)
	 */
	constructor(filePath) {
		this.filePath = filePath;
		this.#stream = createWriteStream(filePath);
	}

	/**
	 * Write header comment lines with fixed-width count field
	 * Starts with (partial) marker - caller must finalize to remove it on success
	 * @param {number} teams
	 * @param {number} weeks
	 * @param {number} [count] - Initial count (default 0, will be updated at end)
	 */
	writeHeader(teams, weeks, count = 0) {
		const prefix = `# teams=${teams} weeks=${weeks} count=`;
		this.#countOffset = prefix.length;
		// Start with (partial) - will be cleared on successful finalize
		// Use trailing spaces for count, not leading zeros
		const countWithPartial = String(count) + ' (partial)';
		// Pad to full reserved width
		const reserved = countWithPartial.padEnd(COUNT_WIDTH + PARTIAL_RESERVE, ' ');
		this.#stream.write(`${prefix}${reserved}\n`);
		this.#headerInfo = { teams, weeks };
	}

	/**
	 * Write a single node at a specific depth.
	 * Lower-level than writePath, useful for manual tree construction.
	 * @param {number} depth - The depth (0 = week0, 1 = week1, etc.)
	 * @param {number[]} schedule - The schedule array
	 */
	writeNode(depth, schedule) {
		const tabs = '\t'.repeat(depth);
		this.#stream.write(`${tabs}${schedule.join(',')}\n`);
		// Update count only for leaf nodes (caller must track)
	}

	/**
	 * Write a complete path from week0 to weekN.
	 * Automatically computes diff with previous path and outputs only changed portion.
	 * @param {number[][]} path - Array of schedules [week0, week1, ..., weekN]
	 */
	writePath(path) {
		// Find where this path diverges from previous
		let commonDepth = 0;
		while (commonDepth < this.#previousPath.length &&
		       commonDepth < path.length &&
		       arraysEqual(this.#previousPath[commonDepth], path[commonDepth])) {
			commonDepth++;
		}

		// Write all nodes from divergence point onwards
		for (let depth = commonDepth; depth < path.length; depth++) {
			const tabs = '\t'.repeat(depth);
			const schedule = path[depth];
			this.#stream.write(`${tabs}${schedule.join(',')}\n`);
		}

		this.#previousPath = path.map(s => Array.from(s));
		this.#count++;
	}

	/**
	 * Get current count of paths written
	 */
	get count() {
		return this.#count;
	}

	/**
	 * Write an incomplete marker at the given depth.
	 * Used when a branch is interrupted before full exploration.
	 * @param {number} depth - The depth at which enumeration was interrupted
	 */
	writeIncompleteMarker(depth) {
		const tabs = '\t'.repeat(depth);
		this.#stream.write(`${tabs}${INCOMPLETE_MARKER}\n`);
	}

	/**
	 * Write a complete marker at the given depth.
	 * Used when a branch is fully explored without hitting breadth.
	 * @param {number} depth - The depth at which enumeration completed
	 */
	writeCompleteMarker(depth) {
		const tabs = '\t'.repeat(depth);
		this.#stream.write(`${tabs}${COMPLETE_MARKER}\n`);
	}

	/**
	 * Get the previous path (for tracking in-flight work)
	 * @returns {number[][]} Copy of the previous path
	 */
	getPreviousPath() {
		return this.#previousPath.map(s => Array.from(s));
	}

	/**
	 * Synchronously close the stream without updating header.
	 * Use when you'll handle finalization manually.
	 */
	close() {
		if (this.#closed) return;
		this.#closed = true;
		this.#stream.end();
	}

	/**
	 * Close the stream and update header count in-place.
	 * No file copying or renaming - just seeks to count field and overwrites.
	 * @param {Object} [finalHeader] - Optional final header values
	 * @param {number} [finalHeader.count] - Final count to write
	 * @param {boolean} [finalHeader.partial] - If true, keeps (partial) marker
	 * @returns {Promise<void>}
	 */
	async finalize(finalHeader = null) {
		if (this.#closed) return;
		this.#closed = true;

		return new Promise((resolve, reject) => {
			this.#stream.end(() => {
				try {
					if (finalHeader && finalHeader.count !== undefined) {
						// Update count in-place using synchronous file operations
						const fd = openSync(this.filePath, 'r+');
						const partialMarker = finalHeader.partial ? ' (partial)' : '';
						// Pad to full reserved width to clear any previous content
						const countStr = (String(finalHeader.count) + partialMarker).padEnd(COUNT_WIDTH + PARTIAL_RESERVE, ' ');
						writeSync(fd, countStr, this.#countOffset);
						closeSync(fd);
					}
					resolve();
				} catch (err) {
					reject(err);
				}
			});
		});
	}
}

/**
 * Read schedules from streaming tree format.
 * Yields complete paths [week0, week1, ..., weekN] as they're discovered.
 */
export class TreeReader {
	#filePath;
	#header = null;
	#targetWeeks = null;

	/**
	 * @param {string} filePath - Input file path
	 * @param {number} [targetWeeks] - Expected number of weeks (for leaf detection). If not provided, reads from header.
	 */
	constructor(filePath, targetWeeks = null) {
		this.#filePath = filePath;
		this.#targetWeeks = targetWeeks;
	}

	/**
	 * Parse header from file (streams only first line to handle large files)
	 * @returns {Promise<{teams: number, weeks: number, count: number, partial: boolean}>}
	 */
	async readHeader() {
		if (this.#header) return this.#header;

		this.#header = await readHeaderFromFile(this.#filePath);
		return this.#header;
	}

	/**
	 * Iterate over all complete paths in the file.
	 * Skips marker lines automatically.
	 * @yields {number[][]} path - [week0, week1, ..., weekN]
	 */
	async *paths() {
		const header = await this.readHeader();
		const weeksInFile = header.weeks;
		const targetWeeks = this.#targetWeeks ?? weeksInFile;

		const rl = createInterface({
			input: createReadStream(this.#filePath),
			crlfDelay: Infinity
		});

		const stack = []; // Stack of schedules at each depth
		let warnedInvalidLine = false;

		for await (const line of rl) {
			// Skip header/comment lines
			if (line.startsWith('#') || line.trim() === '') continue;

			// Count leading tabs for depth
			let depth = 0;
			while (line[depth] === '\t') depth++;

			// Skip markers
			const content = line.slice(depth).trim();
			if (isIncompleteMarker(content) || isCompleteMarker(content) || content === '') continue;

			// Parse schedule
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
			if (invalid) {
				if (!warnedInvalidLine) {
					warnedInvalidLine = true;
					console.warn(`Warning: Skipping invalid schedule line in ${this.#filePath}`);
				}
				continue;
			}

			// Pop stack to current depth
			while (stack.length > depth) {
				stack.pop();
			}

			// Push new schedule
			stack.push(schedule);

			// If we've reached target depth, yield complete path
			if (stack.length === weeksInFile) {
				yield stack.map(s => Array.from(s));
			}
		}
	}

	/**
	 * Read all paths into memory (for small files)
	 * @returns {Promise<number[][][]>} Array of paths
	 */
	async readAll() {
		const paths = [];
		for await (const path of this.paths()) {
			paths.push(path);
		}
		return paths;
	}

	/**
	 * Find incomplete branches in a partial file.
	 * Returns prefixes (paths before the … marker) that need continued exploration,
	 * plus the set of prefixes that have been fully explored (for filtering source paths).
	 * @returns {Promise<{incompletePrefixes: number[][][], completedPrefixes: Set<string>, isPartial: boolean}>}
	 */
	async findIncompleteBranches() {
		const header = await this.readHeader();
		const isPartial = header.partial || false;
		const targetDepth = header.weeks;

		const rl = createInterface({
			input: createReadStream(this.#filePath),
			crlfDelay: Infinity
		});

		const stack = []; // Stack of schedules at each depth
		const incompletePrefixes = [];
		const completedPrefixes = new Set();
		const prefixStates = new Map();  // key -> { lastMarker: 'incomplete'|'complete'|'none', completeSeen: boolean }
		const parentDepth = targetDepth - 2;
		const leafDepth = targetDepth - 1;
		let currentParentKey = null;
		let currentParentMarker = 'none';
		let currentParentCompleteSeen = false;
		let warnedInvalidLine = false;

		function finalizeParent() {
			if (currentParentKey === null) return;
			const state = prefixStates.get(currentParentKey) || { lastMarker: 'none', completeSeen: false };
			state.lastMarker = currentParentMarker;
			state.completeSeen = state.completeSeen || currentParentCompleteSeen;
			prefixStates.set(currentParentKey, state);
			currentParentKey = null;
			currentParentMarker = 'none';
			currentParentCompleteSeen = false;
		}

		for await (const line of rl) {
			// Skip header/comment lines
			if (line.startsWith('#') || line.trim() === '') continue;

			// Count leading tabs for depth
			let depth = 0;
			while (line[depth] === '\t') depth++;

			const content = line.slice(depth).trim();

			// Pop stack to current depth
			while (stack.length > depth) {
				stack.pop();
			}

			if (isIncompleteMarker(content) || isCompleteMarker(content) || content === '') {
				if (depth === leafDepth && currentParentKey !== null) {
					if (isCompleteMarker(content)) {
						currentParentMarker = 'complete';
						currentParentCompleteSeen = true;
					} else if (isIncompleteMarker(content)) {
						currentParentMarker = 'incomplete';
					}
				}
			} else {
				// Parse schedule and push to stack
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
				if (invalid) {
					if (!warnedInvalidLine) {
						warnedInvalidLine = true;
						console.warn(`Warning: Skipping invalid schedule line in ${this.#filePath}`);
					}
					continue;
				}
				stack.push(schedule);

				if (depth <= parentDepth) {
					finalizeParent();
					if (depth === parentDepth) {
						const prefix = stack.map(s => s.join(',')).join('|');
						currentParentKey = prefix;
						currentParentMarker = 'none';
						currentParentCompleteSeen = false;
					}
				}
			}
		}

		finalizeParent();

		for (const [key, state] of prefixStates) {
			if (state.completeSeen) {
				completedPrefixes.add(key);
			} else if (state.lastMarker === 'incomplete') {
				const prefix = key.split('|').map(s => s.split(',').map(v => parseInt(v, 10)));
				incompletePrefixes.push(prefix);
			}
		}

		return { incompletePrefixes, completedPrefixes, isPartial };
	}
}

/**
 * Parse header line into components
 * @param {string} line - Header line starting with #
 * @returns {{teams: number, weeks: number, count: number, partial: boolean}}
 */
export function parseHeader(line) {
	const result = { teams: 0, weeks: 0, count: 0, partial: false };

	const teamsMatch = line.match(/teams=(\d+)/);
	if (teamsMatch) result.teams = parseInt(teamsMatch[1], 10);

	const weeksMatch = line.match(/weeks=(\d+)/);
	if (weeksMatch) result.weeks = parseInt(weeksMatch[1], 10);

	const countMatch = line.match(/count=(\d+)/);
	if (countMatch) result.count = parseInt(countMatch[1], 10);

	// Check for (partial) marker
	result.partial = line.includes('(partial)');

	return result;
}

/**
 * Read header from file using streaming (handles large files)
 * @param {string} filePath - Path to the file
 * @returns {Promise<{teams: number, weeks: number, count: number, partial: boolean}>}
 */
export async function readHeaderFromFile(filePath) {
	const rl = createInterface({
		input: createReadStream(filePath),
		crlfDelay: Infinity
	});

	for await (const line of rl) {
		rl.close();
		return parseHeader(line);
	}

	return parseHeader('');
}

/**
 * Check if two arrays are equal
 */
function arraysEqual(a, b) {
	if (!a || !b) return false;
	if (a.length !== b.length) return false;
	for (let i = 0; i < a.length; i++) {
		if (a[i] !== b[i]) return false;
	}
	return true;
}

/**
 * Find the most recent prior results file for given teams
 * @param {string} resultsDir - Directory to search
 * @param {number} teams - Number of teams
 * @param {number} targetWeeks - Target weeks (looks for files with fewer weeks)
 * @returns {Promise<{path: string, weeks: number} | null>}
 */
export async function findPriorResults(resultsDir, teams, targetWeeks) {
	const { readdir } = await import('fs/promises');
	const { existsSync } = await import('fs');

	if (!existsSync(resultsDir)) return null;

	const files = await readdir(resultsDir);

	// Pattern: {teams}teams-{weeks}weeks.txt or {teams}teams-week0.txt
	const pattern = new RegExp(`^${teams}teams-(\\d+)weeks?\\.txt$`);

	let bestMatch = null;
	let bestWeeks = 0;

	for (const file of files) {
		const match = file.match(pattern);
		if (match) {
			const weeks = parseInt(match[1], 10);
			// week0 file counts as 1 week
			const effectiveWeeks = match[0].includes('week0') ? 1 : weeks;
			if (effectiveWeeks < targetWeeks && effectiveWeeks > bestWeeks) {
				bestWeeks = effectiveWeeks;
				bestMatch = { path: `${resultsDir}/${file}`, weeks: effectiveWeeks };
			}
		}
	}

	// Also check for week0 file
	const week0Path = `${resultsDir}/${teams}teams-week0.txt`;
	if (existsSync(week0Path) && 1 < targetWeeks && 1 > bestWeeks) {
		bestMatch = { path: week0Path, weeks: 1 };
	}

	return bestMatch;
}
