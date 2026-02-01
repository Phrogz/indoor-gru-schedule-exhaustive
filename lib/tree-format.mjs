// Streaming tree format for tournament schedules
// Format: tab-indented depth-first traversal
//
// Header:
//   # teams=8 weeks=2 score=4,8 count=7136
//
// Body: one schedule per line, tabs indicate depth (week number)
//   0,1,7,2,8,14,19,23,24,25,26,27
//   	13,16,20,17,21,5,12,3,4,9,10,22
//   	13,16,20,17,21,5,12,4,3,10,9,22
//   0,1,7,2,8,14,19,24,23,26,25,27
//   	...

import { createReadStream, createWriteStream } from 'fs';
import { createInterface } from 'readline';

/**
 * Write schedules in streaming tree format.
 * Tracks previous path to emit only changed suffix with proper indentation.
 */
export class TreeWriter {
	#stream;
	#previousPath = [];
	#count = 0;
	#closed = false;

	/**
	 * @param {string} filePath - Output file path
	 */
	constructor(filePath) {
		this.filePath = filePath;
		this.tempPath = filePath + '.tmp';
		this.#stream = createWriteStream(this.tempPath);
	}

	/**
	 * Write header comment lines
	 * @param {number} teams
	 * @param {number} weeks
	 * @param {number[]} score - [doubleByes, fiveSlotTeams]
	 * @param {number} [count] - Optional count (can be updated at end)
	 */
	writeHeader(teams, weeks, score, count = 0) {
		this.#stream.write(`# teams=${teams} weeks=${weeks} score=${score.join(',')} count=${count}\n`);
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
		this.#stream.write(`${tabs}…\n`);
	}

	/**
	 * Get the previous path (for tracking in-flight work)
	 * @returns {number[][]} Copy of the previous path
	 */
	getPreviousPath() {
		return this.#previousPath.map(s => Array.from(s));
	}

	/**
	 * Synchronously close the stream without renaming.
	 * Use when you'll handle the temp file manually.
	 */
	close() {
		if (this.#closed) return;
		this.#closed = true;
		this.#stream.end();
	}

	/**
	 * Close the stream and rename temp to final.
	 * @param {Object} [finalHeader] - Optional final header values to rewrite
	 * @returns {Promise<void>}
	 */
	async finalize(finalHeader = null) {
		if (this.#closed) return;
		this.#closed = true;

		return new Promise((resolve, reject) => {
			this.#stream.end(async () => {
				try {
					if (finalHeader) {
						// Rewrite header with final count
						const { rename, readFile, writeFile } = await import('fs/promises');
						const content = await readFile(this.tempPath, 'utf8');
						const lines = content.split('\n');
						// Update header line
						lines[0] = `# teams=${finalHeader.teams} weeks=${finalHeader.weeks} score=${finalHeader.score.join(',')} count=${finalHeader.count}`;
						await writeFile(this.tempPath, lines.join('\n'));
						await rename(this.tempPath, this.filePath);
					} else {
						const { rename } = await import('fs/promises');
						await rename(this.tempPath, this.filePath);
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
	 * Parse header from file
	 * @returns {Promise<{teams: number, weeks: number, score: number[], count: number}>}
	 */
	async readHeader() {
		if (this.#header) return this.#header;

		const { readFile } = await import('fs/promises');
		const content = await readFile(this.#filePath, 'utf8');
		const firstLine = content.split('\n')[0];
		this.#header = parseHeader(firstLine);
		return this.#header;
	}

	/**
	 * Iterate over all complete paths in the file.
	 * Skips incomplete markers (…) automatically.
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

		for await (const line of rl) {
			// Skip header/comment lines
			if (line.startsWith('#') || line.trim() === '') continue;

			// Count leading tabs for depth
			let depth = 0;
			while (line[depth] === '\t') depth++;

			// Skip incomplete markers
			const content = line.slice(depth);
			if (content === '…') continue;

			// Parse schedule
			const schedule = content.split(',').map(n => parseInt(n.trim(), 10));

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
		const completedPrefixes = new Set();  // Prefixes at depth-1 that have complete leaves
		const incompletePrefixKeys = new Set();  // Track which prefixes are incomplete

		for await (const line of rl) {
			// Skip header/comment lines
			if (line.startsWith('#') || line.trim() === '') continue;

			// Count leading tabs for depth
			let depth = 0;
			while (line[depth] === '\t') depth++;

			const content = line.slice(depth);

			// Pop stack to current depth
			while (stack.length > depth) {
				stack.pop();
			}

			if (content === '…') {
				// This branch is incomplete - save the prefix leading to it
				if (stack.length > 0) {
					const prefix = stack.map(s => Array.from(s));
					const prefixKey = prefix.map(s => s.join(',')).join('|');
					incompletePrefixes.push(prefix);
					incompletePrefixKeys.add(prefixKey);
				}
			} else {
				// Parse schedule and push to stack
				const schedule = content.split(',').map(n => parseInt(n.trim(), 10));
				stack.push(schedule);

				// If we've reached a complete leaf, mark its prefix as having completions
				if (stack.length === targetDepth) {
					// The prefix is all but the last element
					const prefix = stack.slice(0, -1);
					const prefixKey = prefix.map(s => s.join(',')).join('|');
					// Only mark as completed if not also marked incomplete
					if (!incompletePrefixKeys.has(prefixKey)) {
						completedPrefixes.add(prefixKey);
					}
				}
			}
		}

		// Remove from completedPrefixes any that also have incomplete markers
		// (they were added before we saw the … marker)
		for (const key of incompletePrefixKeys) {
			completedPrefixes.delete(key);
		}

		return { incompletePrefixes, completedPrefixes, isPartial };
	}
}

/**
 * Parse header line into components
 * @param {string} line - Header line starting with #
 * @returns {{teams: number, weeks: number, score: number[], count: number}}
 */
export function parseHeader(line) {
	const result = { teams: 0, weeks: 0, score: [0, 0], count: 0, partial: false };

	const teamsMatch = line.match(/teams=(\d+)/);
	if (teamsMatch) result.teams = parseInt(teamsMatch[1], 10);

	const weeksMatch = line.match(/weeks=(\d+)/);
	if (weeksMatch) result.weeks = parseInt(weeksMatch[1], 10);

	const scoreMatch = line.match(/score=(\d+),(\d+)/);
	if (scoreMatch) result.score = [parseInt(scoreMatch[1], 10), parseInt(scoreMatch[2], 10)];

	const countMatch = line.match(/count=(\d+)/);
	if (countMatch) result.count = parseInt(countMatch[1], 10);

	// Check for (partial) marker
	result.partial = line.includes('(partial)');

	return result;
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
