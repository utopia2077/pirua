import type { TruncationResult } from "../core/tools/truncate.ts";
export interface BashToolDetails {
	truncation?: TruncationResult;
	fullOutputPath?: string;
}
export interface EditToolDetails {
	/** Display-oriented diff of the changes made */
	diff: string;
	/** Standard unified patch of the changes made */
	patch: string;
	/** Line number of the first change in the new file (for editor navigation) */
	firstChangedLine?: number;
}
export interface FindToolDetails {
	truncation?: TruncationResult;
	resultLimitReached?: number;
}
export interface GrepToolDetails {
	truncation?: TruncationResult;
	matchLimitReached?: number;
	linesTruncated?: boolean;
}
export interface LsToolDetails {
	truncation?: TruncationResult;
	entryLimitReached?: number;
}
export interface ReadToolDetails {
	truncation?: TruncationResult;
}
