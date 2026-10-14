// Python owns the common precedence, repository-relative default and pin check.
import {execFileSync} from 'node:child_process';
import {fileURLToPath, pathToFileURL} from 'node:url';
import {join} from 'node:path';

export const upstreamRoot = execFileSync('python3', [fileURLToPath(new URL('../scripts/upstream.py', import.meta.url))], {encoding: 'utf8'}).trim();
export const upstreamPath = relative => join(upstreamRoot, relative);
export const upstreamUrl = relative => pathToFileURL(upstreamPath(relative));
export const upstreamEnvironment = environment => ({...environment, PIRUA_UPSTREAM_ROOT: upstreamRoot});
