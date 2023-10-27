import { BasicSourceMapConsumer, MappedPosition, SourceMapConsumer } from 'source-map';
import { LoggingDebugSession } from 'vscode-debugadapter';
import { SourcemapArguments } from "./sourcemapArguments";
const path = require('path');
const fs = require('fs');

export abstract class SourcemapSession extends LoggingDebugSession {
	// a map of all absolute file sources found in the sourcemaps
	private _fileToSourceMap = new Map<string, BasicSourceMapConsumer>();
	private _sourceMapsLoading: Promise<any>|undefined;
	// keep track of the sourcemaps and the location of the file.map used to load it
	private _sourceMaps = new Map<BasicSourceMapConsumer, string>();

	abstract logTrace(message: string);
	abstract getArguments(): Promise<SourcemapArguments>;

	async loadSourceMaps() {
		const commonArgs = await this.getArguments();
		if (this._sourceMapsLoading)
			return await this._sourceMapsLoading;

		let sourceMaps = Object.keys(commonArgs.sourceMaps || {}) || [];

		let promises = sourceMaps.map(sourcemap => (async () => {
			try {
				let json = JSON.parse(fs.readFileSync(sourcemap).toString());
				let smc = await new SourceMapConsumer(json);
				this._sourceMaps.set(smc, sourcemap);
				let sourceMapRoot = path.dirname(sourcemap);
				let sources = smc.sources.map(source => path.join(sourceMapRoot, source) as string);
				for (let source of sources) {
					const other = this._fileToSourceMap.get(source);
					if (other) {
						this.logTrace(`sourcemap for ${source} found in ${other.file}.map and ${sourcemap}`);
					}
					else {
						this._fileToSourceMap.set(source, smc);
					}
				}
			}
			catch (e) {
			}
		})());

		this._sourceMapsLoading = Promise.all(promises);

		return await this._sourceMapsLoading;
	}

	async translateFileToRemote(file: string): Promise<string> {
		await this.loadSourceMaps();

		const sm = this._fileToSourceMap.get(file);
		if (!sm)
			return file;
		return sm.file;
	}

	private async getRemoteAbsolutePath(remoteFile: string, remoteRoot?: string): Promise<string> {
		const commonArgs = await this.getArguments();
		if (remoteRoot === null)
			remoteRoot = commonArgs.remoteRoot;
		if (remoteRoot)
			remoteFile = path.join(remoteRoot, remoteFile);
		return remoteFile;
	}

	private async getRemoteRelativePath(remoteFile: string, remoteRoot?: string): Promise<string> {
		const commonArgs = await this.getArguments();
		if (remoteRoot === null)
			remoteRoot = commonArgs.remoteRoot;
		if (remoteRoot)
			return path.relative(remoteRoot, remoteFile);
		return remoteFile;
	}

	private async getLocalAbsolutePath(localFile: string): Promise<string> {
		const commonArgs = await this.getArguments();
		if (commonArgs.localRoot)
			return path.join(commonArgs.localRoot, localFile);
		return localFile;
	}
	private async getLocalRelativePath(localFile: string): Promise<string> {
		const commonArgs = await this.getArguments();
		if (commonArgs.localRoot)
			return path.relative(commonArgs.localRoot, localFile);
		return localFile;
	}


	async translateFileLocationToRemote(sourceLocation: MappedPosition): Promise<MappedPosition[]> {
		await this.loadSourceMaps();
		const commonArgs = await this.getArguments();

		// step 1: translate the absolute local source position to a relative source position.
		// (has sourcemap) /local/path/to/test.ts:10 -> test.js:15
		// (no sourcemap)  /local/path/to/test.js:10 -> test.js:10
		// step 2: translate the relative source file to an absolute remote source file
		// (has sourcemap) test.js:15 -> /remote/path/to/test.js:15
		// (no sourcemap)  test.js:10 -> /remote/path/to/test.js:10
		// (no remote root)test.js:10 -> test.js:10

		try {
			const sm = this._fileToSourceMap.get(sourceLocation.source);
			if (!sm)
				throw new Error('no source map');
			const sourcemap = this._sourceMaps.get(sm);
			if (!sourcemap)
				throw new Error();
			const actualSourceLocation = Object.assign({}, sourceLocation);
			this.logTrace(`translateFileLocationToRemote: ${JSON.stringify(sourceLocation)} to: ${JSON.stringify(actualSourceLocation)}`);
			// convert the local absolute path into a sourcemap relative path.
			actualSourceLocation.source = path.relative(path.dirname(sourcemap), sourceLocation.source);
			// let unmappedPosition: NullablePosition = sm.generatedPositionFor(actualSourceLocation);
			let unmappedPositions = sm.allGeneratedPositionsFor(actualSourceLocation);
			if (!unmappedPositions || !unmappedPositions.length)
				throw new Error('map failed');
			// now given a source mapped relative path, translate that into a remote path.
			const smp = this._sourceMaps.get(sm);
			let remoteRoot = commonArgs.sourceMaps && commonArgs.sourceMaps[smp!];
			let remoteFile = await this.getRemoteAbsolutePath(sm.file, remoteRoot);
			return unmappedPositions.map(unmappedPosition => ({
				source: remoteFile,
				line: (unmappedPosition.line || 0),
				column: unmappedPosition.column || 0,
			}));
			// return {
			// 	source: remoteFile,
			// 	// the source-map docs indicate that line is 1 based, but that seems to be wrong.
			// 	line: (unmappedPosition.line || 0) + 1,
			// 	column: unmappedPosition.column || 0,
			// };
		}
		catch (e) {
			// local files need to be resolved to remote files.
			let ret = Object.assign({}, sourceLocation);
			ret.source = await this.getRemoteAbsolutePath(await this.getLocalRelativePath(sourceLocation.source));
			return [ret];
		}
	}

	async translateRemoteLocationToLocal(sourceLocation: MappedPosition): Promise<MappedPosition> {
		await this.loadSourceMaps();
		const commonArgs = await this.getArguments();

		try {
			for (let sm of this._sourceMaps.keys()) {
				const smp = this._sourceMaps.get(sm);

				// given a remote path, translate that into a source map relative path for lookup
				let remoteRoot = commonArgs.sourceMaps && commonArgs.sourceMaps[smp!];
				let relativeFile = await this.getRemoteRelativePath(sourceLocation.source, remoteRoot);

				if (relativeFile !== sm.file)
					continue;

				const original = sm.originalPositionFor({
					column: sourceLocation.column,
					line: sourceLocation.line,
				});
				this.logTrace(`translateRemoteLocationToLocal: ${JSON.stringify(sourceLocation)} to: ${JSON.stringify(original)}`);
				if (original.line === null || original.column === null || original.source === null)
					throw new Error("unable to map");

				// now given a source mapped relative path, translate that into a local path.
				return {
					source: path.join(path.dirname(smp), original.source),
					line: original.line,
					column: original.column,
				};
			}
			throw new Error();
		}
		catch (e) {
			// remote files need to be resolved to local files.
			let ret = Object.assign({}, sourceLocation);
			ret.source = await this.getLocalAbsolutePath(await this.getRemoteRelativePath(sourceLocation.source));
			return ret;
		}
	}
}