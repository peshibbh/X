import { spawn, spawnSync } from 'child_process';
import { EventEmitter } from 'events';
import fs from 'fs';
import { createRequire } from 'module';
import os from 'os';
import path from 'path';
import readline from 'readline';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const require = createRequire(import.meta.url);

export const getGoEngineDir = () => {
    return path.resolve(__dirname, '../../go-engine');
};

export const engineCanRun = () => {
    const binary = getGoEngineBinary();
    return !!binary && (fs.existsSync(binary) || isGoAvailable());
};

const getGoEngineBinary = () => {
    const dir = getGoEngineDir();
    const name = os.platform() === 'win32' ? 'main_win.exe' : 'main_linux';
    return path.join(dir, name);
};

let goAvailable = null;

const isGoAvailable = () => {
    if (goAvailable !== null) {
        return goAvailable;
    }
    const result = spawnSync('go', ['version']);
    goAvailable = !result.error && result.status === 0;
    return goAvailable;
};

export class MediaEngineBridge extends EventEmitter {
    constructor({ sessionName = 'nozez' } = {}) {
        super();
        this.sessionName = sessionName;
        this.process = null;
        this.pendingRequests = new Map();
        this.reqIdCounter = 1;
        this.mediaCommandsSupported = null;
        this.start();
    }

    start() {
        if (this.process && !this.process.killed) {
            return;
        }
        const dir = getGoEngineDir();
        const binary = getGoEngineBinary();
        if (!fs.existsSync(binary) && !isGoAvailable()) {
            return;
        }
        try {
            if (fs.existsSync(binary)) {
                fs.chmodSync(binary, '755');
            }
        } catch {}
        const args = [this.sessionName];
        const command = fs.existsSync(binary) ? binary : 'go';
        if (command === 'go') {
            args.unshift('run', 'main.go');
        }
        this.process = spawn(command, args, { cwd: dir });

        const rl = readline.createInterface({
            input: this.process.stdout,
            terminal: false
        });

        rl.on('line', (line) => {
            try {
                this._handleMessage(JSON.parse(line));
            } catch {}
        });

        this.process.stderr.on('data', (data) => {
            this.emit('log', data.toString());
        });

        this.process.on('error', () => {
            for (const { reject } of this.pendingRequests.values()) {
                reject(new Error('Go media engine process error'));
            }
            this.pendingRequests.clear();
            this.process = null;
        });

        this.process.on('exit', () => {
            for (const { reject } of this.pendingRequests.values()) {
                reject(new Error('Go media engine process berhenti'));
            }
            this.pendingRequests.clear();
            this.process = null;
        });
    }

    _handleMessage(message) {
        const { event, data, id, error, resp } = message;
        if (event === 'response' && id) {
            const req = this.pendingRequests.get(id);
            if (req) {
                this.pendingRequests.delete(id);
                if (error) {
                    req.reject(new Error(error));
                } else {
                    req.resolve(resp ?? data ?? message);
                }
            }
            return;
        }
        if (event === 'connection.update' || event === 'pairing_code') {
            this.emit(event, data);
        } else if (event === 'error') {
            this.emit('error', data);
        }
    }

    _send(action, payload = {}, timeoutMs = 60000) {
        return new Promise((resolve, reject) => {
            if (!this.process || this.process.killed) {
                return reject(new Error('Go media engine belum berjalan'));
            }
            const id = `req_${this.reqIdCounter++}`;
            let timeout;
            if (timeoutMs > 0) {
                timeout = setTimeout(() => {
                    this.pendingRequests.delete(id);
                    reject(new Error(`Go media engine timeout: ${action} (${timeoutMs}ms)`));
                }, timeoutMs);
            }
            this.pendingRequests.set(id, {
                resolve: (value) => {
                    clearTimeout(timeout);
                    resolve(value);
                },
                reject: (error) => {
                    clearTimeout(timeout);
                    reject(error);
                }
            });
            this.process.stdin.write(JSON.stringify({ action, id, payload }) + '\n');
        });
    }

    isAlive() {
        return !!(this.process && !this.process.killed);
    }

    requestPairingCode(phone) {
        return this._send('requestPairingCode', { phone });
    }

    async _mediaCommand(action, payload, timeoutMs = 60000) {
        if (this.mediaCommandsSupported === false) {
            throw new Error('Go media engine does not support media commands');
        }
        try {
            return await this._send(action, payload, timeoutMs);
        } catch (error) {
            if (/tidak dikenal|timeout/.test(error.message)) {
                this.mediaCommandsSupported = false;
            }
            throw error;
        }
    }

    async uploadMedia(filePath, mediaType) {
        const res = await this._mediaCommand('media:upload', { filePath, mediaType });
        return {
            url: res.url,
            directPath: res.directPath,
            mediaKey: Buffer.from(res.mediaKey || '', 'base64'),
            fileEncSha256: Buffer.from(res.fileEncSha256 || '', 'base64'),
            fileSha256: Buffer.from(res.fileSha256 || '', 'base64')
        };
    }

    async downloadMedia({ url, directPath, mediaKey, mediaType }) {
        const res = await this._mediaCommand('media:download', {
            url: url || '',
            directPath: directPath || '',
            mediaKey: Buffer.isBuffer(mediaKey) ? mediaKey.toString('base64') : String(mediaKey || ''),
            mediaType
        });
        const buffer = await fs.promises.readFile(res.filePath);
        await fs.promises.unlink(res.filePath).catch(() => {});
        return buffer;
    }

    stop() {
        if (this.process && !this.process.killed) {
            this.process.kill('SIGTERM');
        }
        this.process = null;
    }
}

let engineInstance = null;

export const getMediaEngine = (sessionName = 'nozez') => {
    if (!engineInstance || engineInstance.sessionName !== sessionName) {
        engineInstance = new MediaEngineBridge({ sessionName });
    }
    return engineInstance;
};