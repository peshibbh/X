import { LRUCache } from 'lru-cache';
export const makeMessageDeduper = ({ max = 20000, ttlMs = 5 * 60 * 1000 } = {}) => {
    const seen = new LRUCache({
        max,
        ttl: ttlMs,
        ttlResolution: 5_000,
        ttlAutopurge: true
    });
    const keyOf = (msg) => {
        const k = msg.key || {};
        return `${k.remoteJid || ''}::${k.fromMe ? '1' : '0'}::${k.id || ''}::${k.participant || ''}`;
    };
    return {
        keyOf,
        isDuplicate: (msg) => {
            const k = keyOf(msg);
            const duplicate = seen.get(k) !== undefined;
            if (!duplicate) {
                seen.set(k, true);
            }
            return duplicate;
        },
        get size() {
            return seen.size;
        }
    };
}