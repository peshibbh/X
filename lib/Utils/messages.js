import { Boom } from '@hapi/boom';
import { randomBytes } from 'crypto';
import { zip } from 'fflate';
import { promises as fs } from 'fs';
import { proto } from '../../WAProto/index.js';
import { CALL_AUDIO_PREFIX, CALL_VIDEO_PREFIX,LIBRARY_NAME, MEDIA_KEYS, URL_REGEX, WA_DEFAULT_EPHEMERAL } from '../Defaults/index.js';
import { AssociationType, ButtonHeaderType, ButtonType, CarouselCardType, ListType, ProtocolType, WAMessageStatus, WAProto } from '../Types/index.js';
import { isLidUser, isPnUser, isJidGroup, isJidNewsletter, isJidStatusBroadcast, jidNormalizedUser } from '../WABinary/index.js';
import { sha256 } from './crypto.js';
import { generateMessageIDV2, getKeyAuthor, unixTimestampSeconds } from './generics.js';
import { downloadContentFromMessage, encryptedStream, generateThumbnail, getAudioDuration, getAudioWaveform, getImageProcessingLibrary, getRawMediaUploadData, getStream, toBuffer } from './messages-media.js';
import { prepareRichResponseMessage } from './rich-message-utils.js';
import { shouldIncludeReportingToken } from './reporting-utils.js';
const CONCURRENCY_LIMIT = 15;
const MAX_CAROUSEL_CARDS = 10;
const MIMETYPE_MAP = {
    image: 'image/jpeg',
    video: 'video/mp4',
    document: 'application/pdf',
    audio: 'audio/ogg; codecs=opus',
    sticker: 'image/webp',
    'product-catalog-image': 'image/jpeg'
};
const MessageTypeProto = {
    image: WAProto.Message.ImageMessage,
    video: WAProto.Message.VideoMessage,
    audio: WAProto.Message.AudioMessage,
    sticker: WAProto.Message.StickerMessage,
    document: WAProto.Message.DocumentMessage
};
/**
 * Uses a regex to test whether the string contains a URL, and returns the URL if it does.
 * @param text eg. hello https://google.com
 * @returns the URL, eg. https://google.com
 */
export const extractUrlFromText = (text) => text.match(URL_REGEX)?.[0];
export const generateLinkPreviewIfRequired = async (text, getUrlInfo, logger) => {
    const url = extractUrlFromText(text);
    if (!!getUrlInfo && url) {
        try {
            const urlInfo = await getUrlInfo(url);
            return urlInfo;
        }
        catch (error) {
            // ignore if fails
            logger?.warn({ trace: error.stack }, 'url generation failed');
        }
    }
};
const assertColor = async (color) => {
    let assertedColor;
    if (typeof color === 'number') {
        assertedColor = color > 0 ? color : 0xffffffff + Number(color) + 1;
    }
    else {
        let hex = color.trim().replace('#', '');
        if (hex.length <= 6) {
            hex = 'FF' + hex.padStart(6, '0');
        }
        assertedColor = parseInt(hex, 16);
        return assertedColor;
    }
};
export const prepareWAMessageMedia = async (message, options) => {
    const logger = options.logger;
    let mediaType;
    for (const key of MEDIA_KEYS) {
        if (key in message) {
            mediaType = key;
        }
    }
    if (!mediaType) {
        throw new Boom('Invalid media type', { statusCode: 400 });
    }
    const uploadData = {
        ...message,
        media: message[mediaType]
    };
    delete uploadData[mediaType];
    // check if cacheable + generate cache key
    const cacheableKey = typeof uploadData.media === 'object' &&
        'url' in uploadData.media &&
        !!uploadData.media.url &&
        !!options.mediaCache &&
        mediaType + ':' + uploadData.media.url.toString();
    if (mediaType === 'document' && !uploadData.fileName) {
        uploadData.fileName = 'file';
    }
    if (!uploadData.mimetype) {
        uploadData.mimetype = MIMETYPE_MAP[mediaType];
    }
    if (cacheableKey) {
        const mediaBuff = await options.mediaCache.get(cacheableKey);
        if (mediaBuff) {
            logger?.debug({ cacheableKey }, 'got media cache hit');
            const obj = proto.Message.decode(mediaBuff);
            const key = `${mediaType}Message`;
            Object.assign(obj[key], { ...uploadData, media: undefined });
            return obj;
        }
    }
    const isNewsletter = !!options.jid && isJidNewsletter(options.jid);
    if (isNewsletter) {
        logger?.info({ key: cacheableKey }, 'Preparing raw media for newsletter');
        const { filePath, fileSha256, fileLength } = await getRawMediaUploadData(uploadData.media, options.mediaTypeOverride || mediaType, logger);
        const fileSha256B64 = fileSha256.toString('base64');
        const { mediaUrl, directPath, thumbnailDirectPath, thumbnailSha256 } = await options.upload(filePath, {
            fileEncSha256B64: fileSha256B64,
            mediaType: mediaType,
            timeoutMs: options.mediaUploadTimeoutMs,
            newsletter: isNewsletter
        });
        await fs.unlink(filePath);
        const obj = WAProto.Message.fromObject({
            // todo: add more support here
            [`${mediaType}Message`]: MessageTypeProto[mediaType].fromObject({
                url: mediaUrl,
                directPath,
                fileSha256,
                fileLength,
                thumbnailDirectPath,
                thumbnailSha256,
                ...uploadData,
                media: undefined
            })
        });
        if (uploadData.ptv) {
            obj.ptvMessage = obj.videoMessage;
            delete obj.videoMessage;
        }
        if (obj.stickerMessage) {
            obj.stickerMessage.stickerSentTs = Date.now();
        }
        if (cacheableKey) {
            logger?.debug({ cacheableKey }, 'set cache');
            await options.mediaCache.set(cacheableKey, WAProto.Message.encode(obj).finish());
        }
        return obj;
    }
    const requiresDurationComputation = mediaType === 'audio' && typeof uploadData.seconds === 'undefined';
    const requiresThumbnailComputation = (mediaType === 'image' || mediaType === 'video') && typeof uploadData['jpegThumbnail'] === 'undefined';
    const requiresWaveformProcessing = mediaType === 'audio' && uploadData.ptt === true && typeof uploadData.waveform === 'undefined';
    const requiresAudioBackground = options.backgroundColor && mediaType === 'audio' && uploadData.ptt === true;
    const requiresOriginalForSomeProcessing = requiresDurationComputation || requiresThumbnailComputation;
    const { mediaKey, encFilePath, originalFilePath, fileEncSha256, fileSha256, fileLength } = await encryptedStream(uploadData.media, options.mediaTypeOverride || mediaType, {
        logger,
        saveOriginalFileIfRequired: requiresOriginalForSomeProcessing,
        opts: options.options
    });
    const fileEncSha256B64 = fileEncSha256.toString('base64');
    const [{ mediaUrl, directPath }] = await Promise.all([
        (async () => {
            const result = await options.upload(encFilePath, {
                fileEncSha256B64,
                mediaType,
                timeoutMs: options.mediaUploadTimeoutMs
            });
            logger?.debug({ mediaType, cacheableKey }, 'uploaded media');
            return result;
        })(),
        (async () => {
            try {
                if (requiresThumbnailComputation) {
                    const { thumbnail, originalImageDimensions } = await generateThumbnail(originalFilePath, mediaType, options);
                    uploadData.jpegThumbnail = thumbnail;
                    if (!uploadData.width && originalImageDimensions) {
                        uploadData.width = originalImageDimensions.width;
                        uploadData.height = originalImageDimensions.height;
                        logger?.debug('set dimensions');
                    }
                    logger?.debug('generated thumbnail');
                }
                if (requiresDurationComputation) {
                    uploadData.seconds = await getAudioDuration(originalFilePath);
                    logger?.debug('computed audio duration');
                }
                if (requiresWaveformProcessing) {
                    uploadData.waveform = await getAudioWaveform(originalFilePath, logger);
                    logger?.debug('processed waveform');
                }
                if (requiresAudioBackground) {
                    uploadData.backgroundArgb = await assertColor(options.backgroundColor);
                    logger?.debug('computed backgroundColor audio status');
                }
            }
            catch (error) {
                logger?.warn({ trace: error.stack }, 'failed to obtain extra info');
            }
        })()
    ]).finally(async () => {
        try {
            await fs.unlink(encFilePath);
            if (originalFilePath) {
                await fs.unlink(originalFilePath);
            }
            logger?.debug('removed tmp files');
        }
        catch (error) {
            logger?.warn('failed to remove tmp file');
        }
    });
    const obj = WAProto.Message.fromObject({
        [`${mediaType}Message`]: MessageTypeProto[mediaType].fromObject({
            url: mediaUrl,
            directPath,
            mediaKey,
            fileEncSha256,
            fileSha256,
            fileLength,
            mediaKeyTimestamp: unixTimestampSeconds(),
            ...uploadData,
            media: undefined
        })
    });
    if (uploadData.ptv) {
        obj.ptvMessage = obj.videoMessage;
        delete obj.videoMessage;
    }
    if (cacheableKey) {
        logger?.debug({ cacheableKey }, 'set cache');
        await options.mediaCache.set(cacheableKey, WAProto.Message.encode(obj).finish());
    }
    return obj;
};

const prepareProductMessage = async (message, options) => {
    if (!message.businessOwnerJid) {
        throw new Boom('"businessOwnerJid" is missing from the content', { statusCode: 400 });
    }
    const { imageMessage } = await prepareWAMessageMedia({ image: message.image || message.product.productImage }, options);

    const { image, ...content } = message;
    content.product = {
        currencyCode: 'IDR',
        priceAmount1000: 1000,
        title: LIBRARY_NAME,
        ...message.product,
        productImage: imageMessage
    };
    return content;
};
const prepareStickerPackMessage = async (message, options) => {
    const { cover, stickers = [], name = '📦 Sticker Pack', publisher = 'baileys_zero', description = '🏷️ baileys_zero' } = message;
    if (stickers.length > 60) {
        throw new Boom('Sticker pack exceeds the maximum limit of 60 stickers', { statusCode: 400 });
    }
    if (stickers.length === 0) {
        throw new Boom('Sticker pack must contain at least one sticker', { statusCode: 400 });
    }
    if (!cover) {
        throw new Boom('Sticker pack must contain a cover', { statusCode: 400 });
    }
    const logger = options.logger;

    let cacheableKey = false;
    if (Array.isArray(stickers) && stickers.length && options.mediaCache) {
        const urls = [];
        for (let i = 0; i < stickers.length; i++) {
            const data = stickers[i].data;
            if (typeof data === 'object' && data?.url) {
                urls.push(data.url);
            }
        }
        if (urls.length > 0) {
            cacheableKey = 'sticker:' + urls.join('@');
        }
    }
    if (cacheableKey) {
        const mediaBuff = await options.mediaCache.get(cacheableKey);
        if (mediaBuff) {
            logger?.debug({ cacheableKey }, 'got media cache hit');
            return proto.Message.StickerPackMessage.decode(mediaBuff);
        }
    }
    const lib = await getImageProcessingLibrary();
    const hasSharp = 'sharp' in lib && !!lib.sharp?.default;
    const hasImage = 'image' in lib && !!lib.image?.Transformer;
    const hasJimp = 'jimp' in lib && !!lib.jimp?.Jimp;
    if (!hasSharp && !hasImage) {
        throw new Boom('No image processing library (sharp or @napi-rs/image) available for converting sticker to WebP.');
    }
    const stickerPackIdValue = generateMessageIDV2();
    const stickerData = {};
    const stickerMetadata = new Array(stickers.length);
    for (let i = 0; i < stickers.length; i += CONCURRENCY_LIMIT) {
        const promises = [];
        const chunkEnd = Math.min(i + CONCURRENCY_LIMIT, stickers.length);
        for (let j = i; j < chunkEnd; j++) {
            promises.push((async (index) => {
                const sticker = stickers[index];
                const { stream } = await getStream(sticker.data);
                const buffer = await toBuffer(stream);
                let webpBuffer;
                let isAnimated = false;
                if (isWebPBuffer(buffer)) {
                    webpBuffer = buffer;
                    isAnimated = isAnimatedWebP(buffer);
                }
                else if (hasSharp) {
                    webpBuffer = await lib.sharp.default(buffer)
                        .resize(512, 512, { fit: 'inside' })
                        .webp({ quality: 80 })
                        .toBuffer();
                }
                else {
                    webpBuffer = await new lib.image.Transformer(buffer)
                        .resize(512, 512)
                        .webp(80);
                }
                if (webpBuffer.length > 1024 * 1024) {
                    throw new Boom(`Sticker at index ${index} exceeds the 1MB size limit`, { statusCode: 400 });
                }
                const hash = sha256(webpBuffer).toString('base64').replace(/\//g, '-');
                const fileName = `${hash}.webp`;
                stickerData[fileName] = [new Uint8Array(webpBuffer), { level: 0 }];
                stickerMetadata[index] = {
                    fileName,
                    mimetype: 'image/webp',
                    isAnimated,
                    emojis: sticker.emojis || ['✨'],
                    accessibilityLabel: sticker.accessibilityLabel || '‎'
                };
            })(j));
        }
        await Promise.all(promises);
    }
    const trayIconFileName = `${stickerPackIdValue}.webp`;
    const { stream: coverStream } = await getStream(cover);
    const coverBuffer = await toBuffer(coverStream);
    let coverWebpBuffer;
    if (isWebPBuffer(coverBuffer)) {
        coverWebpBuffer = coverBuffer;
    }
    else if (hasSharp) {
        coverWebpBuffer = await lib.sharp.default(coverBuffer)
            .resize(512, 512, { fit: 'inside' })
            .webp({ quality: 80 })
            .toBuffer();
    }
    else {
        coverWebpBuffer = await new lib.image.Transformer(coverBuffer)
            .resize(512, 512)
            .webp(80);
    }
    stickerData[trayIconFileName] = [new Uint8Array(coverWebpBuffer), { level: 0 }];
    const zipBuffer = await new Promise((resolve, reject) => {
        zip(stickerData, (error, data) => error ? reject(error) : resolve(Buffer.from(data)));
    });
    const stickerPackUpload = await encryptedStream(zipBuffer, 'sticker-pack', {
        logger,
        opts: options.options
    });
    let stickerPackUploadResult;
    try {
        stickerPackUploadResult = await options.upload(stickerPackUpload.encFilePath, {
            fileEncSha256B64: stickerPackUpload.fileEncSha256.toString('base64'),
            mediaType: 'sticker-pack',
            timeoutMs: options.mediaUploadTimeoutMs
        });
    }
    finally {
        fs.unlink(stickerPackUpload.encFilePath).catch(() => logger?.warn('failed to remove tmp file'));
    }
    const obj = {
        name,
        publisher,
        stickerPackId: stickerPackIdValue,
        packDescription: description,
        stickerPackOrigin: proto.Message.StickerPackMessage.StickerPackOrigin.USER_CREATED,
        stickerPackSize: zipBuffer.length,
        stickers: stickerMetadata,
        fileSha256: stickerPackUpload.fileSha256,
        fileEncSha256: stickerPackUpload.fileEncSha256,
        mediaKey: stickerPackUpload.mediaKey,
        directPath: stickerPackUploadResult.directPath,
        fileLength: stickerPackUpload.fileLength,
        mediaKeyTimestamp: unixTimestampSeconds(),
        trayIconFileName
    };
    try {
        let thumbnailBuffer;
        if (hasSharp) {
            thumbnailBuffer = await lib.sharp.default(coverBuffer).resize(252, 252).jpeg().toBuffer();
        }
        else if (hasImage) {
            thumbnailBuffer = await new lib.image.Transformer(coverBuffer).resize(252, 252).jpeg();
        }
        else if (hasJimp) {
            const jimpImage = await lib.jimp.Jimp.read(coverBuffer);
            thumbnailBuffer = await jimpImage.resize({ w: 252, h: 252 }).getBuffer('image/jpeg');
        }
        else {
            throw new Error('No image processing library available for thumbnail generation');
        }
        if (!thumbnailBuffer || thumbnailBuffer.length === 0) {
            throw new Error('Failed to generate thumbnail buffer');
        }
        const thumbUpload = await encryptedStream(thumbnailBuffer, 'thumbnail-sticker-pack', {
            logger,
            opts: options.options,
            mediaKey: stickerPackUpload.mediaKey
        });
        let thumbUploadResult;
        try {
            thumbUploadResult = await options.upload(thumbUpload.encFilePath, {
                fileEncSha256B64: thumbUpload.fileEncSha256.toString('base64'),
                mediaType: 'thumbnail-sticker-pack',
                timeoutMs: options.mediaUploadTimeoutMs
            });
        }
        finally {
            fs.unlink(thumbUpload.encFilePath).catch(() => logger?.warn('failed to remove tmp file'));
        }
        Object.assign(obj, {
            thumbnailDirectPath: thumbUploadResult.directPath,
            thumbnailSha256: thumbUpload.fileSha256,
            thumbnailEncSha256: thumbUpload.fileEncSha256,
            thumbnailHeight: 252,
            thumbnailWidth: 252,
            imageDataHash: sha256(thumbnailBuffer).toString('base64')
        });
    }
    catch (error) {
        logger?.warn(`Thumbnail generation failed: ${error}`);
    }
    if (cacheableKey) {
        logger?.debug({ cacheableKey }, 'set cache (background)');
        options.mediaCache.set(cacheableKey, WAProto.Message.StickerPackMessage.encode(obj).finish());
    }
    return WAProto.Message.StickerPackMessage.fromObject(obj);
};

const NATIVE_FLOW_PAYLOAD_BUTTONS = [
    'payment_info',
    'review_and_pay',
    'wa_payment_transaction_details',
    'mpm',
    'payment_key_info',
    'booking_confirmation',
    'card_message',
    'order_details',
    'order_status',
    'payment_status',
    'payment_method'
];
const NATIVE_FLOW_ID_BUTTONS = {
    cta_reminder: { display_text: '⏰ Remind me', icon: 'ALARM' },
    cta_cancel_reminder: { display_text: '🚫 Cancel reminder', icon: 'ALARM' },
    address_message: { display_text: '📍 Send address', icon: 'LOCATION' },
    track_order: { display_text: '🚚 Track order', icon: 'VEHICLE' },
    reorder: { display_text: '🔁 Reorder', icon: 'CART' },
    cancel_order: { display_text: '❌ Cancel order', icon: 'CANCEL' },
    voice_call: { display_text: '📞 Voice call', icon: 'PHONE' },
    video_call_button: { display_text: '🎥 Video call', icon: 'VIDEO' }
};
const NATIVE_FLOW_BARE_BUTTONS = {
    send_location: { icon: 'LOCATION' },
    cta_catalog: { icon: 'SHOP' },
    automated_greeting_message_view_catalog: { icon: 'SHOP' },
    call_permission_request: { icon: 'PHONE' },
    clear_chat: { icon: 'TRASH' }
};
const NATIVE_FLOW_BUTTON_NAMES = new Set([
    ...Object.keys(NATIVE_FLOW_ID_BUTTONS),
    ...Object.keys(NATIVE_FLOW_BARE_BUTTONS),
    ...NATIVE_FLOW_PAYLOAD_BUTTONS,
    'flow'
]);

const buildNativeFlowButton = (button, buttonText, buttonIcon) => {
    if (hasOptionalProperty(button, 'name') && NATIVE_FLOW_BUTTON_NAMES.has(button.name)) {
        if (button.name === 'flow') {
            const flow = button.flow || {};
            if (!flow.id) {
                throw new Boom('flow button requires flow.id', { statusCode: 400 });
            }
            return {
                name: 'flow',
                buttonParamsJson: JSON.stringify({
                    flow_message_version: flow.version || '3',
                    flow_token: flow.token || generateMessageIDV2(),
                    flow_id: flow.id,
                    flow_cta: flow.cta || buttonText || 'Continue',
                    flow_action: flow.action || 'navigate',
                    flow_action_payload: flow.actionPayload ||
                        { screen: flow.screen || 'WELCOME', data: flow.data || {} }
                })
            };
        }
        if (NATIVE_FLOW_PAYLOAD_BUTTONS.includes(button.name)) {
            const payload = button.payload || button.params;
            if (typeof payload !== 'object' || payload === null || Array.isArray(payload)) {
                throw new Boom(`${button.name} button requires a payload object`, { statusCode: 400 });
            }
            return { name: button.name, buttonParamsJson: JSON.stringify(payload) };
        }
        if (NATIVE_FLOW_BARE_BUTTONS[button.name]) {
            return {
                name: button.name,
                buttonParamsJson: JSON.stringify({
                    ...(buttonText ? { display_text: buttonText } : {}),
                    ...NATIVE_FLOW_BARE_BUTTONS[button.name],
                    ...button.options
                })
            };
        }
        if (!button.id) {
            throw new Boom(`${button.name} button requires an id`, { statusCode: 400 });
        }
        return {
            name: button.name,
            buttonParamsJson: JSON.stringify({
                display_text: buttonText || NATIVE_FLOW_ID_BUTTONS[button.name].display_text,
                id: button.id,
                icon: buttonIcon || NATIVE_FLOW_ID_BUTTONS[button.name].icon,
                ...button.options
            })
        };
    }
    if (hasOptionalProperty(button, 'id') && !!button.id) {
        return {
            name: 'quick_reply',
            buttonParamsJson: JSON.stringify({
                display_text: buttonText || '👉🏻 Click',
                id: button.id,
                icon: buttonIcon
            })
        };
    }
    else if (hasOptionalProperty(button, 'copy') && !!button.copy) {
        return {
            name: 'cta_copy',
            buttonParamsJson: JSON.stringify({
                display_text: buttonText || '📋 Copy',
                copy_code: button.copy,
                icon: buttonIcon
            })
        };
    }
    else if (hasOptionalProperty(button, 'url') && !!button.url) {
        return {
            name: 'cta_url',
            buttonParamsJson: JSON.stringify({
                display_text: buttonText || '🌐 Visit',
                url: button.url,
                merchant_url: button.url,
                webview_interaction: button.useWebview,
                icon: buttonIcon
            })
        };
    }
    else if (hasOptionalProperty(button, 'call') && !!button.call) {
        return {
            name: 'cta_call',
            buttonParamsJson: JSON.stringify({
                display_text: buttonText || '📞 Call',
                phone_number: button.call,
                icon: buttonIcon
            })
        };
    }
    else if (hasOptionalProperty(button, 'sections') && !!button.sections) {
        return {
            name: 'single_select',
            buttonParamsJson: JSON.stringify({
                title: buttonText || '📋 Select',
                sections: button.sections,
                icon: buttonIcon
            })
        };
    }
    else if (hasOptionalProperty(button, 'webview') && !!button.webview) {
        return {
            name: 'open_webview',
            buttonParamsJson: JSON.stringify({
                title: buttonText || button.webview.title || '🌐 Open',
                link: { url: button.webview.url },
                ...button.options
            })
        };
    }
    else if (hasOptionalProperty(button, 'navigateToScreen') && !!button.navigateToScreen) {
        return {
            name: 'navigateToScreen',
            buttonParamsJson: JSON.stringify({
                screen_name: button.navigateToScreen,
                data: button.data || {}
            })
        };
    }
    return button;
};

const prepareNativeFlowButtons = (message) => {
    const buttons = message.nativeFlow;
    const isButtonsFieldArray = Array.isArray(buttons);
    const correctedField = isButtonsFieldArray ? buttons : buttons.buttons;
    const messageParamsJson = {};

    if (hasOptionalProperty(message, 'offerText') && !!message.offerText) {
        Object.assign(messageParamsJson, {
            limited_time_offer: {
                text: message.offerText || LIBRARY_NAME,
                url: message.offerUrl ,
                copy_code: message.offerCode,
                expiration_time: message.offerExpiration
            }
        });
    }
    if (hasOptionalProperty(message, 'optionText') && !!message.optionText) {
        Object.assign(messageParamsJson, {
            bottom_sheet: {
                in_thread_buttons_limit: 1,
                divider_indices: Array.from({ length: correctedField.length }, (_, index) => index),
                list_title: message.optionTitle || '📄 Select Options',
                button_title: message.optionText
            }
        });
    }
    if (hasOptionalProperty(message, 'tapTarget') && !!message.tapTarget) {
        Object.assign(messageParamsJson, {
            tap_target_configuration: {
                title: message.tapTarget.title || '',
                description: message.tapTarget.description || '',
                canonical_url: message.tapTarget.canonicalUrl || '',
                domain: message.tapTarget.domain || '',
                buttonIndex: message.tapTarget.buttonIndex || 0
            }
        });
    }
    return {
        buttons: correctedField.map(button => {
            const buttonText = button.text || button.buttonText;
            const buttonIcon = button.icon?.toUpperCase();
            return buildNativeFlowButton(button, buttonText, buttonIcon);
        }),
        messageParamsJson: JSON.stringify(messageParamsJson)
    };
};
export const prepareDisappearingMessageSettingContent = (ephemeralExpiration) => {
    ephemeralExpiration = ephemeralExpiration || 0;
    const content = {
        ephemeralMessage: {
            message: {
                protocolMessage: {
                    type: WAProto.Message.ProtocolMessage.Type.EPHEMERAL_SETTING,
                    ephemeralExpiration
                }
            }
        }
    };
    return WAProto.Message.fromObject(content);
};
/**
 * Generate forwarded message content like WA does
 * @param message the message to forward
 * @param options.forceForward will show the message as forwarded even if it is from you
 */
export const generateForwardMessageContent = (message, forceForward) => {
    let content = message.message;
    if (!content) {
        throw new Boom('no content in message', { statusCode: 400 });
    }
    // hacky copy
    content = normalizeMessageContent(content);
    content = proto.Message.decode(proto.Message.encode(content).finish());
    let key = Object.keys(content)[0];
    let score = content?.[key]?.contextInfo?.forwardingScore || 0;
    score += message.key.fromMe && !forceForward ? 0 : 1;
    if (key === 'conversation') {
        content.extendedTextMessage = { text: content[key] };
        delete content.conversation;
        key = 'extendedTextMessage';
    }
    const key_ = content?.[key];
    if (score > 0) {
        key_.contextInfo = { forwardingScore: score, isForwarded: true };
    }
    else {
        key_.contextInfo = {};
    }
    return content;
};
export const hasNonNullishProperty = (message, key) => {
    return message != null &&
        typeof message === 'object' &&
        key in message &&
        message[key] != null;
};
export const hasOptionalProperty = (obj, key) => {
    return obj != null &&
        typeof obj === 'object' &&
        key in obj &&
        obj[key] != null;
};

export const hasValidAlbumMedia = (message) => {
    return !!(message.imageMessage ||
        message.videoMessage);
};
export const hasValidInteractiveHeader = (message) => {
    return !!(message.imageMessage ||
        message.videoMessage ||
        message.documentMessage ||
        message.productMessage ||
        message.locationMessage);
};

export const hasValidCarouselHeader = (message) => {
    return !!(message.imageMessage ||
        message.videoMessage ||
        message.productMessage);
};
export const generateWAMessageContent = async (message, options) => {
    var _a, _b;
    let m = {};

    if (hasNonNullishProperty(message, 'raw')) {
        delete message.raw;
        return message;
    }

    else if (hasNonNullishProperty(message, 'code') ||
        hasNonNullishProperty(message, 'links') ||
        hasNonNullishProperty(message, 'table') ||
        hasNonNullishProperty(message, 'richResponse')) {
        m = prepareRichResponseMessage(message);
    }
    else if (hasNonNullishProperty(message, 'text')) {
        const extContent = { text: message.text };
        let urlInfo = message.linkPreview;
        if (typeof urlInfo === 'undefined') {
            urlInfo = await generateLinkPreviewIfRequired(message.text, options.getUrlInfo, options.logger);
        }
        if (urlInfo) {
            extContent.matchedText = urlInfo['matched-text'];
            extContent.jpegThumbnail = urlInfo.jpegThumbnail;
            extContent.description = urlInfo.description;
            extContent.title = urlInfo.title;
            extContent.previewType = urlInfo.previewType ?? 0;
            extContent.linkPreviewMetadata = urlInfo.linkPreviewMetadata;
            const img = urlInfo.highQualityThumbnail;
            if (img) {
                extContent.thumbnailDirectPath = img.directPath;
                extContent.mediaKey = img.mediaKey;
                extContent.mediaKeyTimestamp = img.mediaKeyTimestamp;
                extContent.thumbnailWidth = img.width;
                extContent.thumbnailHeight = img.height;
                extContent.thumbnailSha256 = img.fileSha256;
                extContent.thumbnailEncSha256 = img.fileEncSha256;
            }
        }
        const faviconData = message.favicon;
        if (faviconData && typeof options.upload === 'function') {
            const { imageMessage } = await prepareWAMessageMedia({
                image: faviconData
            }, options);
            extContent.faviconMMSMetadata = {
                thumbnailDirectPath: imageMessage.directPath,
                mediaKey: imageMessage.mediaKey,
                mediaKeyTimestamp: imageMessage.mediaKeyTimestamp,
                thumbnailWidth: 32,
                thumbnailHeight: 32,
                thumbnailSha256: imageMessage.fileSha256,
                thumbnailEncSha256: imageMessage.fileEncSha256
            };
        }
        if (options.backgroundColor) {
            extContent.backgroundArgb = await assertColor(options.backgroundColor);
        }
        if (options.font) {
            extContent.font = options.font;
        }
        m.extendedTextMessage = extContent;
    }
    else if (hasNonNullishProperty(message, 'contacts')) {
        const contactLen = message.contacts.contacts.length;
        if (!contactLen) {
            throw new Boom('require atleast 1 contact', { statusCode: 400 });
        }
        if (contactLen === 1) {
            m.contactMessage = WAProto.Message.ContactMessage.create(message.contacts.contacts[0]);
        }
        else {
            m.contactsArrayMessage = WAProto.Message.ContactsArrayMessage.create(message.contacts);
        }
    }
    else if (hasNonNullishProperty(message, 'location')) {
        m.locationMessage = WAProto.Message.LocationMessage.create(message.location);
    }
    else if (hasNonNullishProperty(message, 'react')) {
        if (!message.react.senderTimestampMs) {
            message.react.senderTimestampMs = Date.now();
        }
        m.reactionMessage = WAProto.Message.ReactionMessage.create(message.react);
    }
    else if (hasNonNullishProperty(message, 'delete')) {
        m.protocolMessage = {
            key: message.delete,
            type: WAProto.Message.ProtocolMessage.Type.REVOKE
        };
    }
    else if (hasNonNullishProperty(message, 'forward')) {
        m = generateForwardMessageContent(message.forward, message.force);
    }
    else if (hasNonNullishProperty(message, 'disappearingMessagesInChat')) {
        const exp = typeof message.disappearingMessagesInChat === 'boolean'
            ? message.disappearingMessagesInChat
                ? WA_DEFAULT_EPHEMERAL
                : 0
            : message.disappearingMessagesInChat;
        m = prepareDisappearingMessageSettingContent(exp);
    }
    else if (hasNonNullishProperty(message, 'groupInvite')) {
        m.groupInviteMessage = {};
        m.groupInviteMessage.inviteCode = message.groupInvite.inviteCode;
        m.groupInviteMessage.inviteExpiration = message.groupInvite.inviteExpiration;
        m.groupInviteMessage.caption = message.groupInvite.text;
        m.groupInviteMessage.groupJid = message.groupInvite.jid;
        m.groupInviteMessage.groupName = message.groupInvite.subject;
        //TODO: use built-in interface and get disappearing mode info etc.
        //TODO: cache / use store!?
        if (options.getProfilePicUrl) {
            const pfpUrl = await options.getProfilePicUrl(message.groupInvite.jid, 'preview');
            if (pfpUrl) {
                const resp = await fetch(pfpUrl, { method: 'GET', dispatcher: options?.options?.dispatcher });
                if (resp.ok) {
                    const buf = Buffer.from(await resp.arrayBuffer());
                    m.groupInviteMessage.jpegThumbnail = buf;
                }
            }
        }
    }
    else if (hasNonNullishProperty(message, 'stickers')) {
        m.stickerPackMessage = await prepareStickerPackMessage(message, options);
    }
    else if (hasNonNullishProperty(message, 'pin')) {
        m.pinInChatMessage = {};
        m.messageContextInfo = {};
        m.pinInChatMessage.key = message.pin;
        m.pinInChatMessage.type = message.type;
        m.pinInChatMessage.senderTimestampMs = Date.now();
        m.messageContextInfo.messageAddOnDurationInSecs = message.type === 1 ? message.time || 86400 : 0;
    }
    else if (hasNonNullishProperty(message, 'keep')) {
        m.keepInChatMessage = {};
        m.keepInChatMessage.key = message.keep;
        m.keepInChatMessage.keepType = message.type;
        m.keepInChatMessage.timestampMs = Date.now();
    }
    else if (hasNonNullishProperty(message, 'flowReply')) {
        m.interactiveResponseMessage = {
            body: {
                format: message.flowReply.format || proto.Message.InteractiveResponseMessage.Body.Format.DEFAULT,
                text: message.flowReply.text
            },
            nativeFlowResponseMessage: {
                name: message.flowReply.name,
                paramsJson: message.flowReply.paramsJson || '{}',
                version: message.flowReply.version || 1
            }
        };
    }
    else if (hasNonNullishProperty(message, 'buttonReply')) {
        switch (message.type) {
            case 'template':
                m.templateButtonReplyMessage = {
                    selectedDisplayText: message.buttonReply.displayText,
                    selectedId: message.buttonReply.id,
                    selectedIndex: message.buttonReply.index
                };
                break;
            case 'plain':
                m.buttonsResponseMessage = {
                    selectedButtonId: message.buttonReply.id,
                    selectedDisplayText: message.buttonReply.displayText,
                    type: proto.Message.ButtonsResponseMessage.Type.DISPLAY_TEXT
                };
                break;
        }
    }
    else if (hasNonNullishProperty(message, 'listReply')) {
        m.listResponseMessage = {
            description: message.listReply.description,
            listType: proto.Message.ListResponseMessage.ListType.SINGLE_SELECT,
            singleSelectReply: {
                selectedRowId: message.listReply.id
            },
            title: message.listReply.title
        };
    }
    else if (hasOptionalProperty(message, 'ptv') && message.ptv) {
        const { videoMessage } = await prepareWAMessageMedia({ video: message.video }, options);
        m.ptvMessage = videoMessage;
    }
    else if (hasNonNullishProperty(message, 'product')) {
        m.productMessage = await prepareProductMessage(message, options);
    }
    else if (hasNonNullishProperty(message, 'event')) {
        m.eventMessage = {};
        const startTime = Math.floor(message.event.startDate.getTime() / 1000);
        if (message.event.call && options.getCallLink) {
            const token = await options.getCallLink(message.event.call, { startTime });
            m.eventMessage.joinLink = (message.event.call === 'audio' ? CALL_AUDIO_PREFIX : CALL_VIDEO_PREFIX) + token;
        }
        m.messageContextInfo = {
            // encKey
            messageSecret: message.event.messageSecret || randomBytes(32)
        };
        m.eventMessage.name = message.event.name;
        m.eventMessage.description = message.event.description;
        m.eventMessage.startTime = startTime;
        m.eventMessage.endTime = message.event.endDate ? message.event.endDate.getTime() / 1000 : undefined;
        m.eventMessage.isCanceled = message.event.isCancelled ?? false;
        m.eventMessage.extraGuestsAllowed = message.event.extraGuestsAllowed;
        m.eventMessage.isScheduleCall = message.event.isScheduleCall ?? false;
        m.eventMessage.location = message.event.location;
    }
    else if (hasNonNullishProperty(message, 'poll')) {
        (_a = message.poll).selectableCount || (_a.selectableCount = 0);
        (_b = message.poll).toAnnouncementGroup || (_b.toAnnouncementGroup = false);
        if (!Array.isArray(message.poll.values)) {
            throw new Boom('Invalid poll values', { statusCode: 400 });
        }
        if (message.poll.selectableCount < 0 || message.poll.selectableCount > message.poll.values.length) {
            throw new Boom(`poll.selectableCount in poll should be >= 0 and <= ${message.poll.values.length}`, {
                statusCode: 400
            });
        }
        const pollCreationMessage = {
            name: message.poll.name,
            selectableOptionsCount: message.poll.selectableCount,
            options: message.poll.values.map(optionName => ({ optionName })),
            endTime: message.poll.endDate ? message.poll.endDate.getTime() : undefined,

            hideVoterNames: message.poll.hideVoter ?? message.poll.hideVoterNames ?? false,
            allowAddOption: message.poll.canAddOption ?? false
        };
        if (message.poll.toAnnouncementGroup) {
            // poll v2 is for community announcement groups (single select and multiple)
            m.pollCreationMessageV2 = pollCreationMessage;
        }

        else if (message.poll.pollVersion === 6) {
            m.pollCreationMessageV6 = pollCreationMessage;
        }
        else {

            if (message.poll.pollType === 1) {
                if (!message.poll.correctAnswer) {
                    throw new Boom('No "correctAnswer" provided for quiz', { statusCode: 400 });
                }
                m.pollCreationMessageV5 = {

                    ...pollCreationMessage,
                    correctAnswer: {
                        optionName: message.poll.correctAnswer.toString()
                    },
                    pollType: 1,
                    selectableOptionsCount: 1
                };
            }
            else if (message.poll.selectableCount === 1) {
                //poll v3 is for single select polls
                m.pollCreationMessageV3 = pollCreationMessage;
            }
            else {
                // poll for multiple choice polls
                m.pollCreationMessage = pollCreationMessage;
            }
        }
        m.messageContextInfo = {
            // encKey
            messageSecret: message.poll.messageSecret || randomBytes(32)
        };
    }

    else if (hasNonNullishProperty(message, 'pollResult')) {
        const pollResultSnapshotMessage = {
            name: message.pollResult.name,
            pollVotes: message.pollResult.votes.map(vote => ({
                optionName: vote.name,
                optionVoteCount: parseInt(vote.voteCount)
            }))
        };
        if (message.pollResult.pollType === 1) {
            pollResultSnapshotMessage.pollType = proto.Message.PollType.QUIZ;
            m.pollResultSnapshotMessageV3 = pollResultSnapshotMessage;
        }
        else {
            pollResultSnapshotMessage.pollType = proto.Message.PollType.POLL;
            m.pollResultSnapshotMessage = pollResultSnapshotMessage;
        }
    }

    else if (hasNonNullishProperty(message, 'pollUpdate')) {
        if (!message.pollUpdate.key) {
            throw new Boom('Message key is required', { statusCode: 400 });
        }
        if (!message.pollUpdate.vote) {
            throw new Boom('Encrypted vote payload is required', { statusCode: 400 });
        }
        m.pollUpdateMessage = {
            metadata: message.pollUpdate.metadata,
            pollCreationMessageKey: message.pollUpdate.key,
            senderTimestampMs: Date.now(),
            vote: message.pollUpdate.vote
        };
    }

    else if (hasNonNullishProperty(message, 'paymentInviteServiceType')) {
        m.paymentInviteMessage = {
            expiryTimestamp: Date.now(),
            serviceType: message.paymentInviteServiceType
        };
    }

    else if (hasNonNullishProperty(message, 'orderText')) {
        if (!Buffer.isBuffer(message.thumbnail)) {
            throw new Boom('Must provide thumbnail buffer in order message', { statusCode: 400 });
        }
        m.orderMessage = {
            itemCount: 1,
            messageVersion: 1,
            orderTitle: LIBRARY_NAME,
            status: proto.Message.OrderMessage.OrderStatus.INQUIRY,
            surface: proto.Message.OrderMessage.OrderSurface.CATALOG,
            token: generateMessageIDV2(),
            totalAmount1000: 1000,
            totalCurrencyCode: 'IDR',
            ...message,
            message: message.orderText
        };
        delete m.orderMessage.orderText;
    }

    else if (hasNonNullishProperty(message, 'album')) {
        if (!Array.isArray(message.album)) {
            throw new Boom('Invalid album type. Expected an array.', { statusCode: 400 });
        }
        let videoCount = 0;
        for (let i = 0; i < message.album.length; i++) {
            if (message.album[i].video)
                videoCount++;
        }
        ;
        let imageCount = 0;
        for (let i = 0; i < message.album.length; i++) {
            if (message.album[i].image)
                imageCount++;
        }
        ;
        if ((videoCount + imageCount) < 2) {
            throw new Boom('Minimum provide 2 media to upload album message', { statusCode: 400 });
        }
        m.albumMessage = {
            expectedImageCount: imageCount,
            expectedVideoCount: videoCount
        };
    }
    else if (hasNonNullishProperty(message, 'sharePhoneNumber')) {
        m.protocolMessage = {
            type: proto.Message.ProtocolMessage.Type.SHARE_PHONE_NUMBER
        };
    }
    else if (hasNonNullishProperty(message, 'requestPhoneNumber')) {
        m.requestPhoneNumberMessage = {};
    }
    else if (hasNonNullishProperty(message, 'limitSharing')) {
        m.protocolMessage = {
            type: proto.Message.ProtocolMessage.Type.LIMIT_SHARING,
            limitSharing: {
                sharingLimited: message.limitSharing === true,
                trigger: 1,
                limitSharingSettingTimestamp: Date.now(),
                initiatedByMe: true
            }
        };
    }

    // interactive branches below (buttons/templateButtons/nativeFlow/cards/invoiceNote/...) would
    // try to wrap an empty `m` whenever a media key is sent alongside them
    else if (Object.keys(message).some(mediaKey => MEDIA_KEYS.includes(mediaKey))) {
        m = await prepareWAMessageMedia(message, options);
    }
    if (hasNonNullishProperty(message, 'buttons')) {
        const buttonsMessage = {
            buttons: message.buttons.map(button => {

                const buttonText = button.text || button.buttonText;
                if (hasOptionalProperty(button, 'sections')) {
                    return {
                        nativeFlowInfo: {
                            name: 'single_select',
                            paramsJson: JSON.stringify({
                                title: buttonText,
                                sections: button.sections
                            })
                        },
                        type: ButtonType.NATIVE_FLOW
                    };
                }
                else if (hasOptionalProperty(button, 'name')) {
                    return {
                        nativeFlowInfo: {
                            name: button.name,
                            paramsJson: button.paramsJson
                        },
                        type: ButtonType.NATIVE_FLOW
                    };
                }
                return {
                    buttonId: button.id || button.buttonId,
                    buttonText: typeof buttonText === 'string' ? { displayText: buttonText } : buttonText,
                    type: button.type || ButtonType.RESPONSE
                };
            })
        };
        if (hasOptionalProperty(message, 'text')) {
            buttonsMessage.contentText = message.text;
            buttonsMessage.headerType = ButtonHeaderType.EMPTY;
        }
        else {
            if (hasOptionalProperty(message, 'caption')) {
                buttonsMessage.contentText = message.caption;
            }
            const type = Object.keys(m)[0].replace('Message', '').toUpperCase();
            buttonsMessage.headerType = ButtonHeaderType[type];
            Object.assign(buttonsMessage, m);
        }
        if (hasOptionalProperty(message, 'footer')) {
            buttonsMessage.footerText = message.footer;
        }
        m = { buttonsMessage };
    }
    else if (hasNonNullishProperty(message, 'sections')) {
        const listMessage = {
            sections: message.sections,
            buttonText: message.buttonText,
            title: message.title,
            footerText: message.footer,
            description: message.text,
            listType: ListType.SINGLE_SELECT
        };
        m = { listMessage };
    }

    else if (hasNonNullishProperty(message, 'templateButtons')) {
        const hydratedTemplate = {
            hydratedButtons: message.templateButtons.map((button, i) => {
                const buttonText = button.text || button.buttonText;
                if (hasOptionalProperty(button, 'id')) {
                    return {
                        index: i,
                        quickReplyButton: {
                            displayText: buttonText || '👉🏻 Click',
                            id: button.id
                        }
                    };
                }
                else if (hasOptionalProperty(button, 'url')) {
                    return {
                        index: i,
                        urlButton: {
                            displayText: buttonText || '🌐 Visit',
                            url: button.url
                        }
                    };
                }
                else if (hasOptionalProperty(button, 'call')) {
                    return {
                        index: i,
                        callButton: {
                            displayText: buttonText || '📞 Call',
                            phoneNumber: button.call
                        }
                    };
                }
                button.index = button.index || i;
                return button;
            })
        };
        if (hasOptionalProperty(message, 'text')) {
            hydratedTemplate.hydratedContentText = message.text;
        }
        else {
            if (hasOptionalProperty(message, 'caption')) {
                hydratedTemplate.hydratedTitleText = message.title;
                hydratedTemplate.hydratedContentText = message.caption;
            }
            ;
            Object.assign(hydratedTemplate, m);
        }
        if (hasOptionalProperty(message, 'footer')) {
            hydratedTemplate.hydratedFooterText = message.footer;
        }
        hydratedTemplate.templateId = message.id || 'template-' + Date.now();
        m = {
            templateMessage: {
                hydratedFourRowTemplate: hydratedTemplate,
                hydratedTemplate: hydratedTemplate
            }
        };
    }
    else if (hasNonNullishProperty(message, 'nativeFlow')) {
        const interactiveMessage = {
            nativeFlowMessage: prepareNativeFlowButtons(message)
        };
        if (hasOptionalProperty(message, 'bizJid')) {
            interactiveMessage.collectionMessage = {
                bizJid: message.bizJid,
                id: message.id,
                messageVersion: 1
            };
        }
        else if (hasOptionalProperty(message, 'shopSurface')) {
            interactiveMessage.shopStorefrontMessage = {
                surface: message.shopSurface,
                id: message.id,
                messageVersion: 1
            };
        }
        if (hasOptionalProperty(message, 'text')) {
            interactiveMessage.body = { text: message.text };
        }
        else {
            const isValidHeader = hasValidInteractiveHeader(m);

            // must not crash when `interactiveMessage.header` was never created
            if (hasOptionalProperty(message, 'caption') && !isValidHeader) {
                throw new Boom('Invalid media type for interactive message header', { statusCode: 400 });
            }
            if (isValidHeader) {
                interactiveMessage.header = {
                    title: message.title || '',
                    subtitle: message.subtitle || '',
                    hasMediaAttachment: true
                };
                if (hasOptionalProperty(message, 'caption')) {
                    interactiveMessage.body = { text: message.caption };
                }
                Object.assign(interactiveMessage.header, m);
            }
            if (hasOptionalProperty(message, 'thumbnail') && !!message.thumbnail) {
                interactiveMessage.jpegThumbnail = message.thumbnail;
            }
        }
        if (hasOptionalProperty(message, 'audioFooter')) {
            const { audioMessage } = await prepareWAMessageMedia({
                audio: message.audioFooter
            }, options);
            interactiveMessage.footer = {
                audioMessage,
                hasMediaAttachment: true
            };
        }
        else if (hasOptionalProperty(message, 'footer')) {
            interactiveMessage.footer = { text: message.footer };
        }
        m = { interactiveMessage };
    }
    else if (hasNonNullishProperty(message, 'cards')) {
        const carouselCards = Array.isArray(message.cards) ? message.cards : [];
        if (carouselCards.length === 0) {
            throw new Boom('Carousel requires at least one card', { statusCode: 400 });
        }
        if (carouselCards.length > MAX_CAROUSEL_CARDS) {
            throw new Boom(`Carousel supports at most ${MAX_CAROUSEL_CARDS} cards, got ${carouselCards.length}`, { statusCode: 400 });
        }
        const interactiveMessage = {
            header: { hasMediaAttachment: false },
            carouselMessage: {
                cards: await Promise.all(carouselCards.map(async (card, cardIndex) => {
                    let carouselHeader = {};
                    if (hasNonNullishProperty(card, 'product')) {
                        carouselHeader.productMessage = await prepareProductMessage(card, options);
                    }
                    else {
                        carouselHeader = await prepareWAMessageMedia(card, options).catch(() => ({}));
                    }
                    const isValidHeader = hasValidCarouselHeader(carouselHeader);
                    if (!isValidHeader) {
                        throw new Boom(`Invalid media type for carousel card [${cardIndex}]: a card needs an image, video or product in its header`, { statusCode: 400 });
                    }
                    const carouselCard = {
                        nativeFlowMessage: prepareNativeFlowButtons(card.nativeFlow ? card : [])
                    };
                    if (hasOptionalProperty(card, 'text')) {
                        carouselCard.body = { text: card.text };
                    }
                    else {
                        if (hasOptionalProperty(card, 'caption')) {
                            carouselCard.header = {
                                title: card.title || '',
                                subtitle: card.subtitle || '',
                                hasMediaAttachment: isValidHeader
                            };
                            carouselCard.body = { text: card.caption };
                        }
                        if (hasOptionalProperty(card, 'thumbnail') && !!card.thumbnail) {
                            carouselCard.jpegThumbnail = card.thumbnail;
                        }
                        Object.assign(carouselCard.header, carouselHeader);
                    }
                    if (hasOptionalProperty(card, 'audioFooter')) {
                        const { audioMessage } = await prepareWAMessageMedia({
                            audio: card.audioFooter
                        }, options);
                        carouselCard.footer = {
                            audioMessage,
                            hasMediaAttachment: true
                        };
                    }
                    else if (hasOptionalProperty(card, 'footer')) {
                        carouselCard.footer = { text: card.footer };
                    }
                    return carouselCard;
                })),
                carouselCardType: CarouselCardType.UNKNOWN,
                messageVersion: 1
            }
        };
        if (hasOptionalProperty(message, 'text')) {
            interactiveMessage.body = { text: message.text };
        }
        if (hasOptionalProperty(message, 'footer')) {
            interactiveMessage.footer = { text: message.footer };
        }
        if (hasNonNullishProperty(message, 'contextInfo')) {
            interactiveMessage.contextInfo = message.contextInfo;
        }
        m = { interactiveMessage };
    }

    else if (hasNonNullishProperty(message, 'requestPaymentFrom')) {
        const requestPaymentMessage = {
            amount: {
                currencyCode: 'IDR',
                offset: 1000,
                value: 1000
            },
            amount1000: 1000,
            currencyCodeIso4217: 'IDR',
            expiryTimestamp: Date.now(),
            noteMessage: m,
            requestFrom: message.requestPaymentFrom,
            ...message
        };
        delete requestPaymentMessage.requestPaymentFrom;
        if (hasNonNullishProperty(m, 'extendedTextMessage') || hasNonNullishProperty(m, 'stickerMessage')) {
            Object.assign(requestPaymentMessage.noteMessage, m);
        }
        else {
            throw new Boom('Invalid message type for request payment note message', { statusCode: 400 });
        }
        m = { requestPaymentMessage };
    }

    else if (hasNonNullishProperty(message, 'invoiceNote')) {
        const attachment = m.imageMessage || m.documentMessage;
        const type = Object.keys(m)[0].replace('Message', '').toUpperCase();
        const invoiceMessage = {
            attachmentType: proto.Message.InvoiceMessage.AttachmentType[type === 'DOCUMENT' ? 'PDF' : 'IMAGE'],
            note: message.invoiceNote
        };
        if (attachment) {
            const { directPath, fileEncSha256, fileSha256, jpegThumbnail = undefined, mediaKey, mediaKeyTimestamp, mimetype } = attachment;
            Object.assign(invoiceMessage, {
                attachmentDirectPath: directPath,
                attachmentFileEncSha256: fileEncSha256,
                attachmentFileSha256: fileSha256,
                attachmentJpegThumbnail: jpegThumbnail,
                attachmentMediaKey: mediaKey,
                attachmentMediaKeyTimestamp: mediaKeyTimestamp,
                attachmentMimetype: mimetype,
                token: generateMessageIDV2()
            });
        }
        else {
            throw new Boom('Invalid media type for invoice message', { statusCode: 400 });
        }
        m = { invoiceMessage };
    }

    else if (hasNonNullishProperty(message, 'music')) {
        const music = message.music;
        if (!music.songUri && !music.embeddedMusic) {
            throw new Boom('Either "songUri" or "embeddedMusic" is required for music message', { statusCode: 400 });
        }
        m.musicMessage = {
            embeddedMusic: music.embeddedMusic,
            songUri: music.songUri,
            artworkUri: music.artworkUri,
            style: music.style,
            contextInfo: music.contextInfo
        };
    }

    else if (hasNonNullishProperty(message, 'splitPayment')) {
        const split = message.splitPayment;
        if (!split.splitId) {
            throw new Boom('"splitId" is required for split payment message', { statusCode: 400 });
        }
        if (!Array.isArray(split.participants) || !split.participants.length) {
            throw new Boom('"participants" is required for split payment message', { statusCode: 400 });
        }
        m.splitPaymentMessage = {
            splitId: split.splitId,
            totalAmount: split.totalAmount,
            description: split.description,
            requesterJid: split.requesterJid,
            createdAtMs: split.createdAtMs || Date.now(),
            participants: split.participants.map(participant => ({
                jid: participant.jid,
                amount: participant.amount,
                status: participant.status || 0
            }))
        };
    }

    else if (hasNonNullishProperty(message, 'splitPaymentUpdate')) {
        const splitUpdate = message.splitPaymentUpdate;
        if (!splitUpdate.splitId) {
            throw new Boom('"splitId" is required for split payment update message', { statusCode: 400 });
        }
        m.splitPaymentUpdateMessage = {
            splitId: splitUpdate.splitId,
            participantJid: splitUpdate.participantJid
        };
    }

    else if (hasNonNullishProperty(message, 'paymentReminder')) {
        const reminder = message.paymentReminder;
        if (!reminder.reminderId) {
            throw new Boom('"reminderId" is required for payment reminder message', { statusCode: 400 });
        }
        m.paymentReminderMessage = {
            reminderId: reminder.reminderId,
            instanceId: reminder.instanceId,
            description: reminder.description,
            frequency: reminder.frequency,
            status: reminder.status,
            payeeVpa: reminder.payeeVpa,
            payeeJid: reminder.payeeJid,
            payerJid: reminder.payerJid,
            amount: reminder.amount
        };
    }

    else if (hasNonNullishProperty(message, 'eventInvite')) {
        const eventInvite = message.eventInvite;
        if (!eventInvite.eventId) {
            throw new Boom('"eventId" is required for event invite message', { statusCode: 400 });
        }
        m.eventInviteMessage = {
            eventId: eventInvite.eventId,
            eventTitle: eventInvite.eventTitle,
            jpegThumbnail: eventInvite.jpegThumbnail,
            startTime: eventInvite.startTime,
            endTime: eventInvite.endTime,
            caption: eventInvite.caption,
            callLink: eventInvite.callLink,
            isCanceled: eventInvite.isCanceled ?? false
        };
    }

    else if (hasNonNullishProperty(message, 'pollAddOption')) {
        const pollAddOption = message.pollAddOption;
        if (!pollAddOption.key) {
            throw new Boom('"key" is required for poll add option message', { statusCode: 400 });
        }
        if (!pollAddOption.optionName) {
            throw new Boom('"optionName" is required for poll add option message', { statusCode: 400 });
        }
        m.pollAddOptionMessage = {
            pollCreationMessageKey: pollAddOption.key,
            addOption: { optionName: pollAddOption.optionName },
            metadata: pollAddOption.metadata
        };
    }

    else if (hasNonNullishProperty(message, 'conditionalReveal')) {
        const conditionalReveal = message.conditionalReveal;
        if (!Buffer.isBuffer(conditionalReveal.encPayload) || !Buffer.isBuffer(conditionalReveal.encIv)) {
            throw new Boom('"encPayload" and "encIv" must be buffer type', { statusCode: 400 });
        }
        m.conditionalRevealMessage = {
            encPayload: conditionalReveal.encPayload,
            encIv: conditionalReveal.encIv,
            conditionalRevealMessageType: conditionalReveal.conditionalRevealMessageType || 0,
            revealKeyId: conditionalReveal.revealKeyId
        };
    }

    else if (hasNonNullishProperty(message, 'statusLinkPreview')) {
        m.statusLinkPreviewMetadata = {
            style: message.statusLinkPreview.style ?? 0
        };
    }

    // so when text is present it must be nested in contextInfo instead of set at the top level
    if (hasNonNullishProperty(message, 'statusLinkPreview') && m.extendedTextMessage) {
        m.extendedTextMessage = {
            ...m.extendedTextMessage,
            contextInfo: {
                ...m.extendedTextMessage.contextInfo,
                statusLinkPreviewMetadata: {
                    style: message.statusLinkPreview.style ?? 0
                }
            }
        };
        delete m.statusLinkPreviewMetadata;
    }

    // 'Invalid media type' failure mode for unknown/empty payloads
    if (!Object.keys(m).length) {
        throw new Boom('Invalid media type', { statusCode: 400 });
    }

    if (hasOptionalProperty(message, 'externalAdReply') && !!message.externalAdReply) {
        const messageType = Object.keys(m)[0];
        const key = m[messageType];
        const content = message.externalAdReply;
        if ('thumbnail' in content && !Buffer.isBuffer(content.thumbnail)) {
            throw new Boom('Thumbnail must in buffer type', { statusCode: 400 });
        }
        if (!content.url || typeof content.url !== 'string') {
            content.url = 'eminence-tech-lk.vercel.app';
        }
        const externalAdReply = {
            ...content,
            body: content.body,
            mediaType: content.mediaType || 1,
            mediaUrl: content.url,
            renderLargerThumbnail: content.largeThumbnail,
            sourceUrl: content.url,
            thumbnail: content.thumbnail,
            thumbnailUrl: content.url + '?update=' + Date.now(),
            title: content.title || LIBRARY_NAME
        };
        delete externalAdReply.subTitle;
        delete externalAdReply.largeThumbnail;
        delete externalAdReply.url;
        if ('contextInfo' in key && !!key.contextInfo) {
            key.contextInfo.externalAdReply = { ...key.contextInfo.externalAdReply, ...externalAdReply };
        }
        else if (key) {
            key.contextInfo = { externalAdReply };
        }
    }
    if ((hasOptionalProperty(message, 'mentions') && message.mentions?.length) ||
        (hasOptionalProperty(message, 'mentionAll') && message.mentionAll)) {
        const messageType = Object.keys(m)[0];
        const key = m[messageType];
        if (key && 'contextInfo' in key) {
            key.contextInfo = key.contextInfo || {};
            if (message.mentions?.length) {
                key.contextInfo.mentionedJid = message.mentions;
            }
            if (message.mentionAll) {
                key.contextInfo.nonJidMentions = 1;
            }
        }
        else if (key) {
            key.contextInfo = {
                mentionedJid: message.mentions,
                nonJidMentions: message.mentionAll ? 1 : 0
            };
        }
    }
    if (hasOptionalProperty(message, 'contextInfo') && !!message.contextInfo) {
        const messageType = Object.keys(m)[0];
        const key = m[messageType];
        if ('contextInfo' in key && !!key.contextInfo) {
            key.contextInfo = { ...key.contextInfo, ...message.contextInfo };
        }
        else if (key) {
            key.contextInfo = message.contextInfo;
        }
    }

    if (hasOptionalProperty(message, 'groupStatus') && !!message.groupStatus) {
        const messageType = Object.keys(m)[0];
        const key = m[messageType];
        if ('contextInfo' in key && !!key.contextInfo) {
            key.contextInfo.isGroupStatus = message.groupStatus;
        }
        else if (key) {
            key.contextInfo = {
                isGroupStatus: message.groupStatus
            };
        }
        m = { groupStatusMessageV2: { message: m } };
        delete message.groupStatus;
    }

    if (hasOptionalProperty(message, 'spoiler') && !!message.spoiler) {
        const messageType = Object.keys(m)[0];
        const key = m[messageType];
        if ('contextInfo' in key && !!key.contextInfo) {
            key.contextInfo.isSpoiler = message.spoiler;
        }
        else if (key) {
            key.contextInfo = {
                isSpoiler: message.spoiler
            };
        }
        m = { spoilerMessage: { message: m } };
        delete message.spoiler;
    }

    else if (hasOptionalProperty(message, 'interactiveAsTemplate') && !!message.interactiveAsTemplate) {
        if (!m.interactiveMessage) {
            throw new Boom('Invalid message type for template', { statusCode: 400 });
        }
        m = {
            templateMessage: {
                interactiveMessageTemplate: m.interactiveMessage,
                templateId: message.id || 'template-' + Date.now()
            }
        };
        delete message.interactiveAsTemplate;
    }

    if (hasOptionalProperty(message, 'ephemeral') && !!message.ephemeral) {
        m = { ephemeralMessage: { message: m } };
        delete message.ephemeral;
    }

    if (hasOptionalProperty(message, 'isLottie') && !!message.isLottie) {
        m = { lottieStickerMessage: { message: m } };
    }
    else if (hasOptionalProperty(message, 'viewOnce') && !!message.viewOnce) {
        m = { viewOnceMessage: { message: m } };
    }

    else if (hasOptionalProperty(message, 'viewOnceV2') && !!message.viewOnceV2) {
        m = { viewOnceMessageV2: { message: m } };
        delete message.viewOnceV2;
    }

    else if (hasOptionalProperty(message, 'viewOnceV2Extension') && !!message.viewOnceV2Extension) {
        m = { viewOnceMessageV2Extension: { message: m } };
        delete message.viewOnceV2Extension;
    }
    if (hasOptionalProperty(message, 'edit')) {
        m = {
            protocolMessage: {
                key: message.edit,
                editedMessage: m,
                timestampMs: Date.now(),
                type: WAProto.Message.ProtocolMessage.Type.MESSAGE_EDIT
            }
        };
    }
    if (shouldIncludeReportingToken(m)) {
        m.messageContextInfo = m.messageContextInfo || {};
        if (!m.messageContextInfo.messageSecret) {
            m.messageContextInfo.messageSecret = randomBytes(32);
        }
    }
    return WAProto.Message.create(m);
};
export const generateWAMessageFromContent = (jid, message, options) => {
    // set timestamp to now
    // if not specified
    if (!options.timestamp) {
        options.timestamp = new Date();
    }
    const innerMessage = normalizeMessageContent(message);
    const messageContextInfo = message.messageContextInfo;
    const key = getContentType(innerMessage);
    const timestamp = unixTimestampSeconds(options.timestamp);
    const isNewsletter = isJidNewsletter(jid);
    const { quoted, userJid } = options;
    if (quoted) {
        const participant = quoted.key.fromMe
            ? userJid // TODO: Add support for LIDs
            : quoted.participant || quoted.key.participant || quoted.key.remoteJid;
        let quotedMsg = normalizeMessageContent(quoted.message);
        const msgType = getContentType(quotedMsg);
        // strip any redundant properties
        quotedMsg = proto.Message.create({ [msgType]: quotedMsg[msgType] });
        const quotedContent = quotedMsg[msgType];
        if (typeof quotedContent === 'object' && quotedContent && 'contextInfo' in quotedContent) {
            delete quotedContent.contextInfo;
        }
        const contextInfo = ('contextInfo' in innerMessage[key] && innerMessage[key]?.contextInfo) || {};
        contextInfo.participant = jidNormalizedUser(participant);
        contextInfo.stanzaId = quoted.key.id;
        contextInfo.quotedMessage = quotedMsg;
        // if a participant is quoted, then it must be a group
        // hence, remoteJid of group must also be entered
        if (!isNewsletter && jid !== quoted.key.remoteJid) {
            contextInfo.remoteJid = quoted.key.remoteJid;
        }
        if (contextInfo && innerMessage[key]) {
            /* @ts-ignore */
            innerMessage[key].contextInfo = contextInfo;
        }
    }
    if (
    // if we want to send a disappearing message
    !!options?.ephemeralExpiration &&
        // and it's not a protocol message -- delete, toggle disappear message
        key !== 'protocolMessage' &&
        // already not converted to disappearing message
        key !== 'ephemeralMessage' &&
        // newsletters don't support ephemeral messages
        !isNewsletter) {
        /* @ts-ignore */
        innerMessage[key].contextInfo = {
            ...(innerMessage[key].contextInfo || {}),
            expiration: options.ephemeralExpiration || WA_DEFAULT_EPHEMERAL
            //ephemeralSettingTimestamp: options.ephemeralOptions.eph_setting_ts?.toString()
        };
    }

    if (messageContextInfo?.messageSecret && (isPnUser(jid) || isLidUser(jid))) {
        messageContextInfo.deviceListMetadata = {
            recipientKeyHash: randomBytes(10),
            recipientTimestamp: unixTimestampSeconds()
        };
        messageContextInfo.deviceListMetadataVersion = 2;
    }
    message = WAProto.Message.create(message);
    const messageJSON = {
        key: {
            remoteJid: jid,
            fromMe: true,
            id: options?.messageId || generateMessageIDV2()
        },
        message: message,
        messageTimestamp: timestamp,
        messageStubParameters: [],
        participant: isJidGroup(jid) || isJidStatusBroadcast(jid) ? userJid : undefined, // TODO: Add support for LIDs
        status: WAMessageStatus.PENDING
    };
    return WAProto.WebMessageInfo.fromObject(messageJSON);
};
export const generateWAMessage = async (jid, content, options) => {
    // ensure msg ID is with every log
    options.logger = options?.logger?.child({ msgId: options.messageId });
    // Pass jid in the options to generateWAMessageContent
    if (jid) {
        options.jid = jid;
    }
    return generateWAMessageFromContent(jid, await generateWAMessageContent(content, options), options);
};
/** Get the key to access the true type of content */
export const getContentType = (content) => {
    if (content) {
        const keys = Object.keys(content);
        const key = keys.find(k => (k === 'conversation' || k.includes('Message')) && k !== 'senderKeyDistributionMessage');
        return key;
    }
};
/**
 * Normalizes ephemeral, view once messages to regular message content
 * Eg. image messages in ephemeral messages, in view once messages etc.
 * @param content
 * @returns
 */
export const normalizeMessageContent = (content) => {
    if (!content) {
        return undefined;
    }
    // set max iterations to prevent an infinite loop
    for (let i = 0; i < 5; i++) {
        const inner = getFutureProofMessage(content);
        if (!inner) {
            break;
        }
        content = inner.message;
    }
    return content;

    function getFutureProofMessage(message) {
        return (message?.associatedChildMessage ||
            message?.botForwardedMessage ||
            message?.botInvokeMessage ||
            message?.botPlatformRegistrationSuccessMessage ||
            message?.botTaskMessage ||
            message?.documentWithCaptionMessage ||
            message?.editedMessage ||
            message?.ephemeralMessage ||
            message?.eventCoverImage ||
            message?.groupMentionedMessage ||
            message?.groupStatusMentionMessage ||
            message?.groupStatusMessage ||
            message?.groupStatusMessageV2 ||
            message?.limitSharingMessage ||
            message?.lottieStickerMessage ||
            message?.newsletterAdminProfileMessage ||
            message?.newsletterAdminProfileMessageV2 ||
            message?.newsletterAdminProfileStatusMessage ||
            message?.pollCreationMessageV4 ||
            message?.pollCreationOptionImageMessage ||
            message?.questionMessage ||
            message?.questionReplyMessage ||
            message?.spoilerMessage ||
            message?.statusAddYours ||
            message?.statusMentionMessage ||
            message?.viewOnceMessage ||
            message?.viewOnceMessageV2 ||
            message?.viewOnceMessageV2Extension);
    }
};
/**
 * Extract the true message content from a message
 * Eg. extracts the inner message from a disappearing message/view once message
 */
export const extractMessageContent = (content) => {
    const extractFromTemplateMessage = (msg) => {
        if (msg.imageMessage) {
            return { imageMessage: msg.imageMessage };
        }
        else if (msg.documentMessage) {
            return { documentMessage: msg.documentMessage };
        }
        else if (msg.videoMessage) {
            return { videoMessage: msg.videoMessage };
        }
        else if (msg.locationMessage) {
            return { locationMessage: msg.locationMessage };
        }
        else {
            return {
                conversation: 'contentText' in msg ? msg.contentText : 'hydratedContentText' in msg ? msg.hydratedContentText : ''
            };
        }
    };
    content = normalizeMessageContent(content);
    if (content?.buttonsMessage) {
        return extractFromTemplateMessage(content.buttonsMessage);
    }
    if (content?.templateMessage?.hydratedFourRowTemplate) {
        return extractFromTemplateMessage(content?.templateMessage?.hydratedFourRowTemplate);
    }
    if (content?.templateMessage?.hydratedTemplate) {
        return extractFromTemplateMessage(content?.templateMessage?.hydratedTemplate);
    }
    if (content?.templateMessage?.fourRowTemplate) {
        return extractFromTemplateMessage(content?.templateMessage?.fourRowTemplate);
    }
    return content;
};
/**
 * Returns the device predicted by message ID
 */
export const getDevice = (id) => /^3A.{18}$/.test(id)
    ? 'ios'
    : /^3E.{20}$/.test(id)
        ? 'web'
        : /^(.{21}|.{32})$/.test(id)
            ? 'android'
            : /^(3F|.{18}$)/.test(id)
                ? 'desktop'
                : 'unknown';
/** Upserts a receipt in the message */
export const updateMessageWithReceipt = (msg, receipt) => {
    msg.userReceipt = msg.userReceipt || [];
    const recp = msg.userReceipt.find(m => m.userJid === receipt.userJid);
    if (recp) {
        Object.assign(recp, receipt);
    }
    else {
        msg.userReceipt.push(receipt);
    }
};
/** Update the message with a new reaction */
export const updateMessageWithReaction = (msg, reaction) => {
    const authorID = getKeyAuthor(reaction.key);
    const reactions = (msg.reactions || []).filter(r => getKeyAuthor(r.key) !== authorID);
    reaction.text = reaction.text || '';
    reactions.push(reaction);
    msg.reactions = reactions;
};
/** Update the message with a new poll update */
export const updateMessageWithPollUpdate = (msg, update) => {
    const authorID = getKeyAuthor(update.pollUpdateMessageKey);
    const reactions = (msg.pollUpdates || []).filter(r => getKeyAuthor(r.pollUpdateMessageKey) !== authorID);
    if (update.vote?.selectedOptions?.length) {
        reactions.push(update);
    }
    msg.pollUpdates = reactions;
};
/** Update the message with a new event response */
export const updateMessageWithEventResponse = (msg, update) => {
    const authorID = getKeyAuthor(update.eventResponseMessageKey);
    const responses = (msg.eventResponses || []).filter(r => getKeyAuthor(r.eventResponseMessageKey) !== authorID);
    responses.push(update);
    msg.eventResponses = responses;
};
/**
 * Aggregates all poll updates in a poll.
 * @param msg the poll creation message
 * @param meId your jid
 * @returns A list of options & their voters
 */
export function getAggregateVotesInPollMessage({ message, pollUpdates }, meId) {
    const opts = message?.pollCreationMessage?.options ||
        message?.pollCreationMessageV2?.options ||
        message?.pollCreationMessageV3?.options ||
        [];
    const voteHashMap = opts.reduce((acc, opt) => {
        const hash = sha256(Buffer.from(opt.optionName || '')).toString();
        acc[hash] = {
            name: opt.optionName || '',
            voters: []
        };
        return acc;
    }, {});
    for (const update of pollUpdates || []) {
        const { vote } = update;
        if (!vote) {
            continue;
        }
        for (const option of vote.selectedOptions || []) {
            const hash = option.toString();
            let data = voteHashMap[hash];
            if (!data) {
                voteHashMap[hash] = {
                    name: 'Unknown',
                    voters: []
                };
                data = voteHashMap[hash];
            }
            voteHashMap[hash].voters.push(getKeyAuthor(update.pollUpdateMessageKey, meId));
        }
    }
    return Object.values(voteHashMap);
}
/**
 * Aggregates all event responses in an event message.
 * @param msg the event creation message
 * @param meId your jid
 * @returns A list of response types & their responders
 */
export function getAggregateResponsesInEventMessage({ eventResponses }, meId) {
    const responseTypes = ['GOING', 'NOT_GOING', 'MAYBE'];
    const responseMap = {};
    for (const type of responseTypes) {
        responseMap[type] = {
            response: type,
            responders: []
        };
    }
    for (const update of eventResponses || []) {
        const responseType = update.eventResponse || 'UNKNOWN';
        if (responseType !== 'UNKNOWN' && responseMap[responseType]) {
            responseMap[responseType].responders.push(getKeyAuthor(update.eventResponseMessageKey, meId));
        }
    }
    return Object.values(responseMap);
}
/** Given a list of message keys, aggregates them by chat & sender. Useful for sending read receipts in bulk */
export const aggregateMessageKeysNotFromMe = (keys) => {
    const keyMap = {};
    for (const { remoteJid, id, participant, fromMe } of keys) {
        if (!fromMe) {
            const uqKey = `${remoteJid}:${participant || ''}`;
            if (!keyMap[uqKey]) {
                keyMap[uqKey] = {
                    jid: remoteJid,
                    participant: participant,
                    messageIds: []
                };
            }
            keyMap[uqKey].messageIds.push(id);
        }
    }
    return Object.values(keyMap);
};
const REUPLOAD_REQUIRED_STATUS = [410, 404];
/**
 * Downloads the given message. Throws an error if it's not a media message
 */
export const downloadMediaMessage = async (message, type, options, ctx) => {
    const result = await downloadMsg().catch(async (error) => {
        if (ctx &&
            typeof error?.status === 'number' && // treat errors with status as HTTP failures requiring reupload
            REUPLOAD_REQUIRED_STATUS.includes(error.status)) {
            ctx.logger.info({ key: message.key }, 'sending reupload media request...');
            // request reupload
            message = await ctx.reuploadRequest(message);
            const result = await downloadMsg();
            return result;
        }
        throw error;
    });
    return result;
    async function downloadMsg() {
        const mContent = extractMessageContent(message.message);
        if (!mContent) {
            throw new Boom('No message present', { statusCode: 400, data: message });
        }
        const contentType = getContentType(mContent);
        let mediaType = contentType?.replace('Message', '');
        const media = mContent[contentType];
        if (!media || typeof media !== 'object' || (!('url' in media) && !('thumbnailDirectPath' in media))) {
            throw new Boom(`"${contentType}" message is not a media message`);
        }
        let download;
        if ('thumbnailDirectPath' in media && !('url' in media)) {
            download = {
                directPath: media.thumbnailDirectPath,
                mediaKey: media.mediaKey
            };
            mediaType = 'thumbnail-link';
        }
        else {
            download = media;
        }
        const stream = await downloadContentFromMessage(download, mediaType, options);
        if (type === 'buffer') {
            const bufferArray = [];
            for await (const chunk of stream) {
                bufferArray.push(chunk);
            }
            return Buffer.concat(bufferArray);
        }
        return stream;
    }
};
/** Checks whether the given message is a media message; if it is returns the inner content */
export const assertMediaContent = (content) => {
    content = extractMessageContent(content);
    const mediaContent = content?.documentMessage ||
        content?.imageMessage ||
        content?.videoMessage ||
        content?.audioMessage ||
        content?.stickerMessage;
    if (!mediaContent) {
        throw new Boom('given message is not a media message', { statusCode: 400, data: content });
    }
    return mediaContent;
};
/**
 * Checks if a WebP buffer is animated by looking for VP8X chunk with animation flag
 * or ANIM/ANMF chunks
 */
const isAnimatedWebP = (buffer) => {
    // WebP must start with RIFF....WEBP
    if (buffer.length < 12 ||
        buffer[0] !== 0x52 ||
        buffer[1] !== 0x49 ||
        buffer[2] !== 0x46 ||
        buffer[3] !== 0x46 ||
        buffer[8] !== 0x57 ||
        buffer[9] !== 0x45 ||
        buffer[10] !== 0x42 ||
        buffer[11] !== 0x50) {
        return false;
    }
    ;
    // Parse chunks starting after RIFF header (12 bytes)
    let offset = 12;
    while (offset < buffer.length - 8) {
        const chunkFourCC = buffer.toString('ascii', offset, offset + 4);
        const chunkSize = buffer.readUInt32LE(offset + 4);
        if (chunkFourCC === 'VP8X') {
            // VP8X extended header, check animation flag (bit 1 at offset+8)
            const flagsOffset = offset + 8;
            if (flagsOffset < buffer.length) {
                const flags = buffer[flagsOffset];
                if (flags & 0x02) {
                    return true;
                }
                ;
            }
            ;
        }
        else if (chunkFourCC === 'ANIM' || chunkFourCC === 'ANMF') {
            // ANIM or ANMF chunks indicate animation
            return true;
        }
        ;
        // Move to next chunk (chunk size + 8 bytes header, padded to even)
        offset += 8 + chunkSize + (chunkSize % 2);
    }
    ;
    return false;
};
/**
 * Checks if a buffer is a WebP file
 */
const isWebPBuffer = (buffer) => {
    return (buffer.length >= 12 &&
        buffer[0] === 0x52 &&
        buffer[1] === 0x49 &&
        buffer[2] === 0x46 &&
        buffer[3] === 0x46 &&
        buffer[8] === 0x57 &&
        buffer[9] === 0x45 &&
        buffer[10] === 0x42 &&
        buffer[11] === 0x50);
};
/**
 * Determines whether a message should include a Biz Binary Node.
 * A Biz Binary Node is added only for interactive messages
 * such as buttons or other supported interactive types.
 */
export const shouldIncludeBizBinaryNode = (message) => !!(message.buttonsMessage ||
    message.listMessage ||
    message.templateMessage ||
    (message.interactiveMessage &&
        message.interactiveMessage.nativeFlowMessage));
//# sourceMappingURL=messages.js.map
