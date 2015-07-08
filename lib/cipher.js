var _ = require('lodash');
var crypto = require('crypto');

var ALGORYTHM = 'aes-256-cbc';
var PASSWORD = process.env.MESSAGE_CRYPTO_PASSWORD;

exports.encrypt = encrypt;
exports.decrypt = decrypt;
exports.encryptIV = encryptIV;
exports.decryptIV = decryptIV;
exports.encryptMessageContent = encryptMessageContent;
exports.decryptMessageContent = decryptMessageContent;

function encrypt(rawData) {
    if (!_.isString(rawData)) {
        throw new Error('RabbitMQ message cipher.encrypt() accepts only a string as a parameter.');
    }
    var data = encodeURIComponent(rawData);
    if (!PASSWORD) {
        return data;
    }
    var cipher = crypto.createCipher(ALGORYTHM, PASSWORD);
    var encData = cipher.update(data, 'binary', 'base64');
    encData += cipher.final('base64');
    return encData;
}

function encryptIV(rawData, iv) {
    var encodeKey = crypto.createHash('sha256').update(PASSWORD, 'utf-8').digest();
    var cipher = crypto.createCipheriv('aes-256-cbc', encodeKey, iv);
    return cipher.update(rawData, 'binary', 'base64') + cipher.final('base64');
}

function decryptIV(encData, iv) {
    var decodeKey = crypto.createHash('sha256').update(PASSWORD, 'utf-8').digest();
    var cipher = crypto.createDecipheriv('aes-256-cbc', decodeKey, iv);
    return cipher.update(encData, 'base64', 'binary') + cipher.final('binary');
}

function decrypt(encData) {
    if (!_.isString(encData)) {
        throw new Error('RabbitMQ message cipher.decrypt() accepts only a string as a parameter.');
    }
    if (!PASSWORD) {
        return decodeURIComponent(encData);
    }
    var decipher = crypto.createDecipher(ALGORYTHM, PASSWORD);
    var decData = decipher.update(encData, 'base64', 'binary');
    decData += decipher.final('binary');
    return decodeURIComponent(decData);
}

function encryptMessageContent(messagePayload) {
    return encrypt(JSON.stringify(messagePayload));
}

function decryptMessageContent(messagePayload) {
    if (!messagePayload || messagePayload.toString().length == 0) {
        return null;
    }
    try {
        return JSON.parse(decrypt(messagePayload.toString()));
    } catch (err) {
        throw Error('Failed to decrypt message: ' + err.message);
    }
}