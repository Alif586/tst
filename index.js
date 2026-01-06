/* ========= Number Bot (ULTRA OPTIMIZED: 50K+ Users) =========== */

const TelegramBot = require('node-telegram-bot-api');
const fs = require('fs');
const path = require('path');
const XLSX = require('xlsx');
const request = require('request');
const countryEmoji = require('country-emoji');
const mongoose = require('mongoose');

// ===============================================
// ✅ কনফিগারেশন
// ===============================================
const BOT_TOKEN = '7142079092:AAGRrSPa3su8iuGG4r9n5x1LZOwsFPaFoQ0';
const AUTHORIZED_BOT_ID = 8320415016;
const OTP_GROUP_URL = "https://t.me/RX_ALL_OTP_GROUP";

const GITHUB_USERNAME = "yeiemwpshyienga";
const GITHUB_REPO_NAME = "smszone";
const GITHUB_FILE_PATH = "users.json";

const NUMBER_DB_URI = "mongodb+srv://rakibkhan625162_db_user:sabbir123@number.qdza7vx.mongodb.net/Number?retryWrites=true&w=majority";
const USER_DB_URI = "mongodb+srv://sabbirrehman905_db_user:sabbir123@userjson.f0vppgx.mongodb.net/UserDB?appName=Userjson";

const USER_LIST_FILE = 'users.json';

const REQUIRED_CHANNELS = [
    { id: -1003009541400, url: "https://t.me/techzonebd61" },
    { id: -1002383249427, url: "https://t.me/+SGQCjEiIu_ZlY2Vl" },
    { id: -1002245233356, url: "https://t.me/+9rmkIBmkZ3M0ZWVl" },
];

const ADMIN_IDS = [1817149496, 6135656510, 7802680600, 6006322754];
const SUPPORT_USERNAME = "unknown15x";
const COOLDOWN_TIME = 2;

// ===============================================
// 🆕 মেসেজ টেমপ্লেট
// ===============================================
const NEW_FOOTER_QUOTE = "<blockquote>📢 এই নাম্বারে 𝐎𝐓𝐏 পাঠানোর পর বটেই 𝐎𝐓𝐏 পাবেন। যদি বটে 𝐎𝐓𝐏 না আসে তাহলে 𝐎𝐓𝐏 গ্রুপে 𝐎𝐓𝐏 পাবেন।🌸\n📸┈┈┈┈𝐒𝐭𝐚𝐲 𝐖𝐢𝐭𝐡 𝐔𝐬┈┈┈┈📸</blockquote>";

const ASSIGNMENT_MESSAGE_TEMPLATE = (flag, country_name, number, action_text, footer) => `\
${flag} <b>${country_name}</b> 𝐅𝐫𝐞𝐬𝐡 𝐍𝐮𝐦𝐛𝐞𝐫 <b>${action_text}:</b>

📱 𝐘𝐨𝐮𝐫 𝐍𝐮𝐦𝐛𝐞𝐫:
┗┈» <code>${number}</code> «┈┛

┏━━━━━━━━━━━━━━┓ 
  ⏳ 𝐖𝐚𝐢𝐭𝐢𝐧𝐠 𝐅𝐨𝐫 𝐎𝐓𝐏...  
┗━━━━━━━━━━━━━━┛
━━━━━━━━━━━━━━━━
${footer}
`;

// ===============================================
// 🔥 ULTRA OPTIMIZED DATABASE CONNECTION
// ===============================================
const dbOptions = {
    serverSelectionTimeoutMS: 30000,
    socketTimeoutMS: 45000,
    family: 4,
    maxPoolSize: 300,        // 🔥 150 -> 300 বৃদ্ধি
    minPoolSize: 50,         // 🔥 10 -> 50 বৃদ্ধি
    connectTimeoutMS: 10000,
    maxIdleTimeMS: 30000,
    compressors: ['zlib'],   // 🔥 Compression যোগ করা
    retryWrites: true,
    retryReads: true,
    readPreference: 'secondaryPreferred' // 🔥 Load balancing
};

const numberConn = mongoose.createConnection(NUMBER_DB_URI, dbOptions);
numberConn.on('connected', () => console.log("✅ Number DB Connected (Optimized Pool)!"));

const userConn = mongoose.createConnection(USER_DB_URI, dbOptions);
userConn.on('connected', () => {
    console.log("✅ User & Config DB Connected (Optimized Pool)!");
    syncSystem();
});

// --- Schemas with Indexes ---
const numberSchema = new mongoose.Schema({
    number: { type: String, unique: true, required: true },
    country: { type: String, required: true, index: true }, // 🔥 Index যোগ
    flag: { type: String, default: "🌍" },
    status: { type: String, enum: ['Available', 'Used', 'Used_History'], default: 'Available', index: true }, // 🔥 Index
    assigned_to: { type: Number, default: null, index: true }, // 🔥 Index
    assigned_at: { type: Date, default: null },
    created_at: { type: Date, default: Date.now }
});

// 🔥 Compound Index for faster queries
numberSchema.index({ country: 1, status: 1 });
numberSchema.index({ status: 1, assigned_at: 1 });
numberSchema.index({ assigned_to: 1, status: 1 });

const NumberModel = numberConn.model('Number', numberSchema);

const userSchema = new mongoose.Schema({
    userId: { type: Number, unique: true, required: true },
    joined_at: { type: Date, default: Date.now }
});
const UserModel = userConn.model('User', userSchema);

const configSchema = new mongoose.Schema({
    key: { type: String, unique: true, required: true },
    value: { type: String, required: true }
});
const ConfigModel = userConn.model('Config', configSchema);

// --- ভেরিয়েবল ---
const bot = new TelegramBot(BOT_TOKEN, { 
    polling: { 
        interval: 50,        // 🔥 100 -> 50 (দ্রুত response)
        autoStart: true,
        params: {
            timeout: 10
        }
    } 
});

// ===============================================
// 🛡️ ERROR HANDLING
// ===============================================
bot.on('polling_error', (error) => {
    console.log(`[Polling Error] ${error.code}: ${error.message}`);
});

process.on('unhandledRejection', (reason, promise) => {
    if (reason && reason.response && reason.response.statusCode === 403) {
        console.log("⚠️ User blocked the bot. Message failed (Ignored to prevent crash).");
    } else {
        console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    }
});

process.on('uncaughtException', (error) => {
    console.error('Uncaught Exception:', error);
});

// 🔥 MEMORY OPTIMIZATION
if (global.gc) {
    setInterval(() => {
        global.gc();
    }, 30000); // 30 সেকেন্ডে garbage collection
}

let bot_users = new Set();
let admin_country_temp_data = {};
let last_action_time = {};
let user_details_cache = {};
let country_data_cache = {};
let user_states = {};
let admin_file_buffer = {};
let last_change_time = {};

// 🔥 নতুন: অ্যাসাইনমেন্ট লক সিস্টেম (প্রতি দেশের জন্য)
let country_assignment_locks = {};

// 🔥 FIX: Country Index Mapping
let countryToIndex = {};
let indexToCountry = {};

// 🔥 IN-MEMORY CACHE for faster queries
let membershipCache = new Map(); // userId -> {timestamp, isMember}
const MEMBERSHIP_CACHE_TTL = 60000; // 1 মিনিট

// ===============================================
// 🆕 GLOBAL VARIABLES FOR ADD NOTIFICATION
// ===============================================
let bot_username = "";
let add_session_data = [];
let last_add_timestamp = 0;
let last_channel_msg_ids = {};

bot.getMe().then((me) => {
    bot_username = me.username;
    console.log(`✅ Bot Username Detected: @${bot_username}`);
});

// ===============================================
// 🔥 REQUEST QUEUE SYSTEM (High Concurrency)
// ===============================================
class RequestQueue {
    constructor(concurrency = 100) { // 🔥 100 concurrent requests
        this.queue = [];
        this.running = 0;
        this.concurrency = concurrency;
    }

    async add(fn) {
        return new Promise((resolve, reject) => {
            this.queue.push({ fn, resolve, reject });
            this.process();
        });
    }

    async process() {
        if (this.running >= this.concurrency || this.queue.length === 0) return;

        this.running++;
        const { fn, resolve, reject } = this.queue.shift();

        try {
            const result = await fn();
            resolve(result);
        } catch (error) {
            reject(error);
        } finally {
            this.running--;
            this.process();
        }
    }
}

const requestQueue = new RequestQueue(100);

// ===============================================
// 📧 FIX: Safe edit message wrapper
// ===============================================
async function safeEditMessage(chatId, msgId, text, options = {}) {
    try {
        await bot.editMessageText(text, {
            chat_id: chatId,
            message_id: msgId,
            ...options
        });
    } catch (error) {
        if (error.response && error.response.body) {
            const errDesc = error.response.body.description;
            if (!errDesc.includes('message is not modified')) {
                console.error('Edit message error:', errDesc);
            }
        }
    }
}

// ===============================================
// 🕐 AUTO DELETE CLAIMED NUMBERS AFTER 2 HOURS
// ===============================================
async function autoDeleteExpiredNumbers() {
    try {
        const twoHoursAgo = new Date(Date.now() - 2 * 60 * 60 * 1000);

        const result = await NumberModel.deleteMany({
            status: 'Used',
            assigned_at: { $lt: twoHoursAgo }
        });

        if (result.deletedCount > 0) {
            console.log(`🗑️ Auto-deleted ${result.deletedCount} expired claimed numbers (2+ hours old)`);
            await rebuildCountryCache();
        }
    } catch (error) {
        console.error("Auto-delete error:", error);
    }
}

setInterval(autoDeleteExpiredNumbers, 10 * 60 * 1000);
setTimeout(autoDeleteExpiredNumbers, 5000);

// ===============================================
// 📄 GITHUB & DB SYNC LOGIC
// ===============================================
async function getGitHubToken() {
    const conf = await ConfigModel.findOne({ key: "github_token" }).lean(); // 🔥 .lean() যোগ
    return conf ? conf.value : null;
}

async function fetchGithubUsers(token) {
    if (!token) return null;
    const url = `https://api.github.com/repos/${GITHUB_USERNAME}/${GITHUB_REPO_NAME}/contents/${GITHUB_FILE_PATH}`;

    return new Promise((resolve) => {
        request({
            url: url,
            headers: { 'User-Agent': 'NodeBot', 'Authorization': `token ${token}` }
        }, (err, res, body) => {
            if (err || res.statusCode !== 200) {
                console.log("GitHub Fetch Error or 404 (File might not exist yet).");
                resolve(null);
            } else {
                try {
                    const json = JSON.parse(body);
                    const content = Buffer.from(json.content, 'base64').toString('utf8');
                    resolve({ content: JSON.parse(content), sha: json.sha });
                } catch (e) {
                    resolve(null);
                }
            }
        });
    });
}

async function uploadToGithub(usersArray, token, sha = null) {
    if (!token) return;
    const url = `https://api.github.com/repos/${GITHUB_USERNAME}/${GITHUB_REPO_NAME}/contents/${GITHUB_FILE_PATH}`;
    const contentEncoded = Buffer.from(JSON.stringify(usersArray, null, 2)).toString('base64');

    const bodyData = {
        message: "Update users.json via Bot",
        content: contentEncoded,
        sha: sha
    };

    request({
        url: url,
        method: 'PUT',
        headers: {
            'User-Agent': 'NodeBot',
            'Authorization': `token ${token}`,
            'Accept': 'application/vnd.github.v3+json'
        },
        json: true,
        body: bodyData
    }, (err, res, body) => {
        if (err) console.error("GitHub Upload Error:", err);
        else console.log("✅ GitHub Updated Successfully.");
    });
}

async function syncSystem() {
    console.log("🔄 Starting Sync System...");
    const token = await getGitHubToken();

    const mongoUsersDocs = await UserModel.find({}).lean(); // 🔥 .lean() যোগ
    const mongoUserIds = new Set(mongoUsersDocs.map(u => u.userId));

    let githubData = await fetchGithubUsers(token);
    let githubUserIds = new Set();
    if (githubData && Array.isArray(githubData.content)) {
        githubUserIds = new Set(githubData.content);
    }

    const allUsers = new Set([...mongoUserIds, ...githubUserIds, ...bot_users]);
    ADMIN_IDS.forEach(id => allUsers.add(id));

    bot_users = allUsers;

    const newForMongo = [];
    allUsers.forEach(uid => {
        if (!mongoUserIds.has(uid)) {
            newForMongo.push({ userId: uid });
        }
    });

    if (newForMongo.length > 0) {
        await UserModel.insertMany(newForMongo, { ordered: false }).catch(() => {});
        console.log(`🔥 Added ${newForMongo.length} users to MongoDB from Sync.`);
    }

    if (token) {
        const finalArray = Array.from(allUsers);
        if (finalArray.length !== githubUserIds.size || newForMongo.length > 0) {
            await uploadToGithub(finalArray, token, githubData ? githubData.sha : null);
        }
    }

    try {
        fs.writeFileSync(USER_LIST_FILE, JSON.stringify(Array.from(allUsers), null, 4));
    } catch (e) {}

    console.log(`✅ Sync Complete. Total Users: ${allUsers.size}`);
}

async function addUserToLocalDb(userId) {
    if (!bot_users.has(userId)) {
        bot_users.add(userId);

        // 🔥 Queue এ যোগ করে async save
        requestQueue.add(async () => {
            try {
                await new UserModel({ userId: userId }).save();
            } catch (e) {}
        });

        try {
            fs.writeFileSync(USER_LIST_FILE, JSON.stringify(Array.from(bot_users), null, 4));
        } catch (e) {}

        // 🔥 GitHub sync background এ চলবে
        requestQueue.add(async () => {
            const token = await getGitHubToken();
            if (token) {
                const ghData = await fetchGithubUsers(token);
                await uploadToGithub(Array.from(bot_users), token, ghData ? ghData.sha : null);
            }
        });
    }
}

// ===============================================
// ⚙️ Helper Functions (OPTIMIZED)
// ===============================================
async function rebuildCountryCache() {
    try {
        const result = await NumberModel.aggregate([
            {
                $group: {
                    _id: "$country",
                    flag: { $first: "$flag" },
                    total: { $sum: 1 },
                    available: { $sum: { $cond: [{ $eq: ["$status", "Available"] }, 1, 0] } }
                }
            }
        ]).allowDiskUse(true);

        country_data_cache = {};
        countryToIndex = {};
        indexToCountry = {};

        let idx = 0;
        result.forEach(r => {
            country_data_cache[r._id] = { flag: r.flag, available: r.available, total: r.total };
            countryToIndex[r._id] = idx;
            indexToCountry[idx] = r._id;
            idx++;
        });
    } catch (e) {
        console.error("Cache rebuild error:", e);
    }
}

function isAdmin(userId) {
    return ADMIN_IDS.includes(userId);
}

// 🔥 OPTIMIZED: Membership check with caching
async function isUserMember(userId) {
    if (isAdmin(userId)) return true;

    // Cache check
    const cached = membershipCache.get(userId);
    if (cached && (Date.now() - cached.timestamp < MEMBERSHIP_CACHE_TTL)) {
        return cached.isMember;
    }

    const validStatuses = ['member', 'administrator', 'creator'];

    const checkPromises = REQUIRED_CHANNELS.map(channel => 
        bot.getChatMember(channel.id, userId)
            .then(member => validStatuses.includes(member.status))
            .catch(() => false)
    );

    const results = await Promise.all(checkPromises);
    const isMember = results.every(result => result === true);

    // Cache store
    membershipCache.set(userId, { timestamp: Date.now(), isMember });

    return isMember;
}

function getAvailableCountriesData() {
    const countryData = {};
    for (const [country, data] of Object.entries(country_data_cache)) {
        if (data.available > 0) countryData[country] = { flag: data.flag, count: data.available };
    }
    return countryData;
}

function getAllCountryList() {
    const countryData = {};
    for (const [country, data] of Object.entries(country_data_cache)) {
        countryData[country] = { flag: data.flag, count: data.total };
    }
    return countryData;
}

function isUserAllowedAction(userId) {
    if (isAdmin(userId)) return { allowed: true, remaining: 0 };
    const currentTime = Date.now() / 1000;
    if (last_action_time[userId] && (currentTime - last_action_time[userId]) < COOLDOWN_TIME) {
        const remaining = (COOLDOWN_TIME - (currentTime - last_action_time[userId])).toFixed(1);
        return { allowed: false, remaining: remaining };
    }
    last_action_time[userId] = currentTime;
    return { allowed: true, remaining: 0 };
}

// ===============================================
// ⌨️ Keyboards
// ===============================================
function getMainMenuKeyboard(userId) {
    const keyboard = [
        [{ text: "📲 Get Number" }, { text: "🌍 Available Country" }],
        [{ text: "✅ Active Number" }, { text: "☎️ Support" }]
    ];
    if (isAdmin(userId)) keyboard.push([{ text: "🔐 Admin Menu" }]);
    return { keyboard: keyboard, resize_keyboard: true };
}

function getAdminMenuKeyboard(inSession = false) {
    if (inSession) return { keyboard: [[{ text: "🛑 Stop" }]], resize_keyboard: true };
    return {
        keyboard: [
            [{ text: "➕ ADD" }, { text: "📢 Broadcast" }],
            [{ text: "📊 Status" }, { text: "🔑 Ass Token" }],
            [{ text: "🗑️ Delete" }, { text: "🔄 Restart" }],
            [{ text: "➡️ Main Menu" }]
        ],
        resize_keyboard: true
    };
}

function getNumberControlKeyboard() {
    return {
        inline_keyboard: [
            [{ text: "View OTP 📩", url: OTP_GROUP_URL }],
            [
                { text: "🔄 Change Number", callback_data: `change_number_req` },
                { text: "🌍 Change Country", callback_data: 'change_country_start' }
            ]
        ]
    };
}

function getDeleteCountryKeyboard() {
    const allCountries = getAllCountryList();
    const buttons = [];
    const keys = Object.keys(allCountries).sort();
    for (let i = 0; i < keys.length; i += 2) {
        const row = [];
        const country1 = keys[i];
        row.push({ 
            text: `${allCountries[country1].flag} ${country1}`, 
            callback_data: `sdc:${countryToIndex[country1]}`
        });
        if (i + 1 < keys.length) {
            const country2 = keys[i + 1];
            row.push({ 
                text: `${allCountries[country2].flag} ${country2}`, 
                callback_data: `sdc:${countryToIndex[country2]}`
            });
        }
        buttons.push(row);
    }
    buttons.push([{ text: "❌ Cancel", callback_data: 'cancel_delete' }]);
    return { inline_keyboard: buttons };
}

function getVerificationMarkup() {
    const buttons = REQUIRED_CHANNELS.map((ch, i) => [{ text: `Join Channel ${i + 1}`, url: ch.url }]);
    buttons.push([{ text: "✅ Verify", callback_data: 'verify_check' }]);
    return { inline_keyboard: buttons };
}

async function sendVerificationPrompt(userId, messageId = null) {
    const text = `⚠️ **Access Denied!**\nPlease join our channels to use the bot.`;
    const markup = getVerificationMarkup();
    if (messageId) {
        try { await safeEditMessage(userId, messageId, text, { parse_mode: 'Markdown', reply_markup: markup }); } catch {}
    } else {
        try {
            await bot.sendMessage(userId, text, { parse_mode: 'Markdown', reply_markup: markup });
        } catch (e) {}
    }
}

// ===============================================
// 📩 COMMAND HANDLER (OPTIMIZED)
// ===============================================
bot.on('message', async (msg) => {
    if (!msg.from) return;
    const chatId = msg.chat.id;
    const userId = msg.from.id;
    const text = msg.text;

    // 🔥 Queue এ add user করা
    addUserToLocalDb(userId);

    if (!isAdmin(userId)) {
        if (!(await isUserMember(userId))) {
            sendVerificationPrompt(userId);
            return;
        }
    }

    if (user_states[userId]) {
        if (text === '🛑 Stop' || text === 'stop') {
            delete user_states[userId];
            delete admin_file_buffer[userId];
            bot.sendMessage(chatId, "✅ Action cancelled.", { reply_markup: getAdminMenuKeyboard() });
            return;
        }

        // 🔄 RESTART PASSWORD CHECK
        if (user_states[userId] === 'AWAITING_PASS_FOR_RST') {
            if (text === 'sms') {
                delete user_states[userId];
                
                const countdownMsg = await bot.sendMessage(
                    chatId, 
                    "🔄 **Restarting Bot...**\n\n⏳ Please wait: **6** seconds\n\n⚠️ All buttons disabled!", 
                    { 
                        parse_mode: 'Markdown',
                        reply_markup: { remove_keyboard: true }
                    }
                );
                
                for (let i = 5; i >= 1; i--) {
                    await new Promise(r => setTimeout(r, 1000));
                    try {
                        await bot.editMessageText(
                            `🔄 **Restarting Bot...**\n\n⏳ Please wait: **${i}** seconds\n\n⚠️ All buttons disabled!`,
                            {
                                chat_id: chatId,
                                message_id: countdownMsg.message_id,
                                parse_mode: 'Markdown'
                            }
                        );
                    } catch (e) {}
                }
                
                try {
                    await bot.deleteMessage(chatId, countdownMsg.message_id);
                } catch (e) {}

                await bot.sendMessage(chatId, "✅ **Restart Complete!**\n\n🤖 Bot successfully updated & restarted!\n🎉 All systems operational.\n\n⤵️ **Select an option:**", {
                    parse_mode: 'Markdown',
                    reply_markup: getAdminMenuKeyboard()
                });
                
                setTimeout(() => {
                    const { exec } = require('child_process');
                    const BOT_PATH = '/home/alif/tst';
                    
                    exec(`cd ${BOT_PATH} && git reset --hard && git pull origin main && pm2 restart tst`, (error, stdout, stderr) => {
                        if (error) {
                            bot.sendMessage(chatId, `❌ **Restart Failed!**\n\n<pre>${error.message}</pre>`, { 
                                parse_mode: 'HTML',
                                reply_markup: getAdminMenuKeyboard() 
                            });
                        }
                    });
                }, 1000);
                
            } else {
                bot.sendMessage(chatId, "🚫 বাল পাকনা, এটা আপনার জন্য না 😅**\n\nএই অপশনটা শুধু বট ডেভেলপারদের জন্য। ফাইল আপডেট বট আপডেট এর জন্য ভুল করে ঢুকে পড়লে এখনই ব্যাক যান\n👉 @alifhosson", { 
                    reply_markup: getAdminMenuKeyboard() 
                });
                delete user_states[userId];
            }
            return;
        }

        // 🔑 GITHUB TOKEN PASSWORD CHECK
        if (user_states[userId] === 'AWAITING_PASS_FOR_TOKEN') {
            if (text === 'alif') {
                user_states[userId] = 'AWAITING_GITHUB_TOKEN';
                bot.sendMessage(chatId, "🔓 **Password Accepted!**\n\nPlease upload ur github Repo token:", { parse_mode: 'Markdown', reply_markup: getAdminMenuKeyboard(true) });
            } else {
                bot.sendMessage(chatId, "❌ ভুল পাসওয়ার্ড দিলে কিন্তু চলবে না চাচা! 😴\nপাসওয়ার্ড ভুলে গেলে গুগল না, সোজা আলিফ ভাইয়ের কাছে মেসেজ দেন 📩\nবেশি না—মাত্র 5$ দিলেই এটপট চেঞ্জ করে দিবে 😂💸\n👉 @alifhosson", { reply_markup: getAdminMenuKeyboard() });
                delete user_states[userId];
            }
            return;
        }

        if (user_states[userId] === 'AWAITING_GITHUB_TOKEN') {
            const newToken = text.trim();
            try {
                await ConfigModel.findOneAndUpdate(
                    { key: "github_token" },
                    { value: newToken },
                    { upsert: true, new: true }
                );

                bot.sendMessage(chatId, "✅ **GitHub Token Saved Successfully!**\nSyncing system now...", { parse_mode: 'Markdown', reply_markup: getAdminMenuKeyboard() });
                syncSystem();
            } catch (e) {
                bot.sendMessage(chatId, "❌ Database Error saving token.", { reply_markup: getAdminMenuKeyboard() });
            }
            delete user_states[userId];
            return;
        }

        if (user_states[userId] === 'ADDING_NUMBER_STEP_1') {
            if (msg.document) {
            
// ===============================================
// 📂 FILE PROCESSOR & REST OF THE CODE
// ===============================================
                admin_file_buffer[userId] = { file_id: msg.document.file_id };
                user_states[userId] = 'ADDING_NUMBER_STEP_2';
                bot.sendMessage(chatId, "📂 **File Received!**\nCountry Name:", { parse_mode: 'Markdown' });
                return;
            } else {
                bot.sendMessage(chatId, "❌ Please send Excel file.", { reply_markup: getAdminMenuKeyboard(true) });
                return;
            }
        }
        if (user_states[userId] === 'ADDING_NUMBER_STEP_2') {
            if (text) {
                processUploadedFile(userId, admin_file_buffer[userId].file_id, text.trim());
                delete user_states[userId];
                delete admin_file_buffer[userId];
                return;
            }
        }
        if (user_states[userId] === 'BROADCASTING') {
            processBroadcast(msg);
            return;
        }
    }

    if (!text) return;

    if (text === '/start') {
        bot.sendMessage(chatId, "Welcome! Choose your option:", { reply_markup: getMainMenuKeyboard(userId) });
        
    } else if (text === '/restart' && isAdmin(userId)) {
        user_states[userId] = 'AWAITING_PASS_FOR_RST';
        bot.sendMessage(chatId, "🔐 **Enter Restart Password:**", { 
            parse_mode: 'Markdown', 
            reply_markup: getAdminMenuKeyboard(true) 
        });
        
    } else if ((text === '🔐 Admin Menu' || text === '/admin') && isAdmin(userId)) {
        delete user_states[userId];
        bot.sendMessage(chatId, "🔐 **Admin Panel**", { parse_mode: 'Markdown', reply_markup: getAdminMenuKeyboard() });
        
    } else if (text === '➡️ Main Menu') {
        delete user_states[userId];
        bot.sendMessage(chatId, "Returning to Main Menu...", { reply_markup: getMainMenuKeyboard(userId) });
        
    } else if ((text === '/status' || text === '📊 Status') && isAdmin(userId)) {
        sendStatus(chatId);
        
    } else if (text === '🔑 Ass Token' && isAdmin(userId)) {
        user_states[userId] = 'AWAITING_PASS_FOR_TOKEN';
        bot.sendMessage(chatId, "🔐 **Enter Password:**", { reply_markup: getAdminMenuKeyboard(true) });
        
    } else if (text === '☎️ Support') {
        const markup = { inline_keyboard: [[{ text: "✉️ Contact Admin", url: `https://t.me/${SUPPORT_USERNAME}` }]] };
        bot.sendMessage(chatId, "☎️ Contact support:", { parse_mode: 'Markdown', reply_markup: markup });
        
    } else if (text === '🔄 Restart' && isAdmin(userId)) {
        user_states[userId] = 'AWAITING_PASS_FOR_RST';
        bot.sendMessage(chatId, "🔐 **Enter Restart Password:**", { 
            parse_mode: 'Markdown', 
            reply_markup: getAdminMenuKeyboard(true) 
        });
            
    } else if (text === '➕ ADD' && isAdmin(userId)) {
        user_states[userId] = 'ADDING_NUMBER_STEP_1';
        bot.sendMessage(chatId, "➕ **Add Number**\nSend file.", { reply_markup: getAdminMenuKeyboard(true) });
        
    } else if (text === '📢 Broadcast' && isAdmin(userId)) {
        user_states[userId] = 'BROADCASTING';
        bot.sendMessage(chatId, "📢 **Broadcast**\nSend message.", { reply_markup: getAdminMenuKeyboard(true) });
        
    } else if (text === '🗑️ Delete' && isAdmin(userId)) {
        await rebuildCountryCache();
        const allCountries = getAllCountryList();
        if (Object.keys(allCountries).length === 0) {
            bot.sendMessage(chatId, "❌ Empty DB.", { reply_markup: getAdminMenuKeyboard() });
        } else {
            bot.sendMessage(chatId, "🗑️ **Delete:**", { parse_mode: 'Markdown', reply_markup: getDeleteCountryKeyboard() });
        }
        
    } else if (text === '📲 Get Number' || text === '🌍 Available Country') {
        handleNumberSelectionStart(userId, text);
        
    } else if (text === '✅ Active Number') {
        showActiveNumber(userId);
    }
});

async function processUploadedFile(userId, fileId, inputName) {
    bot.sendMessage(userId, "⏳ **Processing (Smart Extract)...**");
    const rawName = inputName.trim();
    let flag = countryEmoji.flag(rawName) || "🌍";
    let countryName = countryEmoji.name(rawName) || rawName;

    try {
        const fileLink = await bot.getFileLink(fileId);
        request({ url: fileLink, encoding: null }, async (err, resp, buffer) => {
            if (err) { bot.sendMessage(userId, "❌ Error.", { reply_markup: getAdminMenuKeyboard() }); return; }
            try {
                const workbook = XLSX.read(buffer, { type: 'buffer' });
                const sheet = workbook.Sheets[workbook.SheetNames[0]];
                const jsonData = XLSX.utils.sheet_to_json(sheet, { header: 1 });

                let batchNumbers = [];
                let processedSet = new Set();

                jsonData.forEach(row => {
                    row.forEach(cell => {
                        if (cell) {
                            const cellText = String(cell);
                            const matches = cellText.match(/\d+/g);

                            if (matches) {
                                matches.forEach(num => {
                                    if (num.length >= 8) {
                                        if (!processedSet.has(num)) {
                                            processedSet.add(num);
                                            batchNumbers.push({
                                                number: num,
                                                country: countryName,
                                                flag: flag,
                                                status: 'Available'
                                            });
                                        }
                                    }
                                });
                            }
                        }
                    });
                });

                if (batchNumbers.length > 0) {
                    try {
                        const result = await NumberModel.insertMany(batchNumbers, { ordered: false });
                        await rebuildCountryCache();

                        const addedCount = result.length;

                        bot.sendMessage(userId, `✅ **Added Successfully!**\n📂 ${flag} ${countryName}\n📢 Count: \`${addedCount}\``, { parse_mode: 'Markdown', reply_markup: getAdminMenuKeyboard() });

                        const currentTime = Date.now();
                        const sessionDuration = 30 * 60 * 1000;

                        if (currentTime - last_add_timestamp > sessionDuration) {
                            add_session_data = [];
                        }

                        add_session_data.push({
                            flag: flag,
                            country: countryName,
                            count: addedCount
                        });

                        last_add_timestamp = currentTime;

                        let notificationMsg = `✅ Fresh Number Added!\n`;

                        add_session_data.forEach(item => {
                            notificationMsg += `🌎 ${item.country} ${item.flag}\n`;
                            notificationMsg += `☎️ Count: ${item.count} ⚡\n`;
                        });

                        notificationMsg += `🚀 Traffic High. 🔥\n`;
                        notificationMsg += `🤖 @${bot_username}`;

                        for (const channel of REQUIRED_CHANNELS) {
                            const chatID = channel.id;
                            if (last_channel_msg_ids[chatID]) {
                                try { await bot.deleteMessage(chatID, last_channel_msg_ids[chatID]); } catch (e) { console.log("Del msg fail"); }
                            }
                            try {
                                const sentMsg = await bot.sendMessage(chatID, notificationMsg);
                                last_channel_msg_ids[chatID] = sentMsg.message_id;
                            } catch (e) { console.log("Send msg fail"); }
                        }

                    } catch (e) {
                        const count = e.insertedDocs ? e.insertedDocs.length : 0;
                        await rebuildCountryCache();
                        bot.sendMessage(userId, `⚠️ **Partial Add!**\nUnique Added: \`${count}\`\n(Duplicates ignored)`, { parse_mode: 'Markdown', reply_markup: getAdminMenuKeyboard() });
                    }
                } else {
                    bot.sendMessage(userId, `❌ No valid numbers found (Minimum 8 digits required).`, { reply_markup: getAdminMenuKeyboard() });
                }
            } catch (e) { bot.sendMessage(userId, `❌ File Read Error.`, { reply_markup: getAdminMenuKeyboard() }); }
        });
    } catch (e) { bot.sendMessage(userId, `❌ Process Error.`, { reply_markup: getAdminMenuKeyboard() }); }
}

async function processBroadcast(msg) {
    const userId = msg.from.id;
    const totalUsers = bot_users.size;
    bot.sendMessage(userId, `📡 **Broadcasting to ${totalUsers}...**`, { parse_mode: 'Markdown' });

    let success = 0, fail = 0;
    const usersArray = Array.from(bot_users);

    // 🔥 Parallel broadcasting with batches
    const BATCH_SIZE = 50;
    for (let i = 0; i < usersArray.length; i += BATCH_SIZE) {
        const batch = usersArray.slice(i, i + BATCH_SIZE);
        
        await Promise.all(batch.map(async (targetId) => {
            if (ADMIN_IDS.includes(targetId)) return;
            try {
                await bot.copyMessage(targetId, msg.chat.id, msg.message_id);
                success++;
            } catch (e) { 
                fail++; 
            }
        }));
        
        await new Promise(r => setTimeout(r, 100)); // 100ms delay between batches
    }
    
    bot.sendMessage(userId, `✅ **Done!**\n🟢 Success: ${success}\n🔴 Failed: ${fail}`, { parse_mode: 'Markdown', reply_markup: getAdminMenuKeyboard() });
    delete user_states[userId];
}

async function sendStatus(chatId) {
    await rebuildCountryCache();
    
    // 🔥 Parallel queries
    const [total, avail, historyCount, mongoUsers] = await Promise.all([
        NumberModel.countDocuments({}),
        NumberModel.countDocuments({ status: 'Available' }),
        NumberModel.countDocuments({ status: 'Used_History' }),
        UserModel.countDocuments({})
    ]);
    
    const users = bot_users.size;

    const text = `🤖 **System Status**\n---\n💥 Users (Hybrid): \`${users}\`\n💾 Users (DB2): \`${mongoUsers}\`\n➡️ Numbers: \`${total}\`\n🟢 Available: \`${avail}\`\n🔴 Used: \`${total - avail}\`\n⚫ History: \`${historyCount}\``;

    bot.sendMessage(chatId, text, { parse_mode: 'Markdown', reply_markup: getAdminMenuKeyboard() });
}

// ===============================================
// 🟢 USER ACTIONS & CALLBACKS (OPTIMIZED)
// ===============================================
async function handleNumberSelectionStart(userId, text) {
    const { allowed, remaining } = isUserAllowedAction(userId);
    if (!allowed) { bot.sendMessage(userId, `Wait **${remaining}**s.`, { parse_mode: 'Markdown' }); return; }

    const currentNumber = await NumberModel.findOne({ assigned_to: userId, status: 'Used' }).lean(); // 🔥 .lean()
    if (text === '📲 Get Number' && currentNumber) {
        bot.sendMessage(userId, `❌ You have an active number:\n${currentNumber.flag} \`${currentNumber.number}\``, { parse_mode: 'Markdown', reply_markup: getNumberControlKeyboard() });
        return;
    }

    await rebuildCountryCache();
    const availData = getAvailableCountriesData();
    if (Object.keys(availData).length === 0) { bot.sendMessage(userId, "Sorry! No numbers."); return; }

    const buttons = [];
    Object.keys(availData).sort().forEach(country => {
        buttons.push([{ 
            text: `${availData[country].flag} ${country} (${availData[country].count})`, 
            callback_data: `assign_number:${countryToIndex[country]}`
        }]);
    });
    bot.sendMessage(userId, "🌍 **Select Country:**", { parse_mode: 'Markdown', reply_markup: { inline_keyboard: buttons } });
}

async function showActiveNumber(userId) {
    const data = await NumberModel.findOne({ assigned_to: userId, status: 'Used' }).lean(); // 🔥 .lean()
    if (data) {
        bot.sendMessage(userId, `✅ **Active Number**\n${data.flag} ${data.country}\n\`${data.number}\``, { parse_mode: 'Markdown', reply_markup: getNumberControlKeyboard() });
    } else {
        bot.sendMessage(userId, "❌ No active number.", { parse_mode: 'Markdown' });
    }
}


// 🔥 ULTRA OPTIMIZED CALLBACK HANDLER
bot.on('callback_query', async (call) => {
    const userId = call.from.id;
    const data = call.data;
    const msgId = call.message.message_id;
    const chatId = call.message.chat.id;

    // 🔥 Queue system দিয়ে handle করা
    requestQueue.add(async () => {
        if (data === 'verify_check') {
            if (await isUserMember(userId)) {
                await safeEditMessage(chatId, msgId, "✅ Verified!");
                bot.sendMessage(userId, "Menu:", { reply_markup: getMainMenuKeyboard(userId) });
            } else {
                bot.answerCallbackQuery(call.id, { text: "❌ Join channels!", show_alert: true });
            }
            return;
        }

        if (data === 'cancel_delete' && isAdmin(userId)) {
            await safeEditMessage(chatId, msgId, "✅ Cancelled.");
            bot.sendMessage(userId, "Menu:", { reply_markup: getAdminMenuKeyboard() });
            return;
        }

        if (data.startsWith('sdc:') && isAdmin(userId)) {
            const countryIdx = parseInt(data.split(':')[1]);
            const country = indexToCountry[countryIdx];
            admin_country_temp_data[userId] = country;
            const count = await NumberModel.countDocuments({ country: country });
            const markup = {
                inline_keyboard: [
                    [{ text: `✅ DELETE ALL (${count})`, callback_data: `cdc:${countryIdx}` }],
                    [{ text: "❌ CANCEL", callback_data: 'cancel_delete' }]
                ]
            };
            await safeEditMessage(chatId, msgId, `⚠️ Delete **${country}**?`, { parse_mode: 'Markdown', reply_markup: markup });
            return;
        }

        if (data.startsWith('cdc:') && isAdmin(userId)) {
            const countryIdx = parseInt(data.split(':')[1]);
            const country = indexToCountry[countryIdx];
            if (admin_country_temp_data[userId] !== country) return;

            try {
                await bot.deleteMessage(chatId, msgId);
            } catch (e) {
                console.log("Message delete failed or already deleted");
            }

            bot.sendMessage(userId, "⏳ **Backing up FRESH numbers & Deleting...**");

            try {
                const freshNumbers = await NumberModel.find({ country: country, status: 'Available' }).lean(); // 🔥 .lean()

                if (freshNumbers.length > 0) {
                    let fileContent = "";
                    freshNumbers.forEach(item => {
                        fileContent += `${item.number}\n`;
                    });

                    const fileBuffer = Buffer.from(fileContent, 'utf8');
                    const fileName = `${country.replace(/\s/g, '_')}_Fresh_Backup.txt`;

                    for (const adminId of ADMIN_IDS) {
                        try {
                            await bot.sendDocument(adminId, fileBuffer, {
                                caption: `🗑️ **Country Deleted: ${country}**\n💤 Action by: ${userId}\n📂 Backup of Fresh Numbers: ${freshNumbers.length}\n(Used numbers are ignored)`
                            }, {
                                filename: fileName,
                                contentType: 'text/plain'
                            });
                        } catch (err) {
                            console.log(`Failed to send backup to admin ${adminId}:`, err.message);
                        }
                    }
                } else {
                    bot.sendMessage(userId, "⚠️ No fresh numbers found to backup (All used or empty).");
                }

                const result = await NumberModel.deleteMany({ country: country });
                await rebuildCountryCache();

                bot.sendMessage(userId, `✅ **Success!**\nDeleted Total: ${result.deletedCount} numbers from DB.`, { reply_markup: getAdminMenuKeyboard() });

            } catch (error) {
                console.error("Delete Error:", error);
                bot.sendMessage(userId, "❌ Error during process.", { reply_markup: getAdminMenuKeyboard() });
            }
            return;
        }

        if (!isAdmin(userId) && !(await isUserMember(userId))) return;
        const { allowed, remaining } = isUserAllowedAction(userId);
        if (!allowed) { bot.answerCallbackQuery(call.id, { text: `Wait ${remaining}s`, show_alert: true }); return; }

        // 🔥 ASSIGN NUMBER WITH LOCK
        if (data.startsWith('assign_number:')) {
            const countryIdx = parseInt(data.split(':')[1]);
            const country = indexToCountry[countryIdx];

            if (!country_assignment_locks[country]) {
                country_assignment_locks[country] = new Set();
            }

            if (country_assignment_locks[country].has(userId)) {
                bot.answerCallbackQuery(call.id, { text: "⏳ Processing your request...", show_alert: false });
                return;
            }

            country_assignment_locks[country].add(userId);

            try {
                await NumberModel.updateMany({ assigned_to: userId, status: 'Used' }, { $set: { status: 'Used_History', assigned_to: null, assigned_at: null } });

                const availableNumbers = await NumberModel.aggregate([
                    { $match: { country: country, status: 'Available' } },
                    { $sample: { size: 1 } }
                ]);

                if (availableNumbers.length > 0) {
                    const randomNum = await NumberModel.findByIdAndUpdate(
                        availableNumbers[0]._id,
                        { $set: { status: 'Used', assigned_to: userId, assigned_at: new Date() } },
                        { new: true }
                    );

                    let displayNum = randomNum.number.startsWith('+') ? randomNum.number : '+' + randomNum.number;
                    await safeEditMessage(chatId, msgId, ASSIGNMENT_MESSAGE_TEMPLATE(randomNum.flag, randomNum.country, displayNum, "Assigned", NEW_FOOTER_QUOTE),
                        { parse_mode: 'HTML', reply_markup: getNumberControlKeyboard() });
                } else {
                    await rebuildCountryCache();
                    await safeEditMessage(chatId, msgId, `❌ Sold Out.`);
                }
            } finally {
                country_assignment_locks[country].delete(userId);
            }
        }

// 🔥 ULTRA FAST - Change Number এর জন্য এই পুরো section replace করুন

// এটা callback_query handler এর ভিতরে change_number_req এর জায়গায় বসাবেন

else if (data === 'change_number_req') {
    const currentTime = Date.now() / 1000;
    const lastTime = last_change_time[userId] || 0;
    const timeDiff = currentTime - lastTime;
    const cooldownTime = 3; // 3 second cooldown

    // ⏳ Cooldown check
    if (timeDiff < cooldownTime) {
        const remaining = Math.ceil(cooldownTime - timeDiff);
        bot.answerCallbackQuery(call.id, { 
            text: `⏳ Wait ${remaining}s before changing again!`, 
            show_alert: true 
        });
        return;
    }

    // ✅ Cooldown update
    last_change_time[userId] = currentTime;

    // 🔥 INSTANT ANIMATION (শুধু 2 ফ্রেম - নিচে আর উপরে)
    const animationFrames = [
        "🔄 <b>Changing...</b>\n⬇️ Processing...",
        "🔄 <b>Changing...</b>\n⬆️ Done!"
    ];

    // ⚡ Instant answer (button lag remove)
    bot.answerCallbackQuery(call.id);

    // 🎬 Quick animation start
    let frameIndex = 0;
    const animationInterval = setInterval(async () => {
        try {
            await safeEditMessage(chatId, msgId, animationFrames[frameIndex], { parse_mode: 'HTML' });
            frameIndex++;
            if (frameIndex >= animationFrames.length) {
                clearInterval(animationInterval);
            }
        } catch (e) {}
    }, 300); // 300ms per frame = total 600ms animation

    // 📱 Get current number
    const current = await NumberModel.findOne({ assigned_to: userId, status: 'Used' }).lean();

    // ⏱️ Wait for animation to finish (600ms)
    setTimeout(async () => {
        clearInterval(animationInterval);

        if (current) {
            const country = current.country;

            // 🔒 Lock system
            if (!country_assignment_locks[country]) {
                country_assignment_locks[country] = new Set();
            }

            if (country_assignment_locks[country].has(userId)) {
                await safeEditMessage(chatId, msgId, "⏳ Already processing...");
                return;
            }

            country_assignment_locks[country].add(userId);

            try {
                // 🔄 Update old number & get new one (parallel)
                const [_, availableNumbers] = await Promise.all([
                    NumberModel.updateOne(
                        { _id: current._id },
                        { $set: { status: 'Used_History', assigned_to: null, assigned_at: null } }
                    ),
                    NumberModel.aggregate([
                        { $match: { country: country, status: 'Available' } },
                        { $sample: { size: 1 } }
                    ])
                ]);

                if (availableNumbers.length > 0) {
                    const newNumber = await NumberModel.findByIdAndUpdate(
                        availableNumbers[0]._id,
                        { $set: { status: 'Used', assigned_to: userId, assigned_at: new Date() } },
                        { new: true }
                    );

                    let displayNum = newNumber.number.startsWith('+') ? newNumber.number : '+' + newNumber.number;
                    
                    // ✅ Show new number
                    await safeEditMessage(
                        chatId, 
                        msgId, 
                        ASSIGNMENT_MESSAGE_TEMPLATE(newNumber.flag, newNumber.country, displayNum, "Changed", NEW_FOOTER_QUOTE),
                        { parse_mode: 'HTML', reply_markup: getNumberControlKeyboard() }
                    );
                } else {
                    // ❌ No numbers available
                    await safeEditMessage(chatId, msgId, `❌ No numbers left in ${country}.`, {
                        reply_markup: { 
                            inline_keyboard: [[
                                { text: "🌍 Change Country", callback_data: 'change_country_start' }
                            ]] 
                        }
                    });
                }
            } finally {
                // 🔓 Unlock
                country_assignment_locks[country].delete(userId);
            }
        } else {
            await safeEditMessage(chatId, msgId, "❌ No active number.");
        }
    }, 600); // 600ms total (2 frames × 300ms)
}


// Start Sync
try {
    if (fs.existsSync(USER_LIST_FILE)) {
        bot_users = new Set(JSON.parse(fs.readFileSync(USER_LIST_FILE)));
    }
} catch (e) {}

console.log("🚀 Bot is running with ULTRA OPTIMIZATION...");