const fs = require('fs');
const path = require('path');
const TelegramBot = require("node-telegram-bot-api");

// ============================================================
// ⚙️ ADMIN CONFIGURATION
// ============================================================
const ADMIN_IDS = ["6006322754", "6135656510", "1817149496"];

// ============================================================
// 🤖 BOT CONFIGURATION (ALL TOKENS HERE)
// ============================================================
const BOT_TOKENS = {
    // Bot 1: Notification Bot (Group/Channel এ সব SMS পাঠাবে)
    NOTIFICATION_BOT: "7994972018:AAHpfIJZl8zu9j1gtX392vgCIu7RT4sKokU",

    // Bot 2: User Bot (Users দের Private OTP পাঠাবে + Number Bot)
    USER_BOT: "8320415016:AAGSfutZtPUs8cSB0rD-WQTiprXbBC1Azgc"
};

const GROUP_LINKS = {
    OTP_GROUP_ID: "-1003418731250",
    MAIN_CHANNEL_LINK: "https://t.me/RX_ALL_NUMBER_PANEL",
    NUMBER_PANEL_LINK: "https://t.me/smszone1bot"
};

// ============================================================
// 🗄️ MONGODB CONFIGURATION
// ============================================================
// Number Bot এর জন্য USER_DB_URI
const USER_DB_URI = "mongodb+srv://sabbirrehman905_db_user:sabbir123@userjson.f0vppgx.mongodb.net/UserDB?appName=Userjson";

// OTP Workers এর জন্য NUMBER_DB_URI
const NUMBER_DB_URI = "mongodb+srv://rakibkhan625162_db_user:sabbir123@number.qdza7vx.mongodb.net/Number?retryWrites=true&w=majority";

// ============================================================
// 🎨 LOGGING SYSTEM (Time Removed)
// ============================================================
const colors = {
    reset: "\x1b[0m", bright: "\x1b[1m", green: "\x1b[32m",
    yellow: "\x1b[33m", cyan: "\x1b[36m", red: "\x1b[31m", blue: "\x1b[34m"
};

function log(source, msg, type = 'info') {
    // Time variable removed here
    let color = colors.green, icon = "🔹";
    if (type === 'error') { color = colors.red; icon = "❌"; }
    else if (type === 'sms') { color = colors.cyan; icon = "📩"; }
    else if (type === 'warn') { color = colors.yellow; icon = "⚠️"; }
    else if (type === 'success') { color = colors.green; icon = "✅"; }

    // Console log format updated to remove [time]
    console.log(`${colors.bright}${color}${icon} [${source}]${colors.reset} ${msg}`);
}

// ============================================================
// 📢 ERROR REPORTING TO ADMINS
// ============================================================
let adminBot = null;

async function initAdminBot() {
    try {
        adminBot = new TelegramBot(BOT_TOKENS.USER_BOT, { polling: false });
        log("SYSTEM", `Admin error reporting enabled for ${ADMIN_IDS.length} admins!`, "success");
    } catch (e) {
        console.error("Failed to init admin bot:", e.message);
    }
}

async function reportErrorToAdmin(source, errorMessage) {
    if (!adminBot || ADMIN_IDS.length === 0) return;

    const text = `❌ <b>ERROR ALERT</b>\n\n📍 <b>Source:</b> ${source}\n⚠️ <b>Error:</b>\n<pre>${errorMessage.substring(0, 3000)}</pre>`;

    for (const adminId of ADMIN_IDS) {
        try {
            await adminBot.sendMessage(adminId, text, { parse_mode: "HTML" });
            log("ADMIN-NOTIFY", `Error report sent to admin ${adminId}`, "success");
        } catch (e) {
            console.error(`${colors.red}Failed to send error to admin ${adminId}: ${e.message}${colors.reset}`);
        }
    }
}

// ============================================================
// 🚀 LOAD NUMBER BOT
// ============================================================
function loadNumberBot() {
    const numberBotPath = path.join(__dirname, 'Number', 'number-bot.js');

    if (!fs.existsSync(numberBotPath)) {
        const errMsg = "❌ Number Bot file not found at: Number/number-bot.js";
        log("SYSTEM", errMsg, "error");
        reportErrorToAdmin("NUMBER BOT LOADER", errMsg);
        return;
    }

    try {
        log("SYSTEM", "Loading Number Bot...", "warn");

        // Number Bot কে config pass করা
        global.NUMBER_BOT_CONFIG = {
            BOT_TOKEN: BOT_TOKENS.USER_BOT,
            USER_DB_URI: USER_DB_URI,
            NUMBER_DB_URI: NUMBER_DB_URI,
            OTP_GROUP_URL: GROUP_LINKS.MAIN_CHANNEL_LINK
        };

        require(numberBotPath);
        log("NUMBER-BOT", "Started Successfully!", "success");
    } catch (error) {
        const errMsg = `Critical Load Error: ${error.message}\n${error.stack}`;
        log("NUMBER-BOT", errMsg, "error");
        reportErrorToAdmin("NUMBER BOT LOAD", errMsg);
    }
}

// ============================================================
// 📂 LOAD OTP WORKERS FROM 'otp' FOLDER
// ============================================================
function loadOtpWorkers() {
    const otpFolder = path.join(__dirname, 'otp');

    if (!fs.existsSync(otpFolder)) {
        const msg = "❌ 'otp' folder not found! Creating it...";
        log("SYSTEM", msg, "warn");
        fs.mkdirSync(otpFolder, { recursive: true });
        reportErrorToAdmin("OTP FOLDER", "OTP folder was missing, created automatically.");
        return;
    }

    const files = fs.readdirSync(otpFolder).filter(file => file.endsWith('.js'));

    if (files.length === 0) {
        log("SYSTEM", "No OTP workers found in 'otp' folder!", "warn");
        return;
    }

    files.forEach(file => {
        const filePath = path.join(otpFolder, file);
        const workerName = file.replace('.js', '').toUpperCase();

        try {
            log("SYSTEM", `Loading OTP Worker: ${workerName}...`, "warn");

            const WorkerClass = require(filePath);
            const worker = new WorkerClass();

            // Config pass করা
            worker.setConfig({
                BOT_TOKENS,
                GROUP_LINKS,
                NUMBER_DB_URI
            });

            // Event Listeners
            worker.on('log', (msg) => log(workerName, msg, 'info'));
            worker.on('error', (msg) => {
                log(workerName, msg, 'error');
                reportErrorToAdmin(workerName, msg);
            });
            worker.on('sms', (msg) => log(workerName, msg, 'sms'));

            // Start Worker
            worker.start();
            log(workerName, "Started Successfully!", "success");

        } catch (error) {
            const errMsg = `Critical Load Error: ${error.message}\n${error.stack}`;
            log(workerName, errMsg, "error");
            reportErrorToAdmin(`OTP WORKER (${file})`, errMsg);
        }
    });
}

// ============================================================
// ⚠️ GLOBAL ERROR CATCHING
// ============================================================
process.on('uncaughtException', (err) => {
    console.error('💥 Uncaught Exception:', err);
    reportErrorToAdmin("SYSTEM CRASH", `Uncaught Exception:\n${err.message}\n${err.stack}`);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('💥 Unhandled Rejection:', reason);
    reportErrorToAdmin("PROMISE REJECTION", `Unhandled Rejection:\n${reason}`);
});

// ============================================================
// 🚀 START SYSTEM
// ============================================================
async function startSystem() {
    console.log(`
╔════════════════════════════════════╗
║   🤖 MULTI-BOT SYSTEM STARTING    ║
║   👨‍💻 Developer: Alif Hosson        ║
╚════════════════════════════════════╝
    `);

    await initAdminBot();

    log("SYSTEM", "Loading Number Bot...", "warn");
    loadNumberBot();

    log("SYSTEM", "Loading OTP Workers...", "warn");
    loadOtpWorkers();

    log("SYSTEM", "All Systems Running! 🚀", "success");
}

startSystem();



