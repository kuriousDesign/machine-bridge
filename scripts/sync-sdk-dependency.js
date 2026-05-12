const fs = require('fs');
const path = require('path');

const projectRoot = path.resolve(__dirname, '..');
const packageJsonPath = path.join(projectRoot, 'package.json');
const envPath = path.join(projectRoot, '.env');

function loadEnvFile(filePath) {
    if (!fs.existsSync(filePath)) {
        return {};
    }

    const content = fs.readFileSync(filePath, 'utf8');
    const values = {};

    for (const rawLine of content.split(/\r?\n/)) {
        const line = rawLine.trim();
        if (!line || line.startsWith('#')) {
            continue;
        }

        const separatorIndex = line.indexOf('=');
        if (separatorIndex === -1) {
            continue;
        }

        const key = line.slice(0, separatorIndex).trim();
        const value = line.slice(separatorIndex + 1).trim();
        values[key] = value;
    }

    return values;
}

function parseArgs(argv) {
    const parsed = {};

    for (const arg of argv) {
        if (!arg.startsWith('--')) {
            continue;
        }

        const [key, value] = arg.slice(2).split('=');
        parsed[key] = value ?? 'true';
    }

    return parsed;
}

function resolveSdkConfig(args, envValues) {
    const source = (args.source || process.env.MACHINE_SDK_SOURCE || envValues.MACHINE_SDK_SOURCE || 'local').trim().toLowerCase();
    const localPath = (process.env.MACHINE_SDK_LOCAL_PATH || envValues.MACHINE_SDK_LOCAL_PATH || 'file:../machine-sdk').trim();
    const npmVersion = (process.env.MACHINE_SDK_NPM_VERSION || envValues.MACHINE_SDK_NPM_VERSION || '^1.0.112').trim();

    if (source !== 'local' && source !== 'npm') {
        throw new Error(`Unsupported MACHINE_SDK_SOURCE: ${source}. Expected \"local\" or \"npm\".`);
    }

    return {
        source,
        targetDependency: source === 'local' ? localPath : npmVersion,
    };
}

function main() {
    const args = parseArgs(process.argv.slice(2));
    const envValues = loadEnvFile(envPath);
    const sdkConfig = resolveSdkConfig(args, envValues);
    const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));

    const currentDependency = packageJson.dependencies?.['@kuriousdesign/machine-sdk'];
    if (!packageJson.dependencies) {
        throw new Error('package.json does not contain a dependencies section.');
    }

    if (currentDependency === sdkConfig.targetDependency) {
        console.log(`[sdk-sync] machine-sdk already set to ${sdkConfig.source}: ${sdkConfig.targetDependency}`);
        return;
    }

    packageJson.dependencies['@kuriousdesign/machine-sdk'] = sdkConfig.targetDependency;
    fs.writeFileSync(packageJsonPath, `${JSON.stringify(packageJson, null, 2)}\n`);
    console.log(`[sdk-sync] Updated machine-sdk dependency from ${currentDependency} to ${sdkConfig.targetDependency}`);
    console.log('[sdk-sync] Run npm install to refresh node_modules and package-lock.json.');
}

main();